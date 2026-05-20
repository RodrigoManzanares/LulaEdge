/**
 * LULAEDGE EXECUTOR v1.2 - Telemetry
 * ─────────────────────────────────────────────────────────────────
 */

const introspectionCache = new Map();
const CACHE_TTL_MS = 5 * 60 * 1000;

async function runIntrospection(db, catId) {
  const now = Date.now();

  if (catId && introspectionCache.has(catId)) {
    const cached = introspectionCache.get(catId);
    if (now - cached.timestamp < CACHE_TTL_MS) {
      return {
        tables: cached.tables,
        rows: cached.rows,
        rows_source: "cached",
        health: cached.health
      };
    }
  }

  let tables = [];
  try {
    const schemaRes = await db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '_cf_%' AND name NOT LIKE 'sqlite_%'").all();
    tables = schemaRes.results?.map(t => t.name) || [];
  } catch (e) {}

  if (tables.length === 0) {
    try {
      const fallbackRes = await db.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE '_cf_%' AND name NOT LIKE 'sqlite_%'").all();
      tables = fallbackRes.results?.map(t => t.name) || [];
    } catch (_) {}
  }

  try {
    let totalRows = 0;
    let rowSource = 'unknown';

    try {
      const fastCount = await db.prepare("SELECT SUM(seq) AS total FROM sqlite_sequence").first("total");
      if (fastCount !== null && fastCount !== undefined) {
        totalRows = Number(fastCount) || 0;
        rowSource = 'sequence';
      }
    } catch (_) {}

    if (totalRows === 0 && tables.length > 0) {
      try {
        const statRes = await db.prepare("SELECT stat FROM sqlite_stat1").all();
        if (statRes.results && statRes.results.length > 0) {
          let maxEstimatedRows = 0;
          for (const row of statRes.results) {
            const estimated = parseInt(row.stat.split(' ')[0]);
            if (!isNaN(estimated) && estimated > maxEstimatedRows) {
              maxEstimatedRows = estimated;
            }
          }
          if (maxEstimatedRows > 0) {
              totalRows = maxEstimatedRows;
              rowSource = 'stat1';
          }
        }
      } catch (_) {}
    }

    if (totalRows === 0 && tables.length > 0) {
      let exactCount = 0;
      for (const table of tables) {
        try {
          const tableCount = await db.prepare(`SELECT COUNT(*) AS total FROM "${table}"`).first("total");
          if (tableCount !== null && tableCount !== undefined) {
            exactCount += Number(tableCount) || 0;
          }
        } catch (_) {}
      }
      totalRows = exactCount;
      rowSource = 'exact';
    }

    const finalResult = {
      tables,
      rows: totalRows,
      rows_source: rowSource,
      health: 100
    };

    if (catId) {
      introspectionCache.set(catId, { ...finalResult, timestamp: now });
    }

    return finalResult;

  } catch (e) {
    return { tables, rows: 0, rows_source: 'error', health: 100, error: e.message };
  }
}

export default {
  async fetch(req, env, ctx) {
    if (new URL(req.url).pathname !== "/query") return new Response("Not Found", { status: 404 });
    if (req.method !== "POST") return new Response("Method Not Allowed", { status: 405 });

    let body;
    try { body = await req.json(); } catch(_) { return Response.json({error:"INVALID_JSON"},{status:400}); }

    const d1Binding = body.d1_binding;
    const catId = body.cat_id;
    const sql = body.sql;
    const params = body.params || [];
    const shouldIntrospect = body.introspect === true;
    const isMigration = body.is_migration === true;

    if (!d1Binding || !env[d1Binding]) {
      return Response.json({ successes: [], failures: [{ shard: catId, error: "BINDING_ERROR" }] });
    }

    const db = env[d1Binding];

    const geoInfo = {
      lat: req.cf?.latitude || body.client_geo?.lat || null,
      lon: req.cf?.longitude || body.client_geo?.lon || null,
      colo: req.cf?.colo || body.client_geo?.colo || "UNK"
    };

    if (shouldIntrospect) {
      const introspectPromise = runIntrospection(db, catId).then(schemaData => {
        return fetch(`${env.ENGINE_URL || "https://api.lulaedge.com"}/update-schema`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "X-LulaEdge-Key": env.LULAEDGE_API_KEY
          },
          body: JSON.stringify({ cat_id: catId, schema: schemaData, geo: geoInfo })
        });
      }).catch(e => console.error("Introspection Error:", e));

      ctx.waitUntil(introspectPromise);
    }

    try {
      let resultData = [];
      let execDuration = 0;
      let note = null;

      if (isMigration) {
        try {
          const res = await db.prepare(sql).bind(...params).run();
          execDuration = res.meta?.duration || 0;
        } catch (migErr) {
          const errMsg = (migErr.message || "").toLowerCase();
          if (errMsg.includes("duplicate column name")) note = "Columna exist (Idempotent Success)";
          else throw migErr;
        }
      } else {
        const res = await db.prepare(sql).bind(...params).all();
        resultData = res.results || [];
        execDuration = res.meta?.duration || 0;
      }

      return Response.json({
        successes: [{
          shard: catId,
          data: resultData,
          meta: {
            duration: Math.max(1, Math.round(execDuration)),
            introspected: shouldIntrospect,
            warning: note,
            lat: geoInfo.lat,
            lon: geoInfo.lon,
            colo: geoInfo.colo
          }
        }],
        failures: []
      });

    } catch (err) {
      return Response.json({ successes: [], failures: [{ shard: catId, error: err.message || "EXECUTION_ERROR" }] });
    }
  }
};