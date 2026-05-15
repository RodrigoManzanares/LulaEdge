/**
 * LULAEDGE EXECUTOR v1.1 - Alter Update
 * ─────────────────────────────────────────────────────────────────
 */

async function runIntrospection(db) {
  const schemaRes = await db.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE '_cf_%' AND name NOT LIKE 'sqlite_%'").all();
  return { introspected: true, tables: schemaRes.results.map(t => t.name) };
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

    if (shouldIntrospect) {
      const geoInfo = {
        lat: req.cf?.latitude || body.client_geo?.lat || null,
        lon: req.cf?.longitude || body.client_geo?.lon || null,
        colo: req.cf?.colo || body.client_geo?.colo || "UNK"
      };

      ctx.waitUntil((async () => {
        try {
          const schema = await runIntrospection(db);
          await fetch(`${env.ENGINE_URL || "https://api.lulaedge.com"}/update-schema`, {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "X-LulaEdge-Key": env.LULAEDGE_API_KEY
            },
            body: JSON.stringify({ cat_id: catId, schema: schema, geo: geoInfo })
          });
        } catch (e) { console.error("Introspection Error:", e); }
      })());
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
          if (errMsg.includes("duplicate column name")) {
            note = "Columna ya existía (Idempotent Success)";
          } else {
            throw migErr;
          }
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
            warning: note
          }
        }],
        failures: []
      });

    } catch (err) {
      return Response.json({
        successes: [],
        failures: [{ shard: catId, error: err.message || "EXECUTION_ERROR" }]
      });
    }
  }
};