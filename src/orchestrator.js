/**
 * LULAEDGE ORCHESTRATOR v1.0
 */

const JWKS_CACHE = new Map();

function base64ToUint8(b64) {
  const bin = atob(b64);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  return bytes;
}

async function verifySignature(plan, env) {
  let key = JWKS_CACHE.get('engine_key');
  if (!key) {
    if (!env.ENGINE_PUB_KEY) return false;
    key = await crypto.subtle.importKey("jwk", JSON.parse(env.ENGINE_PUB_KEY), { name: "Ed25519" }, false, ["verify"]);
    JWKS_CACHE.set('engine_key', key);
  }
  const data = { cache_key: plan.cache_key, phase_1: plan.phase_1, phase_2: plan.phase_2, assembly: plan.assembly };
  const sigBytes = base64ToUint8(plan.signature);
  return crypto.subtle.verify("Ed25519", key, sigBytes, new TextEncoder().encode(JSON.stringify(data)));
}

async function callExecutor(env, binding, payload, timeoutMs) {
  const service = env[binding];

  if (!service) return { success: false, data: [], shard: payload.cat_id, ms: 0, err: "No binding" };

  const ctrl = new AbortController();
  const tid = setTimeout(() => ctrl.abort(), timeoutMs);
  const t0 = performance.now();

  try {
    const res = await service.fetch("http://internal/query", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload), signal: ctrl.signal
    });
    clearTimeout(tid);
    if (!res.ok) throw new Error("HTTP " + res.status);
    const body = await res.json();

    if (body.failures && body.failures.length > 0) {
        return {
          success: false,
          data: [],
          shard: payload.cat_id,
          ms: Math.round(performance.now() - t0),
          err: body.failures[0].error
        };
    }

    const executorGeo = body.successes?.[0]?.meta || body.meta || {};

    return {
      success: true,
      data: body.successes?.[0]?.data || [],
      shard: payload.cat_id,
      ms: Math.round(performance.now() - t0),
      lat: executorGeo.lat,
      lon: executorGeo.lon,
      colo: executorGeo.colo
    };
  } catch (e) {
    clearTimeout(tid);
    return { success: false, data: [], shard: payload.cat_id, ms: timeoutMs, err: e.message || "Timeout/Error" };
  }
}

function assembleBlindly(action, phase1Data, phase2Results, masterMatch, shardMatch) {
  const flatPhase2 = phase2Results.flatMap(r => r.data.map(d => ({ ...d, _shard: r.shard })));

  if (action === "concat") return flatPhase2;

  if (action === "sum") {
      let t = 0; flatPhase2.forEach(d => t += Number(d.val || 0)); return [{ val: t }];
  }

  if (action === "map_merge") {
      if (!phase1Data || !phase1Data.length) return [];
      return phase1Data.map(p1Row => {
          const matchVal = String(p1Row[masterMatch]);
          const matches = flatPhase2.filter(p2Row => String(p2Row[shardMatch]) === matchVal);
          return { ...p1Row, _shards_data: matches.length ? matches : null };
      });
  }

  return [];
}

export default {
  async fetch(req, env, ctx) {
    const cors = { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Headers": "*" };
    if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: cors });

    const clientGeo = {
      lat: req.cf?.latitude || null,
      lon: req.cf?.longitude || null,
      colo: req.cf?.colo || 'UNK',
      country: req.cf?.country || 'UNK'
    };

    const sendLogAsync = (shardsHit, ms, planId) => {
      if (env.TRUSTED_ENGINE_URL) {
        ctx.waitUntil(
          fetch(`${env.TRUSTED_ENGINE_URL}/log`, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "X-LulaEdge-Key": req.headers.get("X-LulaEdge-Key") || ""
            },
            body: JSON.stringify({ s: shardsHit, t: ms, p: planId })
          }).catch(e => console.error("❌ LOG ERROR:", e.message))
        );
      }
    };

    try {
      const tStart = performance.now();
      const plan = await req.json();

      if (!(await verifySignature(plan, env))) return new Response("UNAUTHORIZED", { status: 401, headers: cors });

      const cacheUrl = new URL(req.url);
      cacheUrl.pathname = `/cache/${plan.cache_key}`;
      const cacheReq = new Request(cacheUrl.toString());
      const cache = caches.default;

      let response = await cache.match(cacheReq);
      if (response) {
          const cachedRes = new Response(response.body, response);
          cachedRes.headers.set("X-Lula-Cache", "HIT");
          Object.entries(cors).forEach(([k,v]) => cachedRes.headers.set(k,v));

          sendLogAsync(0, Math.round(performance.now() - tStart), plan.plan_id);

          return cachedRes;
      }

      let phase1Data = [];
      let phase1Keys = [];
      if (plan.phase_1) {
        const res = await env.MASTER_DB.prepare(plan.phase_1.sql).bind(...(plan.phase_1.params || [])).all();
        phase1Data = res.results || [];
        if (plan.phase_1.export_col) {
            phase1Keys = phase1Data.map(r => r[plan.phase_1.export_col]).filter(k => k != null);
        }
      }

      const executionPromises = plan.phase_2.map(async (instruction) => {
        let finalSql = instruction.sql;
        let finalParams = instruction.params || [];

        if (instruction.phase_1_export && instruction.placeholder) {
            if (!phase1Keys.length) return { success: true, data: [], shard: instruction.cat_id, ms: 0 };
            const qMarks = phase1Keys.map(() => "?").join(",");
            finalSql = finalSql.replace(instruction.placeholder, qMarks);
            finalParams = [...phase1Keys, ...finalParams];
        }

        const payload = {
          sql: finalSql,
          params: finalParams,
          d1_binding: instruction.d1_binding,
          cat_id: instruction.cat_id,
          introspect: instruction.introspect,
          client_geo: clientGeo
        };

        return callExecutor(env, instruction.binding, payload, instruction.timeout);


        return callExecutor(env, instruction.binding, payload, instruction.timeout);
      });

      const phase2Results = await Promise.all(executionPromises);

      const telemetry = {};
      phase2Results.forEach(r => {
        telemetry[r.shard] = {
          ms: r.ms,
          success: r.success,
          err: r.err,
          val: r.data[0]?.val,
          colo: r.colo
        };
      });

      const finalResult = assembleBlindly(
        plan.assembly.action,
        phase1Data,
        phase2Results,
        plan.assembly.master_match,
        plan.assembly.shard_match
      );

      const totalMs = Math.round(performance.now() - tStart);

      sendLogAsync(phase2Results.length, totalMs, plan.plan_id);

      const finalResponse = Response.json({
        results: finalResult,
        telemetry,
        shards_hit: phase2Results.length,
        plan_id: plan.plan_id,
        geo: {
          client: clientGeo,
          shards: phase2Results.map(r => {
            const baseLat = parseFloat(clientGeo.lat) || 0;
            const baseLon = parseFloat(clientGeo.lon) || 0;

            const lat = r.lat != null ? r.lat : (baseLat + (Math.random() - 0.5) * 2);
            const lon = r.lon != null ? r.lon : (baseLon + (Math.random() - 0.5) * 2);

            return {
              id: r.shard,
              lat: lat,
              lon: lon,
              colo: (r.colo && r.colo !== "UNK") ? r.colo : clientGeo.colo
            };
          })
        }
      }, { headers: cors });

      if (plan.ttl_ms > 0) {
          const cacheRes = finalResponse.clone();
          cacheRes.headers.set("Cache-Control", `s-maxage=${Math.floor(plan.ttl_ms / 1000)}`);
          ctx.waitUntil(cache.put(cacheReq, cacheRes).catch(()=>{}));
      }

      return finalResponse;

    } catch (e) {
      return new Response("EXECUTION_ERROR", { status: 500, headers: cors });
    }
  }
};