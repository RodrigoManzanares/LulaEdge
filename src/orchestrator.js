

import { DurableObject } from "cloudflare:workers";

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

  const data = {
      strategy: plan.strategy, target_table: plan.target_table,
      cache_key: plan.cache_key, phase_1: plan.phase_1,
      phase_2: plan.phase_2, assembly: plan.assembly, do_payload: plan.do_payload
  };

  const sigBytes = base64ToUint8(plan.signature);
  return crypto.subtle.verify("Ed25519", key, sigBytes, new TextEncoder().encode(JSON.stringify(data)));
}

async function callExecutor(env, binding, payload, timeoutMs) {
  const service = env[binding];
  if (!service) return { success: false, data: [], shard: payload.cat_id, ms: 0, err: `Executor binding [${binding}] not found in Orchestrator` };

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
        return { success: false, data: [], shard: payload.cat_id, ms: Math.round(performance.now() - t0), err: body.failures[0].error };
    }

    const execMeta = body.successes?.[0]?.meta || body.meta || {};
    return {
      success: true, data: body.successes?.[0]?.data || [], shard: payload.cat_id, ms: Math.round(performance.now() - t0),
      lat: execMeta.lat, lon: execMeta.lon, colo: execMeta.colo, rows: execMeta.rows ?? payload.known_rows ?? 0,
      rows_source: execMeta.rows_source ?? payload.known_source ?? 'exact', health: execMeta.health || 100
    };
  } catch (e) {
    clearTimeout(tid);
    return { success: false, data: [], shard: payload.cat_id, ms: timeoutMs, err: e.message || "Timeout/Error" };
  }
}

function assembleBlindly(action, phase1Data, phase2Results, masterMatch, shardMatch) {
  const flatPhase2 = phase2Results.flatMap(r => r.data.map(d => ({ ...d, _shard: r.shard })));

  if (action === "concat") return flatPhase2;
  if (action === "sum") { let t = 0; flatPhase2.forEach(d => t += Number(d.val || 0)); return [{ val: t }]; }
  if (action === "min") { const values = flatPhase2.map(d => Number(d.val)).filter(v => !isNaN(v)); return [{ val: values.length ? Math.min(...values) : null }]; }
  if (action === "max") { const values = flatPhase2.map(d => Number(d.val)).filter(v => !isNaN(v)); return [{ val: values.length ? Math.max(...values) : null }]; }
  if (action === "map_merge") {
      if (!phase1Data || !phase1Data.length) return [];
      return phase1Data.map(p1Row => {
          const matchVal = String(p1Row[masterMatch]);
          const matches = flatPhase2.filter(p2Row => String(p2Row[shardMatch]) === matchVal);
          return { ...p1Row, _shards_data: matches.length ? matches : null };
      });
  }
  if (action === "migration_summary") {
      return [{
          total_shards_targeted: phase2Results.length, success_count: phase2Results.filter(r => r.success).length,
          fail_count: phase2Results.filter(r => !r.success).length, failed_shards: phase2Results.filter(r => !r.success).map(r => r.shard),
          details: phase2Results.map(r => ({ shard: r.shard, status: r.success ? "OK" : "ERROR", error: r.err || null, latency_ms: r.ms }))
      }];
  }
  if (action === "discovery_summary") {
      return phase2Results.map(r => ({ shard: r.shard, status: r.success ? "ONLINE" : "OFFLINE", rows: r.rows || 0, rows_source: r.rows_source || 'exact', health: r.health || 100, latency_ms: r.ms, colo: r.colo || "UNK", error: r.err || null }));
  }
  if (action === "mutation_result") {
      return phase2Results.map(r => ({ shard: r.shard, mutation_success: r.success, latency_ms: r.ms, error: r.err || null }));
  }
  return [];
}

export default {
  async fetch(req, env, ctx) {
    const cors = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, X-LulaEdge-Key, Authorization"
    };
    if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: cors });

    const clientGeo = { lat: req.cf?.latitude || null, lon: req.cf?.longitude || null, colo: req.cf?.colo || 'UNK', country: req.cf?.country || 'UNK' };

    if (req.headers.get("Upgrade") === "websocket") {
        const stubId = new URL(req.url).searchParams.get("stub_id");
        if (!stubId || !env.CONTEXT_DO) return new Response("Missing stub_id or DO binding", { status: 400 });
        const doId = env.CONTEXT_DO.idFromString(stubId);
        return env.CONTEXT_DO.get(doId).fetch(req);
    }

    const sendLogAsync = (shardsHit, ms, planId, strategy, table) => {
      if (env.TRUSTED_ENGINE_URL) {
        ctx.waitUntil(fetch(`${env.TRUSTED_ENGINE_URL}/log`, {
            method: "POST", headers: { "Content-Type": "application/json", "X-LulaEdge-Key": req.headers.get("X-LulaEdge-Key") || "" },
            body: JSON.stringify({ s: shardsHit, t: ms, p: planId, st: strategy, tb: table })
        }).catch(() => {}));
      }
    };

    try {
      const tStart = performance.now();
      const plan = await req.json();

      if (plan.strategy === "live_sync") {
          const stubId = plan.do_payload?.stub_id;
          if (!stubId) return new Response("BAD REQUEST: Missing stub_id", { status: 400, headers: cors });
          if (!env.CONTEXT_DO) return new Response("CONTEXT_DO binding not found", { status: 500, headers: cors });

          const doId = env.CONTEXT_DO.idFromString(stubId);
          const doStub = env.CONTEXT_DO.get(doId);

          const action = plan.do_payload?.action || "get";
          const targetPath = action === "patch" ? "/patch" : action === "query" ? "/query" : "/get";
          const bodyPayload = action === "patch" ? { patch: plan.do_payload.patch, version: plan.do_payload.version } : action === "query" ? { path: plan.do_payload.path } : null;

          const internalReq = new Request(`http://internal${targetPath}`, {
              method: req.method, headers: req.headers, body: bodyPayload ? JSON.stringify(bodyPayload) : null
          });

          const doResponse = await doStub.fetch(internalReq);
          const corsResponse = new Response(doResponse.body, doResponse);
          Object.entries(cors).forEach(([k, v]) => corsResponse.headers.set(k, v));

          return corsResponse;
      }

      if (!(await verifySignature(plan, env))) return new Response("UNAUTHORIZED", { status: 401, headers: cors });

      const isLiveStrategy = ["discovery_summary", "mutation_result", "create_do_result", "live_sync"].includes(plan.assembly?.action || plan.strategy);
      const cacheUrl = new URL(req.url);
      cacheUrl.pathname = `/cache/${plan.cache_key}`;
      const cacheReq = new Request(cacheUrl.toString());
      const cache = caches.default;

      if (!isLiveStrategy) {
        let response = await cache.match(cacheReq);
        if (response) {
            const cachedRes = new Response(response.body, response);
            cachedRes.headers.set("X-Lula-Cache", "HIT");
            Object.entries(cors).forEach(([k,v]) => cachedRes.headers.set(k,v));
            sendLogAsync(0, Math.round(performance.now() - tStart), plan.plan_id, plan.strategy, plan.target_table);
            return cachedRes;
        }
      }

      let phase1Data = [], phase1Keys = [];
      if (plan.phase_1) {
        const res = await env.MASTER_DB.prepare(plan.phase_1.sql).bind(...(plan.phase_1.params || [])).all();
        phase1Data = res.results || [];
        if (plan.phase_1.export_col) phase1Keys = phase1Data.map(r => r[plan.phase_1.export_col]).filter(k => k != null);
      }

      const executionPromises = (plan.phase_2 || []).map(async (instruction) => {
        let finalSql = instruction.sql;
        let finalParams = instruction.params || [];

        if (instruction.phase_1_export && instruction.placeholder) {
            if (!phase1Keys.length) return { success: true, data: [], shard: instruction.cat_id, ms: 0 };
            const qMarks = phase1Keys.map(() => "?").join(",");
            finalSql = finalSql.replace(instruction.placeholder, qMarks);
            finalParams = [...phase1Keys, ...finalParams];
        }

        const isModifyingQuery = plan.assembly?.action === "migration_summary" || instruction.is_migration === true;

        const payload = {
          sql: finalSql, params: finalParams, d1_binding: instruction.d1_binding, cat_id: instruction.cat_id,
          introspect: instruction.introspect, client_geo: clientGeo, is_migration: isModifyingQuery,
          known_rows: instruction.known_rows, known_source: instruction.known_source
        };

        return callExecutor(env, instruction.binding, payload, instruction.timeout);
      });

      const phase2Results = await Promise.all(executionPromises);
      let finalResult = [];

      if (plan.assembly?.action === "create_do_result") {
          if (phase2Results.length > 0 && !phase2Results[0].success) throw new Error("DDL Initialization Error via Executor: " + phase2Results[0].err);
          if (!env.CONTEXT_DO) throw new Error("CONTEXT_DO binding not found in Orchestrator.");

          const { tenant_id, shard_binding, document, schema, metadata, executor_binding } = plan.do_payload;
          const documentId = crypto.randomUUID();

          const doId = env.CONTEXT_DO.newUniqueId();
          const doStub = env.CONTEXT_DO.get(doId);

          const executorToUse = plan.phase_2?.[0]?.binding || executor_binding || "EXEC_1";
          const catId = plan.phase_2?.[0]?.cat_id || plan.do_payload?.cat_id || "lula-shard-default";

          const initRes = await doStub.initializeDocument({
              documentId, tenantId: tenant_id, shardName: shard_binding, document, schema: schema || {}, metadata: metadata || {},
              executorBinding: executorToUse, catId: catId
          });

          if (!initRes.success) throw new Error("DO Initialization Failed: " + (initRes.error || "Unknown"));

          const now = Date.now();
          const insertPayload = {
              sql: `INSERT INTO documents (document_id, tenant_id, stub_id, document, version, schema_json, metadata, created_at, updated_at, last_snapshot_at, last_snapshot_version, accumulated_change_bytes) VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?, 1, 0)`,
              params: [documentId, tenant_id, doId.toString(), JSON.stringify(document), JSON.stringify(schema || null), JSON.stringify(metadata || null), now, now, now],
              d1_binding: shard_binding, cat_id: catId, is_migration: true
          };

          const persistResult = await callExecutor(env, executorToUse, insertPayload, 3000);
          if (!persistResult.success) throw new Error("Persistence failed in Executor: " + persistResult.err);

          finalResult = [{ documentId, DO_stub: doId.toString(), version: 1, status: "CREATED", shard_assigned: shard_binding }];
      } else {
          finalResult = assembleBlindly(plan.assembly.action, phase1Data, phase2Results, plan.assembly.master_match, plan.assembly.shard_match);
      }

      const telemetry = {};
      phase2Results.forEach(r => { telemetry[r.shard] = { ms: r.ms, success: r.success, err: r.err, val: r.rows, colo: r.colo }; });

      const totalMs = Math.round(performance.now() - tStart);
      sendLogAsync(phase2Results.length, totalMs, plan.plan_id, plan.strategy, plan.target_table);

      const finalResponse = Response.json({
        results: finalResult, telemetry, shards_hit: phase2Results.length, plan_id: plan.plan_id,
        geo: {
          client: clientGeo,
          shards: phase2Results.map(r => {
            const baseLat = parseFloat(clientGeo.lat) || 40.4168; const baseLon = parseFloat(clientGeo.lon) || -3.7038;
            return { id: r.shard, lat: r.lat != null ? r.lat : (baseLat + (Math.random() - 0.5) * 2), lon: r.lon != null ? r.lon : (baseLon + (Math.random() - 0.5) * 2), colo: (r.colo && r.colo !== "UNK") ? r.colo : clientGeo.colo };
          })
        }
      }, { headers: cors });

      if (plan.ttl_ms > 0 && !isLiveStrategy) {
          const cacheRes = finalResponse.clone();
          cacheRes.headers.set("Cache-Control", `s-maxage=${Math.floor(plan.ttl_ms / 1000)}`);
          ctx.waitUntil(cache.put(cacheReq, cacheRes).catch(()=>{}));
      }

      return finalResponse;

    } catch (e) { return new Response(JSON.stringify({error: e.message}), { status: 500, headers: cors }); }
  }
};

export class ContextDO extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.env = env;
    this.storage = ctx.storage;
    this.docState = null;
    this.initialized = false;
    this.metrics = { update_count: 0, active_subscriptions: 0, connection_peak: 0, last_activity: Date.now() };
    this.sessions = new Map();
  }

  async ensureInitialized() {
    if (this.initialized) return;
    const stored = await this.storage.get(["docState", "metrics"]);
    this.docState = stored.get("docState") || null;

    if (this.docState) {
       this.docState.last_snapshot_version = this.docState.last_snapshot_version || this.docState.version || 1;
       this.docState.last_snapshot_at = this.docState.last_snapshot_at || this.docState.updated_at || Date.now();
       this.docState.accumulated_change_bytes = this.docState.accumulated_change_bytes || 0;
    }

    if (stored.get("metrics")) this.metrics = { ...this.metrics, ...stored.get("metrics") };
    this.initialized = true;
  }

  async initializeDocument({ documentId, tenantId, shardName, document, schema, metadata, executorBinding, catId }) {
    await this.ensureInitialized();

    const newState = {
      document_id: documentId, tenant_id: tenantId, shard_assigned: shardName,
      executor_binding: executorBinding, cat_id: catId,
      data: document, schema: schema || {}, metadata: metadata || {},
      version: 1, created_at: Date.now(), updated_at: Date.now(),

      last_snapshot_version: 1,
      last_snapshot_at: Date.now(),
      accumulated_change_bytes: 0
    };

    if (JSON.stringify(newState).length > 100000) return { success: false, error: "DOCUMENT_SIZE_LIMIT_EXCEEDED" };

    this.docState = newState;
    await this.storage.put({ "docState": this.docState, "metrics": this.metrics });
    return { success: true, document_id: documentId };
  }

  async fetch(request) {
    await this.ensureInitialized();
    this.metrics.last_activity = Date.now();
    const url = new URL(request.url);

    if (request.headers.get("Upgrade") === "websocket") return this.handleWebSocketSubscription(request);

    if (url.pathname === "/get") return Response.json({ document: this.docState, metrics: { ...this.metrics, active_subscriptions: this.sessions.size } });
    if (url.pathname === "/patch" && request.method === "POST") {
      try { return await this.applyPatch(await request.json()); }
      catch (e) { return Response.json({ success: false, error: e.message }, { status: 400 }); }
    }
    if (url.pathname === "/query" && request.method === "POST") {
        try {
            const { path } = await request.json();
            let result = this.docState.data;
            if (path) {
                result = path.split('.').reduce((acc, part) => acc && acc[part] !== undefined ? acc[part] : undefined, result);
            }
            return Response.json({ success: true, path, result, version: this.docState.version });
        } catch (e) { return Response.json({ success: false, error: e.message }, { status: 400 }); }
    }
    return new Response("Not Found", { status: 404 });
  }

  async applyPatch({ patch, version }) {
    if (!this.docState) return Response.json({ success: false, error: "DOCUMENT_NOT_INITIALIZED" }, { status: 400 });
    if (version && version <= this.docState.version) return Response.json({ success: false, error: "VERSION_CONFLICT", current_version: this.docState.version }, { status: 409 });

    const patchSize = JSON.stringify(patch).length;
    const nextData = { ...this.docState.data, ...patch };

    if (JSON.stringify(nextData).length > 100000) return Response.json({ success: false, error: "PAYLOAD_TOO_LARGE_100KB_LIMIT" }, { status: 413 });

    this.docState.data = nextData;
    this.docState.version = version || (this.docState.version + 1);
    this.docState.updated_at = Date.now();
    this.docState.accumulated_change_bytes += patchSize;
    this.metrics.update_count++;

    await this.storage.put({ "docState": this.docState, "metrics": this.metrics });

    const currentAlarm = await this.storage.getAlarm();
    if (!currentAlarm) await this.storage.setAlarm(Date.now() + 10000);

    this.broadcast({ event: "document_updated", version: this.docState.version, patch: patch, updated_by_patch: true });

    return Response.json({ success: true, version: this.docState.version, metrics: { ...this.metrics, active_subscriptions: this.sessions.size } });
  }

  async alarm() {
    await this.ensureInitialized();
    if (!this.docState) return;

    try {
        const executorBinding = this.docState.executor_binding;
        const shardBinding = this.docState.shard_assigned;
        const catId = this.docState.cat_id;

        if (!executorBinding || !shardBinding || !this.env[executorBinding]) {
            console.error("Alarm failed: Missing Executor or Shard bindings in DO State.");
            return;
        }

        const now = Date.now();
        const service = this.env[executorBinding];


        const version_delta = this.docState.version - this.docState.last_snapshot_version;
        const time_delta = now - this.docState.last_snapshot_at;
        const doc_size = JSON.stringify(this.docState.data).length || 1;
        const change_ratio = this.docState.accumulated_change_bytes / doc_size;

        let snapshotReason = null;
        if (version_delta >= 100) snapshotReason = "VERSION_DELTA";
        else if (time_delta >= 300000) snapshotReason = "TIME_DELTA";
        else if (change_ratio >= 0.20) snapshotReason = "CHANGE_RATIO";


        let createHistoryEntry = false;
        if (snapshotReason) {
            this.docState.last_snapshot_version = this.docState.version;
            this.docState.last_snapshot_at = now;
            this.docState.accumulated_change_bytes = 0;
            createHistoryEntry = true;
            await this.storage.put({ "docState": this.docState });
        }


        const payloadUpdate = {
            sql: "UPDATE documents SET document = ?, version = ?, updated_at = ?, last_snapshot_at = ?, last_snapshot_version = ?, accumulated_change_bytes = ? WHERE document_id = ?",
            params: [JSON.stringify(this.docState.data), this.docState.version, this.docState.updated_at, this.docState.last_snapshot_at, this.docState.last_snapshot_version, this.docState.accumulated_change_bytes, this.docState.document_id],
            d1_binding: shardBinding, cat_id: catId, is_migration: true
        };

        const execRes = await service.fetch("http://internal/query", {
            method: "POST", headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payloadUpdate)
        });

        if (!execRes.ok) {
            await this.storage.setAlarm(Date.now() + 30000);
            return;
        }


        if (createHistoryEntry) {
            const payloadHistory = {
                sql: "INSERT INTO document_history (document_id, version, document, reason, created_at) VALUES (?, ?, ?, ?, ?)",
                params: [this.docState.document_id, this.docState.version, JSON.stringify(this.docState.data), snapshotReason, now],
                d1_binding: shardBinding, cat_id: catId, is_migration: true
            };

            await service.fetch("http://internal/query", {
                method: "POST", headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payloadHistory)
            });
            console.log(`Snapshot histórico generado por: ${snapshotReason}`);
        }

    } catch (e) {
        console.error("Alarm error:", e);
    }
  }

  handleWebSocketSubscription(request) {
    const [client, server] = new WebSocketPair();
    server.accept();
    const sessionId = crypto.randomUUID();
    this.sessions.set(sessionId, server);

    this.metrics.active_subscriptions = this.sessions.size;
    if (this.sessions.size > this.metrics.connection_peak) this.metrics.connection_peak = this.sessions.size;

    server.send(JSON.stringify({ event: "subscribed", connection_id: sessionId, document: this.docState }));

    server.addEventListener("message", async (msg) => {
      try {
        const payload = JSON.parse(msg.data);
        if (payload.action === "patch") {
          const res = await this.applyPatch({ patch: payload.patch, version: payload.version });
          server.send(JSON.stringify({ event: "patch_ack", result: await res.json() }));
        }
      } catch (err) { server.send(JSON.stringify({ event: "error", error: "Malformed message" })); }
    });

    const cleanup = () => { this.sessions.delete(sessionId); this.metrics.active_subscriptions = this.sessions.size; };
    server.addEventListener("close", cleanup);
    server.addEventListener("error", cleanup);

    return new Response(null, { status: 101, webSocket: client });
  }

  broadcast(data) {
    const message = JSON.stringify(data);
    for (const [id, ws] of this.sessions.entries()) {
      try { ws.send(message); } catch (e) { this.sessions.delete(id); }
    }
  }
}