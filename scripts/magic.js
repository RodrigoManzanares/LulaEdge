/**
 * LULAEDGE MAGIC SCRIPT v6.6 - Smart Discovery Edition
 */
"use strict";

const path = require("path");
const os = require("os");
const fs = require("fs");
const crypto = require("crypto");
const { execSync } = require("child_process");
const readline = require("readline");
const dotenv = require("dotenv");

const envPath = path.resolve(process.cwd(), '.env');

if (!fs.existsSync(envPath)) {
    const envTemplate = `CF_ACCOUNT_ID=
CLOUDFLARE_API_TOKEN=

# Optional: The text that your D1 databases must contain to be detected
# LULAEDGE_SHARD_PREFIX=shard

`;
    fs.writeFileSync(envPath, envTemplate);
    console.log("🛑 [STOP] ¡Welcome! new '.env' in this folder");
    console.log("👉 Set the Cloudflare ID and Token and rerun");
    process.exit(0);
}

dotenv.config({ path: envPath });

if (!process.env.CF_ACCOUNT_ID || !process.env.CLOUDFLARE_API_TOKEN) {
    console.error("❌ Error: Empty credentials in the file .env. Set it to start");
    process.exit(1);
}

const ENGINE_URL      = process.env.ENGINE_URL || "https://api.lulaedge.com";
const INSTALL_TOKEN   = process.env.LULAEDGE_INSTALL_TOKEN || "";
const SHARD_PREFIX    = process.env.LULAEDGE_SHARD_PREFIX || "shard";
const CF_ACCOUNT_ID   = process.env.CF_ACCOUNT_ID;
const CF_API_TOKEN    = process.env.CLOUDFLARE_API_TOKEN;
const MASTER_DB_NAME  = "lulaedge-catalog";
const CONFIG_DIR      = ".lulaedge";
const CONFIG_FILE     = path.join(CONFIG_DIR, "config.json");

const delay = ms => new Promise(res => setTimeout(res, ms));

function loadConfig() { try { return JSON.parse(fs.readFileSync(CONFIG_FILE,"utf8")); } catch(_) { return {}; } }
function saveConfig(d) { if(!fs.existsSync(CONFIG_DIR)) fs.mkdirSync(CONFIG_DIR,{recursive:true}); fs.writeFileSync(CONFIG_FILE,JSON.stringify({...loadConfig(),...d},null,2)); }

function verifyBlobSignature(blob, signatureB64, jwk) {
    try {
        const key = crypto.createPublicKey({ key: jwk, format: 'jwk' });
        return crypto.verify(null, Buffer.from(blob), key, Buffer.from(signatureB64, 'base64'));
    } catch (e) {
        return false;
    }
}

async function askEmail(currentConfig) {
    if (process.env.LULAEDGE_EMAIL) return process.env.LULAEDGE_EMAIL;
    if (currentConfig.email) return currentConfig.email;

    const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
    return new Promise(resolve => {
        rl.question("\n📧 Type your email to setup the infra: ", (email) => {
            rl.close();
            resolve(email.trim() || "admin@lula.local");
        });
    });
}

async function cfApi(endpoint, method="GET", body=null) {
    const res = await fetch(`https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}${endpoint}`, {
        method,
        headers: { "Authorization": `Bearer ${CF_API_TOKEN}`, "Content-Type": "application/json" },
        body: body ? JSON.stringify(body) : null,
    });
    const json = await res.json();
    if (!json.success) throw new Error(`CF API Error: ${JSON.stringify(json.errors)}`);
    return json;
}

async function d1Query(databaseId, sql) {
    try {
        const response = await cfApi(`/d1/database/${databaseId}/query`, "POST", { sql });
        if (response.result && response.result[0] && !response.result[0].success) {
            throw new Error(response.result[0].error || "Unknown D1 Execution Error");
        }
        return response;
    } catch (e) {
        throw new Error(`D1 Query Failed: ${e.message}\nQuery: ${sql.substring(0, 50)}...`);
    }
}

function wranglerBin() {
    const isWin = os.platform() === "win32";
    const local = path.join(process.cwd(), "node_modules", ".bin", isWin ? "wrangler.cmd" : "wrangler");
    return fs.existsSync(local) ? local : (isWin ? "wrangler.cmd" : "wrangler");
}

function deployWorker(tomlPath) {
    execSync(`"${wranglerBin()}" deploy -c ${tomlPath}`, {
        stdio: "inherit",
        env: { ...process.env, CLOUDFLARE_API_TOKEN: CF_API_TOKEN, CLOUDFLARE_ACCOUNT_ID: CF_ACCOUNT_ID },
    });
}

function putSecret(workerName, secretName, secretValue, tomlPath) {
    execSync(`"${wranglerBin()}" secret put ${secretName} -c ${tomlPath}`, {
        input: secretValue, stdio: ["pipe", "inherit", "inherit"],
        env: { ...process.env, CLOUDFLARE_API_TOKEN: CF_API_TOKEN, CLOUDFLARE_ACCOUNT_ID: CF_ACCOUNT_ID },
    });
    console.log(`   🔑 Secret ${secretName} injected.`);
}

async function run() {
    console.log("\n  ╔══════════════════════════════════════════╗");
    console.log("  ║   LULAEDGE MAGIC SCRIPT                  ║");
    console.log("  ╚══════════════════════════════════════════╝\n");

    const currentConfig = loadConfig();
    const userEmail = await askEmail(currentConfig);

    console.log(`\n Checking LulaEdge cloud (${ENGINE_URL})...`);
    try {
        const jwksRes = await fetch(`${ENGINE_URL}/jwks.json`);
        if (!jwksRes.ok) throw new Error("API unreachable");
        console.log("✅ Successful Connection");
    } catch (e) {
        console.error(`❌ LulaEdge cloud unreachable.`);
        process.exit(1);
    }

    console.log(`\n🔍 Searching local data bases (Filtro: '${SHARD_PREFIX}')...`);
    const { result: allDBs } = await cfApi("/d1/database?per_page=100");
    const shardCandidates = allDBs.filter(db => db.name.includes(SHARD_PREFIX));

    if (!shardCandidates.length) {
        console.error(`❌ No databases were found using the word '${SHARD_PREFIX}' name.`);
        console.error(`👉 Solution: Rename your D1 or define using a different LULAEDGE_SHARD_PREFIX here .env`);
        process.exit(1);
    }

    let orchestratorUrl = "";
    try {
        const subRes = await cfApi("/workers/subdomain");
        if (subRes.result && subRes.result.subdomain) {
            orchestratorUrl = `https://lulaedge-orchestrator.${subRes.result.subdomain}.workers.dev`;
            console.log(`✅ Subdomain detected. Router URL: ${orchestratorUrl}`);
        }
    } catch (e) {
        console.warn(`⚠️ Could not retrieve subdomain. Using fallback.`);
    }

    console.log(`Checking latency ${shardCandidates.length} shards...`);
    const shardsWithMetrics = [];
    for (const db of shardCandidates) {
        const t0 = performance.now();
        try {
            await d1Query(db.uuid, "SELECT 1;");
            shardsWithMetrics.push({ name: db.name, uuid: db.uuid, latency: Math.round(performance.now() - t0) });
            process.stdout.write(`.`);
        } catch (e) {
            shardsWithMetrics.push({ name: db.name, uuid: db.uuid, latency: 5000 });
        }
    }

    console.log(`\n\n Asking about Architecture deployment...`);

    const fetchHeaders = { "Content-Type": "application/json" };
    if (INSTALL_TOKEN) fetchHeaders["Authorization"] = `Bearer ${INSTALL_TOKEN}`;

    const regRes = await fetch(`${ENGINE_URL}/register-cluster`, {
        method: "POST",
        headers: fetchHeaders,
        body: JSON.stringify({
            email: userEmail,
            cf_account_id: CF_ACCOUNT_ID,
            shards: shardsWithMetrics,
            shards_count: shardCandidates.length,
            orchestrator_url: orchestratorUrl
        })
    });

    const engineResponse = await regRes.json();
    if (!engineResponse.success) {
        console.error("❌ Signup rejected", engineResponse.error || "Unauthorized");
        process.exit(1);
    }

    const { manifest, api_key } = engineResponse;
    console.log(`✅ License validated`);

    if (!fs.existsSync(CONFIG_DIR)) fs.mkdirSync(CONFIG_DIR, { recursive: true });

    let master = allDBs.find(db => db.name === MASTER_DB_NAME);
    if (!master) {
        console.log(`Creating DB local: ${MASTER_DB_NAME}...`);
        master = (await cfApi("/d1/database", "POST", { name: MASTER_DB_NAME })).result;
    }

    if (manifest.setup_blob) {
        console.log(" Checking sign schema...");
        if (!manifest.setup_signature || !verifyBlobSignature(manifest.setup_blob, manifest.setup_signature, manifest.pub_key)) {
            console.error("❌ INVALID SIGN: schema has been altered in network");
            process.exit(1);
        }
        await d1Query(master.uuid, manifest.setup_blob);
    }

    for (const exec of manifest.executors) {
        let execToml = exec.toml;
        if (!execToml.includes("[vars]")) execToml += `\n[vars]\n`;
        execToml += `ENGINE_URL = "${ENGINE_URL}"\n`;
        execToml += `\n[placement]\nmode = "smart"\n`;

        const tomlPath = path.join(CONFIG_DIR, `wrangler-${exec.name}.toml`);
        fs.writeFileSync(tomlPath, execToml);

        console.log(`\n⚙️  Deploying ${exec.name} ( using Smart Placement)...`);
        deployWorker(tomlPath);
        putSecret(exec.name, "LULAEDGE_API_KEY", api_key, tomlPath);
        await delay(1500);
    }

    console.log("\n Deploying Orchestrator...");
    let orchToml = manifest.orchestrator_toml;
    if (!orchToml.includes("[vars]")) orchToml += `\n[vars]\n`;
    orchToml += `TRUSTED_ENGINE_URL = "${ENGINE_URL}"\n`;
    orchToml += `[[d1_databases]]\nbinding = "MASTER_DB"\ndatabase_name = "${master.name}"\ndatabase_id = "${master.uuid}"\n`;
    orchToml += `\n[placement]\nmode = "smart"\n`;

    const orchTomlPath = path.join(CONFIG_DIR, "wrangler-orchestrator.toml");
    fs.writeFileSync(orchTomlPath, orchToml);
    deployWorker(orchTomlPath);

    if (manifest.pub_key) {
        putSecret("lulaedge-orchestrator", "ENGINE_PUB_KEY", JSON.stringify(manifest.pub_key), orchTomlPath);
    }

    saveConfig({ api_key, email: userEmail, orchestrator_url: orchestratorUrl });

    // 🔥 NUEVO: Bloque de salida final con credenciales claras para el usuario
    console.log("\n  ╔════════════════════════════════════════════════════════╗");
    console.log("  ║ 🎉 LULAEDGE INFRASTRUCTURE DEPLOYED SUCCESSFULLY 🎉    ║");
    console.log("  ╚════════════════════════════════════════════════════════╝\n");

    console.log("   Your distributed SaaS database is now live on the Edge.\n");

    console.log("   🔑 YOUR CREDENTIALS (DO NOT SHARE):");
    console.log("   ────────────────────────────────────────────────────────");
    console.log(`   ▶ UI Console:  https://lulaedgeui.pages.dev/`);
    console.log(`   ▶ Router URL:  ${orchestratorUrl || "Check your Cloudflare Dashboard"}`);
    console.log(`   ▶ API Key:     ${api_key}`);
    console.log("   ────────────────────────────────────────────────────────\n");

    console.log("   🚀 NEXT STEPS:");
    console.log("   1. Open the UI Console link above in your browser.");
    console.log("   2. Paste your Router URL and API Key.");
    console.log(`   3. Start querying your ${shardCandidates.length} shards globally!\n`);
}

run().catch(e => { console.error("\n❌ Fatal Error:", e.message); process.exit(1); });