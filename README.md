# LulaEdge

**Query hundreds of Cloudflare D1 databases as if they were a single relational system**

**LulaEdge orchestrates distributed SQL execution entirely inside your Cloudflare account using Cloudflare Workers**

---

# Why LulaEdge?

- Keep data geographically local
- Query globally across shards
- Avoid centralized Postgres infrastructure
- Built entirely on Cloudflare Workers
- Designed for multi-tenant and AI workloads
---
## How it works

- **Engine** The LulaEdge engine only generates signed execution plans. Data queries execute entirely inside your Cloudflare account.
- **Your cluster** (deployed by this script) --> Orchestrator + Executors live in your Cloudflare account. Only they talk to your D1 shards.

```
Your App → Engine (sign plan) → Orchestrator (execute) → Executors → D1 Shards
```

```
sql
SELECT region, SUM(revenue)
FROM tenant_orders
GROUP BY region
```
Execute across 100+ distributed D1 shards in parallel.

---

## Quick Start

### Prerequisites

- Cloudflare account with D1 databases (your shards)
- Node.js 18+
- Cloudflare token permissions:
  * Account - Workers Scripts - Edit
  * Account - D1 - Edit
  * Account - Account Settings Read (Optional, but helps to Wrangler)

### 1. Clone and install

```bash
git clone https://github.com/your-org/lulaedge.git
cd lulaedge
npm install
```

### 2. Configure

```bash
.env
# Edit .env with your Cloudflare credentials: CF_ACCOUNT_ID and CLOUDFLARE_API_TOKEN
```

### 3. Deploy

```bash
node scripts/deploy.js
```

That's it. The script will:
- Detect all your D1 databases
- Deploy executor workers 
- Deploy the orchestrator
- Register you with the LulaEdge Engine
- Print your orchestrator URL
- Print your lulaEdge API_KEY

### 4. Open the console

Go to **[lulaedgeui.pages.dev](https://lulaedgeui.pages.dev)** and paste your orchestrator URL and API_KEY

---

## Strategies

| Strategy    | What it does                                                                     |
|-------------|----------------------------------------------------------------------------------|
| `join`      | Fetch rows from master DB, look them up across all shards                        |
| `agg`       | SUM / AVG / COUNT / MIN / MAX across all shards                                  |
| `scatter`   | Broadcast a SELECT to all shards and collect results                             |
| `migrate`   | Add / Rename column across all shards                                            |
| `telemetry` | Get global telemetry across all shards checking the Capacity, Status, Latency... |
---

## Architecture

```

Your Cloudflare Account
└── Orchestrator Worker
    ├── D1: lulaedge-catalog (shard map, ranges,...)
    └── Service bindings to Executors

    └── Executor Workers 
        └── Service bindings to D1 shards
```

---

## Limits (Free / Beta)

- 50 queries/day
- 100 shards

---

## Security

- **Your data never leaves your account.** Executors talk only to your D1 shards.

# LulaEdge MCP Server (Model Context Protocol)

An MCP server that bridges AI tools (like **Cursor Desktop** or **Claude Desktop**) with the **LulaEdge** distributed shard architecture. It allows LLMs to inspect cluster health, auto-generate plans, and execute queries or migrations using natural language.

---

### 🛠️ Cursor Desktop Configuration

1. Open **Cursor Desktop** and navigate to **Settings** (`Ctrl + ,` or `Cmd + ,`).
2. Go to **Features** > **MCP**.
3. Click **+ Add New MCP Server** and fill in the fields:
  * **Name:** `LulaEdge`
  * **Type:** `stdio`
  * **Command:** `node` (or `bun` / `npx`)
  * **Args:** Absolute path to your compiled file (e.g., `/Users/path/to/lulaedge-mcp/build/index.js`)

### 🔑 Required Environment Variables
Ensure these are set in your environment or passed as environment args in Cursor:
* `LULA_API_KEY`: Your private LulaEdge access key.
* `LULA_ORCHESTRATOR_URL`: Endpoint of your Cloudflare Worker Orchestrator.
---

### 🧰 Available Tools

The schema enforces strict validation (`additionalProperties: false`) to eliminate LLM hallucinations.

### 1. `get_fleet_status`
* **Description:** Retrieves real-time health, size, and latency metrics for all shards.
* **Prompts:** *"Show me the cluster health"* or *"Are any shards down?"*

### 2. `execute_cluster_query`
* **Description:** Runs distributed operations across the cluster. Supports `scatter`, `agg`, `join`, and `migration` strategies.
* **Prompts:** * *"Run a scatter strategy on table local_stock."* (Engine auto-generates the base SQL).
  * *"Calculate the SUM aggregate of the field 'price' on 'local_stock'."*
  * *"Add a column 'discount' (INT) to table 'products' using a migration."* (Enforces strict `migration_config` structure).

---

### 🛑 Structured Error Handling

When an operation fails, the server responds with a structured JSON object so the LLM can pinpoint the exact lifecycle failure state (`phase`) and self-correct when applicable:

```json
{
  "status": "failed",
  "mcp_contract": "1.1.0",
  "phase": "engine_plan_generation",
  "error": {
    "type": "LulaEdgeFlowError",
    "message": "Engine Plan Rejection [Status 403]: Invalid API Key"
  }