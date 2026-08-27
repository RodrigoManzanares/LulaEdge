# LulaEdge

**Operational Truth for Agentic Systems**

**Humans, agents, workflows and services now collaborate on the same operational entities**

---

# Why LulaEdge?

- **Location Hint:** Keep data geographically local
- **Agent Lineage:** Trace every decision across agents, humans, workflows and services
- **Ownership:** Know who owns every operational decision
- **Auditability:** Track every state change and every actor involved
- **Shared State:** Humans and agents collaborate using the same operational truth
- **Decision History:** Understand how an entity reached its current state
- **Accountability:** Connect outcomes to the decisions that produced them
---
## How it works

**Not another database. Not another observability platform**

  LulaEdge sits above your existing systems. It becomes the operational layer where entities evolve, decisions are recorded, ownership is established and accountability becomes visible


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

## Distributed Document Collaboration
LulaEdge v2.1+ includes native support for Stateful Real-Time Documents. This allows multiple users (or AI Agents) to edit a JSON document concurrently with ultra-low latency, while LulaEdge automatically handles the underlying DDL and persistence to your distributed D1 shards via Enterprise Snapshotting.

### 1. Create a Document
To instantiate a new document, you request a signed plan from the Engine using the `create_document`   strategy. The Engine will automatically generate the DDL (`documents` and `document_history` tables) on the target D1 shard if they don't exist.

Request to Engine:

```bash
{
  "payload": {
    "strategy": "create_document",
    "target_shard": "eu-west-shard-1",
    "tenant_id": "org_123",
    "document": { "title": "My Shared Doc", "content": "..." },
    "schema": { "type": "article" },
    "metadata": { "author": "Alice" }
  }
}

```
The Orchestrator will execute the DDL and return a DO_stub (the unique ID of the Document).

### 2. Live Sync & Concurrency

Once created, you can interact with the Document Object via the Orchestrator using the live_sync strategy. The Document Object holds the hot state in RAM (< 5ms latency).

Patching via HTTP:

```bash
{
  "strategy": "live_sync",
  "do_payload": {
    "stub_id": "<DO_stub_id>",
    "action": "patch",
    "patch": { "content": "Updated content..." },
    "version": null 
  }
}

```
Real-time WebSockets:
For sub-millisecond collaboration, connect directly to the Orchestrator via WebSocket:

```bash
ws://your-orchestrator.workers.dev?stub_id=<DO_stub_id>
```
### 3. Enterprise Snapshotting (Hot/Cold Backup)
You never have to worry about choking your D1 database with thousands of concurrent keystrokes.
The LulaEdge Documents implements a Write-Behind Coalescing mechanism. It absorbs all parallel edits in RAM and flushes them to D1:

* **Hot State:** Every 10 seconds of inactivity, it runs an UPDATE on the documents table.

* **Cold History:** It generates an INSERT into document_history only when mathematically sensible:

  * version_delta >= 100 (100+ edits)

  * time_delta >= 5 mins

  * change_ratio >= 20% (Document grew/changed significantly).

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