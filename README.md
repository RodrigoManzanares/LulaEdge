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

