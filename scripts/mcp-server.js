#!/usr/bin/env node
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { CallToolRequestSchema, ListToolsRequestSchema } from "@modelcontextprotocol/sdk/types.js";

const API_KEY = process.env.LULA_API_KEY || process.argv[2]?.trim();
const ORCHESTRATOR_URL = process.env.LULA_ORCHESTRATOR_URL || process.argv[3]?.trim();
const ENGINE_URL = process.env.LULA_ENGINE_URL || "https://api.lulaedge.com";

const DEFAULT_MASTER_KEY = process.env.LULA_MASTER_KEY || "ID";
const DEFAULT_SHARD_KEY = process.env.LULA_SHARD_KEY || "CAT_ID";
const DEFAULT_MASTER_QUERY = process.env.LULA_MASTER_QUERY || "SHARDS QUERY (USE '?')";
const CONTRACT_VERSION = "1.1.0";

if (!API_KEY || !ORCHESTRATOR_URL) {
  console.error("Critical error: Login credentials are missing.");
  console.error("CLI: node mcp-server.js <API_KEY> <ORCHESTRATOR_URL>");
  console.error("ENV: Define LULA_API_KEY y LULA_ORCHESTRATOR_URL");
  process.exit(1);
}

const server = new Server(
  { name: "lulaedge-mcp", version: CONTRACT_VERSION },
  { capabilities: { tools: {} } }
);

server.setRequestHandler(ListToolsRequestSchema, async () => {
  return {
    tools: [
      {
        name: "get_fleet_status",
        description: "Retrieve the health and latency status of the shards in LulaEdge.",
        inputSchema: {
          type: "object",
          properties: {},
          additionalProperties: false
        }
      },
      {
        name: "execute_cluster_query",
        description: "Run a distributed query or structured migration in LulaEdge cluster.",
        inputSchema: {
          type: "object",
          properties: {
            strategy: { type: "string", enum: ["scatter", "agg", "join", "migration"] },
            shard_table: { type: "string", description: "Target table on the shards." },
            field: { type: "string", description: "Operation objective column." },
            aggregate: { type: "string", enum: ["SUM", "AVG", "COUNT", "MIN", "MAX"] },
            master_table: { type: "string" },
            master_key: { type: "string" },
            shard_key: { type: "string" },
            master_query: { type: "string" },
            query: { type: "string", description: "Optional SQL statement. If omitted, the Engine auto-generates the plan using shard_table." },
            migration_config: {
              type: "object",
              description: "Migration setup details. Mandatory strictly when strategy is 'migration'.",
              properties: {
                method: { type: "string", enum: ["ADD_COLUMN", "RENAME_COLUMN", "DROP_COLUMN", "CREATE_TABLE"] },
                table: { type: "string" },
                params: { type: "object" }
              },
              required: ["method"],
              additionalProperties: false
            },
            target_shards: {
              type: "array",
              items: { type: "string" },
              description: "Optional custom list of target shards (e.g. ['C1', 'C10'])."
            }
          },
          required: ["strategy", "shard_table"],
          additionalProperties: false,
          allOf: [
            {
              if: { properties: { strategy: { const: "migration" } } },
              then: { required: ["migration_config"] }
            }
          ]
        }
      }
    ]
  };
});

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;
  let currentPhase = "argument_validation";

  try {
    if (name === "get_fleet_status") {
      currentPhase = "engine_fleet_fetch";
      const res = await fetch(`${ENGINE_URL}/get-shards`, {
        headers: { "X-LulaEdge-Key": API_KEY }
      });
      if (!res.ok) throw new Error(`Engine unreachable: ${res.statusText}`);
      const data = await res.json();
      return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };

    } else if (name === "execute_cluster_query") {
      if (args.strategy === "migration" && !args.migration_config) {
        throw new Error("Validation Error: The 'migration' strategy strictly requires a 'migration_config' object.");
      }

      const payloadData = {
        strategy: args.strategy,
        shard_table: args.shard_table,
        orchestrator_url: ORCHESTRATOR_URL,
        _contract_version: CONTRACT_VERSION
      };

      if (args.field) payloadData.field = args.field;
      if (args.target_shards) payloadData.target_shards = args.target_shards;
      if (args.aggregate) payloadData.aggregate = args.aggregate;

      if (args.strategy === "migration") {
        payloadData.migration_config = {
          table: args.migration_config?.table || args.shard_table,
          method: args.migration_config?.method || "ADD_COLUMN",
          params: args.migration_config?.params || { new_column: args.field }
        };
      } else {
        if (args.query) payloadData.query = args.query;

        payloadData.master_key = args.master_key || DEFAULT_MASTER_KEY;
        payloadData.shard_key = args.shard_key || DEFAULT_SHARD_KEY;
        payloadData.master_query = args.master_query || DEFAULT_MASTER_QUERY;
        if (args.master_table) payloadData.master_table = args.master_table;
      }

      currentPhase = "engine_plan_generation";
      console.error(`--> [${currentPhase.toUpperCase()}] Requesting cryptographic signature...`);
      const planRes = await fetch(ENGINE_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-LulaEdge-Key": API_KEY },
        body: JSON.stringify({ payload: payloadData })
      });

      if (!planRes.ok) {
        throw new Error(`Engine Plan Rejection [Status ${planRes.status}]: ${await planRes.text()}`);
      }

      const plan = await planRes.json();
      if (plan.error) {
        throw new Error(`Engine logic block: ${plan.error} - ${plan.message || ''}`);
      }

      currentPhase = "orchestrator_cluster_execution";
      console.error(`--> [${currentPhase.toUpperCase()}] Broadcasting signed plan to shards...`);
      const executeRes = await fetch(ORCHESTRATOR_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-LulaEdge-Key": API_KEY },
        body: JSON.stringify(plan)
      });

      const responseText = await executeRes.text();
      if (!executeRes.ok) {
        throw new Error(`Orchestrator Execution Failure [Status ${executeRes.status}]: ${responseText}`);
      }

      const result = JSON.parse(responseText);
      return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
    }

    throw new Error(`Unknown tool execution attempt: ${name}`);
  } catch (error) {
    return {
      isError: true,
      content: [{
        type: "text",
        text: JSON.stringify({
          status: "failed",
          mcp_contract: CONTRACT_VERSION,
          phase: currentPhase,
          error: {
            type: "LulaEdgeFlowError",
            message: error.message
          }
        }, null, 2)
      }]
    };
  }
});

const transport = new StdioServerTransport();
await server.connect(transport);