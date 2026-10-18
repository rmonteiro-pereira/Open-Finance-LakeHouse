import { SQL } from "bun";
import { openAiCompatibleModel } from "@inkwell/core";
import { migratePostgres, postgresConversationStore, postgresTranscript } from "@inkwell/store-postgres";
import { SPECIALISTS } from "./agents";
import { createOflEdge } from "./edge";
import { ollamaModel } from "./ollama";
import { oflSource } from "./source";
function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} must be configured`);
  return value;
}
// Dedicated research database only. Never use the Airflow metadata database.
const sql = new SQL(required("OFL_RESEARCH_DATABASE_URL"));
const api = required("OFL_RESEARCH_API_URL");
const token = required("OFL_RESEARCH_GATEWAY_TOKEN");
const origin = required("OFL_RESEARCH_ORIGIN");
const modelName = required("OFL_AGENT_MODEL_NAME");
const model = process.env.OFL_AGENT_MODEL_PROTOCOL === "ollama"
  ? ollamaModel(required("OFL_AGENT_MODEL_BASE_URL"), process.env.OFL_AGENT_NO_THINK === "1")
  : openAiCompatibleModel({baseUrl: required("OFL_AGENT_MODEL_BASE_URL"), apiKey: required("OFL_AGENT_MODEL_API_KEY")});
function budget(name:string, fallback:number, maximum:number){const value=Number(process.env[name]??fallback);if(!Number.isInteger(value)||value<10_000||value>maximum)throw Error(`${name} outside supported bounds`);return value;}
const [database] = await sql`SELECT to_regclass('public.dag_run') AS airflow`;
if (database?.airflow) throw new Error("Refusing to migrate an Airflow metadata database");
for (const kind of SPECIALISTS) await migratePostgres(sql, {schema: `ofl_agent_${kind}`});
const fetch = createOflEdge({gatewayToken: token, origin, model, modelName, noThinking: process.env.OFL_AGENT_NO_THINK === "1", modelTimeoutMs:budget("OFL_AGENT_MODEL_TIMEOUT_MS",60_000,180_000), turnTimeoutMs:budget("OFL_AGENT_TURN_TIMEOUT_MS",110_000,600_000),
  source: identity => oflSource(api, token, identity),
  conversations: kind => postgresConversationStore(sql, {schema: `ofl_agent_${kind}`}),
  transcript: kind => postgresTranscript(sql, {schema: `ofl_agent_${kind}`}),
});
const server = Bun.serve({hostname: process.env.OFL_AGENT_HOST ?? "127.0.0.1", port: Number(process.env.OFL_AGENT_PORT ?? 8082), idleTimeout: 0, fetch});
console.info(`OFL research agent service listening on port ${server.port}`);
async function stop() { await server.stop(false); await sql.close(); }
process.on("SIGTERM", () => void stop());
process.on("SIGINT", () => void stop());
