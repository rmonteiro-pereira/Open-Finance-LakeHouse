import { SQL } from "bun";
import { openAiCompatibleModel } from "@inkwell/core";
import { migratePostgres, postgresConversationStore, postgresTranscript } from "@inkwell/store-postgres";
import { SPECIALISTS } from "./agents";
import { createOflEdge } from "./edge";
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
const model = openAiCompatibleModel({baseUrl: required("OFL_AGENT_MODEL_BASE_URL"), apiKey: required("OFL_AGENT_MODEL_API_KEY")});
const [database] = await sql`SELECT to_regclass('public.dag_run') AS airflow`;
if (database?.airflow) throw new Error("Refusing to migrate an Airflow metadata database");
for (const kind of SPECIALISTS) await migratePostgres(sql, {schema: `ofl_agent_${kind}`});
const fetch = createOflEdge({gatewayToken: token, origin, model, modelName,
  source: identity => oflSource(api, token, identity),
  conversations: kind => postgresConversationStore(sql, {schema: `ofl_agent_${kind}`}),
  transcript: kind => postgresTranscript(sql, {schema: `ofl_agent_${kind}`}),
});
const server = Bun.serve({hostname: process.env.OFL_AGENT_HOST ?? "127.0.0.1", port: Number(process.env.OFL_AGENT_PORT ?? 8082), idleTimeout: 120, fetch});
console.info(`OFL research agent service listening on port ${server.port}`);
async function stop() { await server.stop(false); await sql.close(); }
process.on("SIGTERM", () => void stop());
process.on("SIGINT", () => void stop());
