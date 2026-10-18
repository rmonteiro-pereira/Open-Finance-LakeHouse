import { timingSafeEqual } from "node:crypto";
import { actor, actorId, createChat, type ConversationStore, type ModelPort, type TranscriptPort } from "@inkwell/core";
import { createChatServer } from "@inkwell/server";
import { CATALOG, SPECIALISTS, oflAgent, type Specialist } from "./agents";
import type { ResearchSource } from "./source";
import { evidencePlanner } from "./planner";
export interface EdgeConfig {
  gatewayToken: string; origin: string; model: ModelPort; modelName: string;
  source(identity: string): ResearchSource;
  conversations(kind: Specialist): ConversationStore;
  transcript?(kind: Specialist): TranscriptPort;
}
function equal(left: string, right: string) {
  const a = Buffer.from(left); const b = Buffer.from(right);
  return a.length === b.length && timingSafeEqual(a, b);
}
export function createOflEdge(config: EdgeConfig) {
  if (!config.gatewayToken || !config.origin || !config.modelName) throw new Error("Private gateway, origin and model must be configured");
  const servers = new Map<string, ReturnType<typeof createChatServer>>();
  const json = (value: unknown, status = 200) => Response.json(value, {status, headers: {"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"}});
  return async function handle(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (!equal(request.headers.get("x-ofl-gateway-token") ?? "", config.gatewayToken)) return json({detail: "Verified private gateway required"}, 401);
    const identity = actorId(request.headers.get("x-ofl-identity"));
    if (!identity) return json({detail: "Research identity required"}, 401);
    if (!["GET", "HEAD"].includes(request.method) && request.headers.get("origin") !== config.origin) return json({detail: "Same-origin request required"}, 403);
    const source = config.source(identity);
    // Recheck roles even for cached sessions: removing OFL access takes effect immediately.
    try {
      const me = await source.read("me") as {identity?: string; role?: string};
      if (me.identity !== identity || !["reader", "reviewer", "admin"].includes(me.role ?? "")) return json({detail: "Research access denied"}, 403);
    } catch { return json({detail: "Research authorization unavailable"}, 503); }
    if (url.pathname === "/api/agents" && request.method === "GET") return json({agents: CATALOG, mode: "grounded_excerpts", model: config.modelName});
    const match = /^\/api\/agents\/(market|archive|quality)(?:\/|$)/.exec(url.pathname);
    if (!match || !SPECIALISTS.includes(match[1] as Specialist)) return json({detail: "Not found"}, 404);
    const kind = match[1] as Specialist;
    const key = JSON.stringify([kind, identity]);
    let server = servers.get(key);
    if (!server) {
      // Bound memory without evicting active streamed turns or weakening durable ownership.
      if (servers.size >= 256) return json({detail: "Agent session capacity reached"}, 503);
      const chat = createChat({agent: oflAgent(kind, source), model: evidencePlanner(config.model, kind, async (id, signal) => {const value = await source.read(`passages/${id}`, {}, signal) as {passage?:{text?:string}}; return value.passage?.text;}),
        roles: {research: {model: config.modelName, maxTokens: 2048, timeoutMs: 60_000}},
        conversations: config.conversations(kind), transcript: config.transcript?.(kind),
        // Bounded search/context/extractive synthesis; source values remain exact tables.
        policy: {maxModelSteps: 6, maxToolCallsPerTurn: 12, policyVersion: "ofl/grounded-excerpts/2"},
      });
      server = createChatServer({chat, prefix: `/api/agents/${kind}`, authenticate: async () => actor(identity)});
      servers.set(key, server);
    }
    try { return await server.fetch(request); }
    catch { return json({detail: "Agent temporarily unavailable"}, 503); }
  };
}
