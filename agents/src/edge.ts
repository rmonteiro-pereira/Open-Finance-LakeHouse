import { timingSafeEqual } from "node:crypto";
import { actor, actorId, createChat, type ConversationStore, type ModelPort, type TranscriptPort, type CallLedger } from "@inkwell/core";
import { createChatServer } from "@inkwell/server";
import { CATALOG, SPECIALISTS, oflAgent, type Specialist } from "./agents";
import type { ResearchSource } from "./source";
import {guidedDocuments,guidedDataset} from "./guided";
import { evidencePlanner, researchOffer } from "./planner";
export interface EdgeConfig {
  ledger?:CallLedger;
  gatewayToken: string; origin: string; model: ModelPort; modelName: string;
  guidedDocuments?:boolean; noThinking?: boolean; modelTimeoutMs?:number; turnTimeoutMs?:number; modelMaxTokens?:number;
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
    request = new Request(request,{signal:AbortSignal.any([request.signal,AbortSignal.timeout(config.turnTimeoutMs??110_000)])});
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
    if (url.pathname === "/api/agents" && request.method === "GET") return json({agents: CATALOG, mode: config.guidedDocuments ? "guided_source_research" : "grounded_excerpts", model: config.modelName});
    const match = /^\/api\/agents\/(market|archive|quality)(?:\/|$)/.exec(url.pathname);
    if (!match || !SPECIALISTS.includes(match[1] as Specialist)) return json({detail: "Not found"}, 404);
    const kind = match[1] as Specialist;
    const key = JSON.stringify([kind, identity]);
    let server = servers.get(key);
    if (!server) {
      // Bound memory without evicting active streamed turns or weakening durable ownership.
      if (servers.size >= 256) return json({detail: "Agent session capacity reached"}, 503);
      const resolvePassage=async (id:string,signal?:AbortSignal)=>{const value=await source.read(`passages/${id}`,{},signal) as {passage?:{text?:string}};return value.passage?.text;};
      const resolveBriefPassage=async(id:string,signal?:AbortSignal)=>{const value=await source.read(`passages/${id}`,{},signal) as {passage?:Record<string,unknown>};return value.passage;};
      const definition=oflAgent(kind,source);
      definition.offer=(ctx,specs)=>researchOffer(kind,ctx.userProse,specs);
      if(config.guidedDocuments)definition.replyStep=async(ctx,step)=>(await guidedDataset(ctx,step,ctx.userProse))??guidedDocuments(ctx,step,ctx.userProse,resolveBriefPassage);
      const chat = createChat({agent:definition, model:evidencePlanner(config.model,kind,resolvePassage,config.noThinking?"\n/no_think":""),
        roles:{research:{model:config.modelName,maxTokens:config.modelMaxTokens??2048,timeoutMs:config.modelTimeoutMs??60_000},interpretation:{model:config.modelName,maxTokens:128,timeoutMs:45_000}},
        ledger:config.ledger,conversations:config.conversations(kind),transcript:config.transcript?.(kind),
        policy:{maxModelSteps:6,maxToolCallsPerTurn:12,policyVersion:"ofl/typed-source-workflow/3"},
      });
      server = createChatServer({chat, prefix: `/api/agents/${kind}`, authenticate: async () => actor(identity)});
      servers.set(key, server);
    }
    try { return await server.fetch(request); }
    catch { return json({detail: "Agent temporarily unavailable"}, 503); }
  };
}
