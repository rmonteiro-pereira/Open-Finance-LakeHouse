import { describe, expect, test } from "bun:test";
import { actor, createChat, memoryConversationStore } from "@inkwell/core";
import { scriptedModel } from "@inkwell/core/testkit";
import { oflAgent, researchTools, type Specialist } from "./agents";
import { createOflEdge } from "./edge";
import {evidencePlanner} from "./planner";
import { oflSource, type ResearchSource } from "./source";

const input = {symbol: "PETR4", market: "010", date_from: "2026-01-05", date_to: "2026-01-05", limit: 30};
function source(value: unknown): ResearchSource { return {read: async () => value}; }
describe("OFL specialist agents", () => {
  test("preserves exact quote strings, release, currency and quotation basis", async () => {
    const tools = researchTools(source({release: "r1", qualification: "unadjusted", rows: [{date: "2026-01-05", symbol: "PETR4", currency: "R$", close: "9007199254740993.123456789012", quotation_factor: 100, close_per_unit: "90071992547409.931234567890", record_id: "record1"}]}));
    const result = await tools.market[0]!.run(input, {});
    expect(result).toMatchObject({rows: [{close: "9007199254740993.123456789012", release: "r1", quotation_factor: 100, evidence: "record1"}]});
  });
  test("rejects reversed dates before contacting OFL", async () => {
    let calls = 0;
    const tools = researchTools({read: async () => {calls++; return {};}});
    await expect(tools.market[0]!.run({...input, date_from: "2027-01-01"}, {})).rejects.toThrow("date_from");
    expect(calls).toBe(0);
  });
  test("archive result has passage, page, asset and model revision references", async () => {
    const tool = researchTools(source({mode: "hybrid", model_revision: "pinned", results: [{id: "p1", item_id: "i1", asset_id: "a1", page: 2, snippet: "Evidence", title: "Bulletin"}]})).archive[0]!;
    expect(await tool.run({q: "question", mode: "hybrid", limit: 8}, {})).toMatchObject({rows: [{citation: "item:i1;passage:p1;asset:a1;page:2", model_revision: "pinned", original_link: "/api/research/assets/a1"}]});
  });
  test("all specialists are read-only with isolated tool catalogs", () => {
    const tools = researchTools(source({}));
    for (const definitions of Object.values(tools)) for (const tool of definitions) expect(tool.effect).toBe("read_only");
    expect(tools.quality.some(t => t.name === "read_quotes")).toBe(false);
    expect(tools.market.some(t => t.name === "inspect_ocr_review")).toBe(false);
  });
  test("an actual Inkwell turn delivers evidence and persists ownership", async () => {
    const model = scriptedModel([{content: "Vou consultar a fonte.", toolCalls: [{name: "read_quotes", arguments: input}]}]);
    const conversations = memoryConversationStore();
    const chat = createChat({agent: oflAgent("market", source({release: "r1", rows: [{close: "30.20"}]})), model, conversations,
      roles: {research: {model: "test"}}, policy: {maxModelSteps: 1}});
    const result = await chat.turn(actor("ana"), {conversationId: "investigation", message: "PETR4 mercado 010 em 2026-01-05"});
    expect(JSON.stringify(result)).toContain('30.20');
    expect(model.requests).toHaveLength(1);
    await expect(chat.get(actor("bia"), "investigation")).rejects.toThrow();
    expect((await chat.get(actor("ana"), "investigation")).messages.length).toBeGreaterThan(0);
  });
  test("source rejects arbitrary paths, follows no redirects and hides internal errors", async () => {
    let calls = 0;
    const client = oflSource("http://localhost:8081", "gateway", "ana", (async (_url, init) => {
      calls++; expect(init?.redirect).toBe("error"); throw new Error("password=hidden");
    }));
    await expect(client.read("../admin")).rejects.toThrow("source_query");
    expect(calls).toBe(0);
    await expect(client.read("coverage")).rejects.toThrow("source_access");
  });
  test("edge rejects forged gateway and cross-origin writes, rechecks revoked roles", async () => {
    let allowed = true;
    const stores = new Map<Specialist, ReturnType<typeof memoryConversationStore>>();
    const edge = createOflEdge({gatewayToken: "secret", origin: "https://research.test", modelName: "test", model: scriptedModel([]),
      source: identity => ({read: async () => ({identity, role: allowed ? "reader" : "revoked"})}),
      conversations: kind => {if (!stores.has(kind)) stores.set(kind, memoryConversationStore()); return stores.get(kind)!;},
    });
    const headers = {"x-ofl-gateway-token": "secret", "x-ofl-identity": "ana", "origin": "https://research.test", "content-type": "application/json"};
    expect((await edge(new Request("http://service/api/agents"))).status).toBe(401);
    expect((await edge(new Request("http://service/api/agents/market/conversations", {method: "POST", headers: {...headers, origin: "https://evil.test"}, body: "{}"}))).status).toBe(403);
    expect((await edge(new Request("http://service/api/agents/market/conversations", {method: "POST", headers, body: JSON.stringify({conversationId: "c1"})}))).status).toBe(200);
    allowed = false;
    expect((await edge(new Request("http://service/api/agents/market/conversations/c1", {headers}))).status).toBe(403);
  });
});

test("planner cannot present invented financial assertions as a generated reading", async () => {
  const model = scriptedModel([{content: "Invented price 9999 and invented citation."}]);
  const result = await evidencePlanner(model, "market").complete({model: "test", messages: [{role: "user", content: "price?"}]});
  expect(result.content).not.toContain("9999");
  expect(result.content).toContain("intervalo de datas");
});
