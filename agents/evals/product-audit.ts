/** Deterministic product counterexamples through OFL's actual Inkwell HTTP edge.
 * Scripted models and synthetic sources: NOT live-model or adoption evaluation.
 * Run: cd agents && bun evals/product-audit.ts
 */
import {memoryConversationStore} from "@inkwell/core";
import {scriptedModel, type ScriptStep} from "@inkwell/core/testkit";
import {createOflEdge} from "../src/edge";
import {researchTools, type Specialist} from "../src/agents";
import type {ResearchSource} from "../src/source";

type Row = Record<string, any>;
async function turn(kind: Specialist, question: string, steps: ScriptStep[], data: Record<string, unknown>) {
  const model = scriptedModel(steps);
  const reads: {path: string; params: unknown}[] = [];
  const source: ResearchSource = {async read(path, params) {
    if (path === "me") return {identity: "product-audit", role: "admin"};
    reads.push({path, params});
    return data[path] ?? {};
  }};
  const edge = createOflEdge({gatewayToken: "synthetic-evaluation-only", origin: "https://ofl-eval.test", modelName: "scripted-evaluation-only", model,
    source: () => source, conversations: () => memoryConversationStore()});
  const response = await edge(new Request(`https://ofl-eval.test/api/agents/${kind}/turn`, {method: "POST",
    headers: {"x-ofl-gateway-token": "synthetic-evaluation-only", "x-ofl-identity": "product-audit", "origin": "https://ofl-eval.test", "content-type": "application/json"},
    body: JSON.stringify({conversationId: crypto.randomUUID(), message: question})}));
  const body = await response.json() as Row;
  if (!response.ok) throw new Error(`Audit could not exercise edge (${response.status})`);
  return {data: body.data as Row, reads, modelCalls: model.requests.length};
}
const cases: Row[] = [];
const definition = await turn("market", "Qual a diferença entre data de observação e data de publicação?", [
  {content: "Data de observação é o período medido; data de publicação indica quando a informação foi divulgada."},
], {});
const definitionText = JSON.stringify(definition.data);
cases.push({id: "AE01", job: "Understand financial data semantics", kind: "structural_gap",
  criterion: "Preserve a relevant clarification instead of replacing every no-tool response with generic required fields",
  criterion_met: definitionText.includes("período medido"), observation: "A relevant scripted explanation is replaced by the fixed market-field request.", model_calls: definition.modelCalls});

const clarify = await turn("market", "Quero estudar Petrobras. Por onde começo?", [
  {content: "Você quer examinar resultados da empresa, preços das ações ou sua relação com petróleo e câmbio?"},
], {});
cases.push({id: "AE02", job: "Scope an investigation without knowing database codes", kind: "structural_gap",
  criterion: "Retain the user's goal-oriented clarification before requiring a three-digit market code",
  criterion_met: JSON.stringify(clarify.data).includes("resultados da empresa"), observation: "The planner discards the tailored scoping question."});

const archive = await turn("archive", "Encontre o documento relevante e confira a passagem no contexto antes de concluir.", [
  {content: "Vou buscar.", toolCalls: [{name: "search_archive", arguments: {q: "documento relevante", mode: "keyword", limit: 8}}]},
  {content: "Vou conferir o item encontrado.", toolCalls: [{name: "read_item", arguments: {item_id: "synthetic-item"}}]},
], {search: {mode: "keyword", results: [{id: "synthetic-passage", item_id: "synthetic-item", asset_id: "synthetic-asset", page: 2, title: "Synthetic evaluation document", snippet: "Synthetic source excerpt; not a real financial claim."}]},
  "items/synthetic-item": {item: {title: "Synthetic evaluation document"}, assets: [], pages: []}});
cases.push({id: "AE03", job: "Search, inspect context, then produce an evidence-backed result", kind: "structural_gap",
  criterion: "Allow a dependent second read using the item ID found by the first read",
  criterion_met: archive.reads.some(r => r.path === "items/synthetic-item"), reads: archive.reads.map(r => r.path), model_calls: archive.modelCalls,
  observation: "The one-planning-step policy ends the turn before the dependent read."});

const tools = researchTools({read: async () => ({})});
const offered = [...new Set(Object.values(tools).flat().map(t => t.name))];
cases.push({id: "AE04", job: "Combine OFL macro data, fund letters and news for a research brief", kind: "structural_gap",
  criterion: "Provide a route from specialist tools to all three existing upstream asset classes",
  criterion_met: offered.includes("read_macro") && offered.includes("search_letters") && offered.includes("search_news"), offered_tools: offered,
  observation: "Current catalogs cannot access the macro snapshot or the separately stored letters/news."});

const exact = "9007199254740993.123456789012";
const quote = await turn("market", "Mostre a cotação original do ativo SYNTH no mercado 010 em 2026-01-05.", [
  {content: "Preço inventado de 9999.", toolCalls: [{name: "read_quotes", arguments: {symbol: "SYNTH", market: "010", date_from: "2026-01-05", date_to: "2026-01-05", limit: 30}}]},
], {quotes: {release: "synthetic-release", qualification: "Original currency and quotation basis; unadjusted prices", rows: [{date: "2026-01-05", symbol: "SYNTH", tpmerc: "010", currency: "SYNTHETIC", close: exact, quotation_factor: 100, close_per_unit: "90071992547409.931234567890", record_id: "synthetic-record"}]}});
const quoteText = JSON.stringify(quote.data);
cases.push({id: "AE05", job: "Inspect precise, qualified source data without model-invented figures", kind: "existing_strength",
  criterion: "Preserve exact source price, release and basis while suppressing invented planner prose",
  criterion_met: quoteText.includes(exact) && quoteText.includes("synthetic-release") && quoteText.includes("quotation_factor") && !quoteText.includes("Preço inventado"),
  observation: "Source values and provenance survive the turn; invented planning prose is not displayed."});

cases.push({id: "AE06", job: "Inspect research without accidental publication or data mutation", kind: "existing_strength",
  criterion: "Offer only read-only tool effects", criterion_met: Object.values(tools).flat().every(t => t.effect === "read_only"),
  observation: "All currently offered specialist tools are read-only."});
console.log(JSON.stringify({method: "deterministic diagnostic through actual OFL edge; scripted model and synthetic sources", scope: "architecture constraints and preserved safeguards, not market demand or live-model quality", cases,
  summary: {demonstrated_gaps: cases.filter(c => c.kind === "structural_gap" && !c.criterion_met).length, demonstrated_strengths: cases.filter(c => c.kind === "existing_strength" && c.criterion_met).length}}, null, 2));
