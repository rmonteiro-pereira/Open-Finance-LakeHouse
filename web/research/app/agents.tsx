"use client";
import { useEffect, useRef, useState } from "react";
type Row = Record<string, unknown>;
type Table = {kind: string; title?: string; columns?: {key: string; label: string}[]; rows?: Row[]; truncated?: boolean};
type Message = {id: string; role: string; content: string; toolResults?: {widget?: Table; refusal?: {explanation?: string}; error?: unknown}[]};
type Conversation = {conversationId?: string; id?: string; title?: string};
const specialists = [{id: "market", title: "Pesquisa financeira"}, {id: "archive", title: "Dados e métodos"}, {id: "quality", title: "Qualidade dos dados"}];
async function api(path: string, body?: unknown, signal?: AbortSignal) {
  const result = await fetch(`/api/agents/${path}`, {method: body === undefined ? "GET" : "POST", headers: body === undefined ? {} : {"Content-Type": "application/json"}, body: body === undefined ? undefined : JSON.stringify(body), cache: "no-store", signal});
  const data = await result.json();
  if (!result.ok) throw new Error(data.detail ?? data.message ?? "Não foi possível acessar o agente");
  return data.data ?? data;
}
function value(v: unknown): string { return v == null ? "" : typeof v === "object" ? JSON.stringify(v) : String(v); }
function Evidence({table}: {table: Table}) {
  const [inspection,setInspection]=useState(false);
  if (table.kind !== "result_table") return null;
  return <section className="card"><h3>{table.title ?? "Evidência"}</h3><button onClick={()=>setInspection(!inspection)}>{inspection?"Voltar à leitura":"Inspecionar estrutura e valores"}</button>{inspection&&<pre>{JSON.stringify(table,null,2)}</pre>}
    {table.truncated && <p className="muted">Recorte limitado. Estas linhas não representam todo o período.</p>}
    {!table.rows?.length ? <p>Nenhum registro encontrado neste recorte.</p> : <div style={{overflowX: "auto"}}><table><thead><tr>{table.columns?.map(c => <th key={c.key}>{c.label}</th>)}</tr></thead>
      <tbody>{table.rows.map((row, i) => <tr key={i}>{table.columns?.map(c => <td key={c.key} style={{maxWidth: 420, whiteSpace: "pre-wrap", overflowWrap: "anywhere"}}>
        {["item_link", "original_link"].includes(c.key) && typeof row[c.key] === "string" && /^\/api\/research\/(items|assets)\/[a-zA-Z0-9_-]+$/.test(String(row[c.key])) ? <a href={String(row[c.key])} target="_blank" rel="noreferrer">Abrir origem</a> : value(row[c.key])}
      </td>)}</tr>)}</tbody></table></div>}
  </section>;
}
export default function Agents() {
  const [specialist, setSpecialist] = useState("market");
  const [conversationId, setConversationId] = useState("");
  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [messages, setMessages] = useState<Message[]>([]);
  const [question, setQuestion] = useState("");
  const [audience,setAudience]=useState("analista");const [scopeFrom,setScopeFrom]=useState("");const [scopeTo,setScopeTo]=useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [status, setStatus] = useState("");
  const request = useRef<AbortController | null>(null);
  const generation = useRef(0);
  useEffect(() => {
    const current = ++generation.current;
    const controller = new AbortController();
    api(`${specialist}/conversations`, undefined, controller.signal).then(data => {if (current === generation.current) setConversations(data.conversations ?? []);}).catch(e => {if (!controller.signal.aborted) setError(e.message);});
    return () => {controller.abort(); request.current?.abort();};
  }, [specialist]);
  function selectSpecialist(target: string) {
    ++generation.current;
    request.current?.abort();
    setConversationId(""); setMessages([]); setConversations([]); setError(""); setStatus("");
    setSpecialist(target);
  }
  async function run(action: () => Promise<void>) {
    setBusy(true); setError("");
    try {await action();} catch (e) {setError(e instanceof Error ? e.message : "Falha no agente");} finally {setBusy(false);}
  }
  async function open(id: string) {
    await run(async () => {const data = await api(`${specialist}/conversations/${encodeURIComponent(id)}`); setConversationId(id); setMessages(data.messages ?? []);});
  }
  async function send() {
    if (!question.trim() || busy) return;
    await run(async () => {
      const controller = new AbortController(); request.current = controller;
      let id = conversationId;
      if (!id) {id = crypto.randomUUID(); await api(`${specialist}/conversations`, {conversationId: id}, controller.signal); setConversationId(id);}
      const data = await api(`${specialist}/turn`, {conversationId: id, message: `Público: ${audience}. Escopo solicitado: ${scopeFrom||"início não definido"} a ${scopeTo||"fim não definido"}. Pergunta: ${question}`}, controller.signal);
      // Fetch the committed conversation, including earlier evidence, after each successful turn.
      const saved = await api(`${specialist}/conversations/${id}`, undefined, controller.signal);
      setMessages(saved.messages ?? []); setQuestion(""); setStatus(data.outcome === "degraded" ? "A consulta terminou com limitações; confira as evidências." : "Consulta salva.");
      const list = await api(`${specialist}/conversations`, undefined, controller.signal); setConversations(list.conversations ?? []);
    });
  }
  function handoff(target: string) {
    const lastQuestion = [...messages].reverse().find(m => m.role === "user")?.content ?? question;
    const evidence = messages.flatMap(m => m.toolResults ?? []).flatMap(r => r.widget?.rows ?? []).map(row => Object.fromEntries(
      ["citation", "evidence", "release", "symbol", "tpmerc", "currency", "quotation_factor", "date", "item_link"].filter(key => row[key] !== undefined).map(key => [key, row[key]]),
    )).filter(row => Object.keys(row).length).slice(-12);
    setQuestion(`Transferência do especialista ${specialist}. Pergunta: ${lastQuestion}\nReferências preservadas (revalide nas fontes): ${JSON.stringify(evidence)}`.slice(0, 8000));
    selectSpecialist(target); setStatus("Pergunta e referências transferidas. Confira antes de investigar.");
  }
  function downloadMarkdown() {
    const sections = messages.filter(m => m.role !== "system").map(m => {
      const evidence = (m.toolResults ?? []).flatMap(result => result.widget?.rows ?? []);
      return `## ${m.role === "user" ? "Pergunta" : "Fonte / agente"}\n\n${m.content}\n\n${evidence.length ? "```json\n" + JSON.stringify(evidence, null, 2) + "\n```" : ""}`;
    });
    const text = `# Pesquisa OFL\n\nEspecialista: ${specialist}\nInvestigação: ${conversationId}\n\nEvidências consultadas; preços originais sem ajuste. Resultados limitados não representam cobertura completa.\n\n${sections.join("\n\n")}`;
    const url = URL.createObjectURL(new Blob([text], {type: "text/markdown;charset=utf-8"}));
    const link = document.createElement("a"); link.href = url; link.download = `ofl-${specialist}-${conversationId}.md`; link.click(); URL.revokeObjectURL(url);
  }
  function download() {
    const blob = new Blob([JSON.stringify({specialist, conversationId, messages}, null, 2)], {type: "application/json"});
    const url = URL.createObjectURL(blob); const link = document.createElement("a"); link.href = url; link.download = `ofl-${specialist}-${conversationId || "pesquisa"}.json`; link.click(); URL.revokeObjectURL(url);
  }
  return <>
    <section className="card"><h2>Agentes de pesquisa OFL</h2><p>Investigações privadas, salvas por usuário. As consultas apresentam evidências da fonte; valores monetários mantêm sua precisão original.</p>
      <div className="filters"><label>Especialista<select value={specialist} disabled={busy} onChange={e => selectSpecialist(e.target.value)}>{specialists.map(s => <option key={s.id} value={s.id}>{s.title}</option>)}</select></label>
      <label>Investigação salva<select value={conversationId} disabled={busy} onChange={e => e.target.value ? void open(e.target.value) : (setConversationId(""), setMessages([]))}><option value="">Nova investigação</option>{conversations.map(c => {const id = c.conversationId ?? c.id ?? ""; return <option key={id} value={id}>{c.title ?? id}</option>;})}</select></label>
      <button disabled={!messages.length} onClick={download}>Exportar evidências JSON</button><button disabled={!messages.length} onClick={downloadMarkdown}>Exportar relatório</button></div>
      <p className="muted">Mercado: informe ativo, código do mercado e período. Acervo: datas se referem ao catálogo. Qualidade: consultas não aprovam nem reprocessam dados.</p>
    </section>
    {messages.length > 0 && <section className="card"><h3>Consultar outro especialista</h3><p>A pergunta e as referências serão copiadas para uma nova investigação. O histórico original permanece salvo.</p>{specialists.filter(s => s.id !== specialist).map(s => <button key={s.id} disabled={busy} onClick={() => handoff(s.id)}>{s.title}</button>)}</section>}
    {error && <p role="alert" className="status error">{error}</p>}{status && <p role="status">{status}</p>}
    <div aria-live="polite">{messages.filter(m => m.role !== "system").map(m => <article className="card" key={m.id}><strong>{m.role === "user" ? "Você" : m.role === "tool" ? "Fonte OFL" : "Agente"}</strong><p style={{whiteSpace: "pre-wrap"}}>{m.content}</p>{m.toolResults?.map((result, i) => <div key={i}>{result.widget && <Evidence table={result.widget}/>} {result.refusal?.explanation && <p>{result.refusal.explanation}</p>}{result.error != null && <p>Uma leitura falhou. Não há evidência para essa parte da consulta.</p>}</div>)}</article>)}</div>
    <form className="card" onSubmit={e => {e.preventDefault(); void send();}}><div className="filters"><label>Para quem é a pesquisa?<select value={audience} onChange={e=>setAudience(e.target.value)}><option value="analista">Analista financeiro</option><option value="assessor ou consultor">Assessor ou consultor</option><option value="pessoa de dados">Pessoa de dados</option><option value="pessoa estudando mercados e sua carteira">Estou estudando mercados e minha carteira</option></select></label><label>Período a partir de<input type="date" value={scopeFrom} onChange={e=>setScopeFrom(e.target.value)}/></label><label>Período até<input type="date" value={scopeTo} onChange={e=>setScopeTo(e.target.value)}/></label></div><p className="muted">Confira pergunta, público e período antes de executar. O agente deve esclarecer o objetivo quando faltarem informações.</p><label htmlFor="research-question">Pergunta de pesquisa</label><textarea id="research-question" value={question} disabled={busy} maxLength={8000} rows={4} onChange={e => setQuestion(e.target.value)} placeholder={specialist === "market" ? "Consulte PETR4 no mercado 010 de 2026-01-05 a 2026-01-05" : "O que você quer investigar?"}/><button disabled={busy || !question.trim()} type="submit">{busy ? "Consultando fontes…" : "Investigar"}</button>{busy&&<button type="button" onClick={()=>request.current?.abort()}>Cancelar consulta</button>}</form>
  </>;
}
