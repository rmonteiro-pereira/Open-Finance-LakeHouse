"use client";
import {readSourceSelection,sourceSelectionMessage} from "./source-selection";
import {AgentDatasetChoices,AgentDatasetResult} from "./agent-dataset";
import { useEffect, useRef, useState } from "react";
type Row = Record<string, unknown>;
type Table = {kind: string; title?: string; columns?: {key: string; label: string}[]; rows?: Row[]; truncated?: boolean};
type Message = {id: string; role: string; content: string; toolCalls?: {name:string;arguments:Row}[]; toolResults?: {widget?: Table; refusal?: {explanation?: string}; error?: unknown}[]};
type Conversation = {conversationId?: string; id?: string; title?: string};
const specialists = [{id: "market", title: "Pesquisa financeira"}, {id: "archive", title: "Dados e métodos"}, {id: "quality", title: "Qualidade dos dados"}];
async function api(path: string, body?: unknown, signal?: AbortSignal) {
  const result = await fetch(`/api/agents/${path}`, {method: body === undefined ? "GET" : "POST", headers: body === undefined ? {} : {"Content-Type": "application/json"}, body: body === undefined ? undefined : JSON.stringify(body), cache: "no-store", signal});
  const data = await result.json();
  if (!result.ok) throw new Error(data.detail ?? data.message ?? "Não foi possível acessar o agente");
  return data.data ?? data;
}
function messageContent(message:Message):string {
  const chosen=message.role==='user'?readSourceSelection(message.content):undefined;
  if(chosen)return `Pergunta: ${chosen.question}\nFontes escolhidas: ${chosen.passage_ids.length} passagens para releitura; a escolha não confirma pertinência.`;
  if(message.role==='user'&&message.content.includes('OFL_DATASET_REQUEST:'))try{
    const spec=JSON.parse(message.content.split('OFL_DATASET_REQUEST:')[1]);
    if(Array.isArray(spec.series))return `Receita confirmada: ${spec.series.map((s:Row)=>`${s.series_id} (${s.operation}, janela ${s.window}, defasagem ${s.lag})`).join('; ')}. Período: ${spec.date_from} a ${spec.date_to}. Alinhamento: ${spec.alignment}. Versão: ${spec.snapshot_id}. Valores ausentes permanecem nulos.`;
  }catch{/* Keep the original invalid request inspectable. */}
  return message.content;
}
function value(v: unknown): string { return v == null ? "" : typeof v === "object" ? JSON.stringify(v) : String(v); }
function Evidence({table}: {table: Table}) {
  const [inspection,setInspection]=useState(false);
  if (table.kind !== "result_table") return null;
  return <section className="card"><h3>{table.title ?? "Evidência"}</h3><button onClick={()=>setInspection(!inspection)}>{inspection?"Voltar à leitura":"Inspecionar estrutura e valores"}</button>{inspection&&<pre>{JSON.stringify(table,null,2)}</pre>}
    {table.title==='Dataset reproduzível · amostra e manifesto'&&table.rows?.map((row,i)=><AgentDatasetResult key={i} row={row}/>)}
    {table.truncated && <p className="muted">Recorte limitado. Estas linhas não representam todo o período.</p>}
    {!table.rows?.length ? <p>Nenhum registro encontrado neste recorte.</p> : <div style={{overflowX: "auto"}}><table><thead><tr>{table.columns?.map(c => <th key={c.key}>{c.label}</th>)}</tr></thead>
      <tbody>{table.rows.map((row, i) => <tr key={i}>{table.columns?.map(c => <td key={c.key} style={{maxWidth: 420, whiteSpace: "pre-wrap", overflowWrap: "anywhere"}}>
        {["item_link", "original_link"].includes(c.key) && typeof row[c.key] === "string" && /^\/api\/research\/(items|assets)\/[a-zA-Z0-9_-]+$/.test(String(row[c.key])) ? <a href={String(row[c.key])} target="_blank" rel="noreferrer">Abrir origem</a> : value(row[c.key])}
      </td>)}</tr>)}</tbody></table></div>}
  </section>;
}
export default function Agents({onDossier,selection}:{selection?:{question:string;passageIds:string[]};onDossier?:(draft:{question:string;references:string[];scope:Record<string,unknown>})=>void} = {}) {
  const [specialist, setSpecialist] = useState("market");
  const [conversationId, setConversationId] = useState("");
  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [messages, setMessages] = useState<Message[]>([]);
  const [question, setQuestion] = useState(selection?.question??"");
  const [manualIds,setManualIds]=useState<string[]>(selection?.passageIds??[]);
  const [audience,setAudience]=useState("analista");const [scopeFrom,setScopeFrom]=useState("");const [scopeTo,setScopeTo]=useState("");
  const [interpretation,setInterpretation]=useState(false);
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
    await run(async () => {const data = await api(`${specialist}/conversations/${encodeURIComponent(id)}`); setConversationId(id); setMessages(data.messages ?? []); const latest=[...(data.messages??[])].reverse().find((m:Message)=>m.role==="user");setManualIds(readSourceSelection(latest?.content??"")?.passage_ids??[]);setQuestion("");});
  }
  async function send(explicitQuestion?:string) {
    const submitted=explicitQuestion??question;
    if (!submitted.trim() || busy) return;
    if(scopeFrom && scopeTo && scopeFrom>scopeTo){setError("A data inicial deve ser anterior à data final.");return;}
    await run(async () => {
      const controller = new AbortController(); request.current = controller;
      let inquiry=submitted;
      if(manualIds.length&&explicitQuestion===undefined){
        const catalog=await api('catalog',undefined,controller.signal);
        if(catalog.capabilities?.selected_sources!==true||!Number.isInteger(catalog.capabilities?.max_selected_passages)||catalog.capabilities?.max_selected_passages<manualIds.length)throw Error('O agente disponível ainda não suporta esta seleção. As fontes não foram enviadas; tente novamente após a atualização ou escolha busca automática.');
        inquiry=sourceSelectionMessage(submitted,manualIds);
      }
      let id = conversationId;
      if (!id) {id = crypto.randomUUID(); await api(`${specialist}/conversations`, {conversationId: id}, controller.signal); setConversationId(id);}
      const data = await api(`${specialist}/turn`, {conversationId: id, message: `Público: ${audience}. Objetivo: ${interpretation?"fontes e hipóteses para revisão":"coletar e conferir fontes, sem hipóteses"}. Escopo solicitado: ${scopeFrom||"início não definido"} a ${scopeTo||"fim não definido"}. Pergunta: ${inquiry}`}, controller.signal);
      // Fetch the committed conversation, including earlier evidence, after each successful turn.
      const saved = await api(`${specialist}/conversations/${id}`, undefined, controller.signal);
      setMessages(saved.messages ?? []); setQuestion(""); setStatus(data.outcome === "degraded" ? "A consulta terminou com limitações; confira as evidências." : "Consulta salva.");
      const list = await api(`${specialist}/conversations`, undefined, controller.signal); setConversations(list.conversations ?? []);
    });
  }
  function handoff(target: string) {
    const original = [...messages].reverse().find(m => m.role === "user")?.content ?? question;
    const selected=readSourceSelection(original);
    const lastQuestion=selected?.question??original;
    setManualIds(selected?.passage_ids??[]);
    if(selected){setScopeFrom("");setScopeTo("");}
    const evidence = messages.flatMap(m => m.toolResults ?? []).flatMap(r => r.widget?.rows ?? []).map(row => Object.fromEntries(
      ["citation", "evidence", "release", "symbol", "tpmerc", "currency", "quotation_factor", "date", "item_link"].filter(key => row[key] !== undefined).map(key => [key, row[key]]),
    )).filter(row => Object.keys(row).length).slice(-12);
    setQuestion(selected?selected.question:`Transferência do especialista ${specialist}. Pergunta: ${lastQuestion}\nReferências preservadas (revalide nas fontes): ${JSON.stringify(evidence)}`.slice(0, 8000));
    selectSpecialist(target); setStatus("Pergunta e referências transferidas. Confira antes de investigar.");
  }
  function downloadMarkdown() {
    const sections = messages.filter(m => m.role !== "system").map(m => {
      const evidence = (m.toolResults ?? []).flatMap(result => result.widget?.rows ?? []);
      return `## ${m.role === "user" ? "Pergunta" : "Fonte / agente"}\n\n${messageContent(m)}\n\n${evidence.length ? "```json\n" + JSON.stringify(evidence, null, 2) + "\n```" : ""}`;
    });
    const text = `# Pesquisa OFL\n\nEspecialista: ${specialist}\nInvestigação: ${conversationId}\n\nEvidências consultadas; preços originais sem ajuste. Resultados limitados não representam cobertura completa.\n\n${sections.join("\n\n")}`;
    const url = URL.createObjectURL(new Blob([text], {type: "text/markdown;charset=utf-8"}));
    const link = document.createElement("a"); link.href = url; link.download = `ofl-${specialist}-${conversationId}.md`; link.click(); URL.revokeObjectURL(url);
  }
  const catalogRows=[...new Map(messages.flatMap(m=>m.toolResults??[]).filter(r=>r.widget?.title==='Catálogo de dados macro').flatMap(r=>r.widget?.rows??[]).map(r=>[`${r.snapshot_id}-${r.key}`,r])).values()];
  const references=[...new Set(messages.flatMap(m=>m.toolResults??[]).flatMap(r=>r.widget?.rows??[]).flatMap(row=>typeof row.passage_id==="string"?[row.passage_id]:typeof row.id==="string"&&typeof row.text==="string"?[row.id]:[]))].filter(id=>/^[a-zA-Z0-9_-]{1,128}$/.test(id)).slice(0,50);
  function dossier(){const raw=[...messages].reverse().find(m=>m.role==="user")?.content??"";const question=readSourceSelection(raw)?.question??raw.replace(/^[\s\S]*?Pergunta:\s*/,"");onDossier?.({question,references,scope:{conversation_id:conversationId,specialist,tool_calls:messages.flatMap(m=>m.toolCalls??[])}});}
  function download() {
    const blob = new Blob([JSON.stringify({specialist, conversationId, messages}, null, 2)], {type: "application/json"});
    const url = URL.createObjectURL(blob); const link = document.createElement("a"); link.href = url; link.download = `ofl-${specialist}-${conversationId || "pesquisa"}.json`; link.click(); URL.revokeObjectURL(url);
  }
  return <div className="research-agent">
    <section className="card"><h2>Agentes de pesquisa OFL</h2><p>Investigações privadas, salvas por usuário. As consultas apresentam evidências da fonte; valores monetários mantêm sua precisão original.</p>
      <div className="filters"><label>Especialista<select value={specialist} disabled={busy} onChange={e => selectSpecialist(e.target.value)}>{specialists.filter(s=>!manualIds.length||s.id!=="quality").map(s => <option key={s.id} value={s.id}>{s.title}</option>)}</select></label>
      <label>Investigação salva<select value={conversationId} disabled={busy} onChange={e => e.target.value ? void open(e.target.value) : (setConversationId(""), setMessages([]),setManualIds([]),setQuestion(""))}><option value="">Nova investigação</option>{conversations.map(c => {const id = c.conversationId ?? c.id ?? ""; return <option key={id} value={id}>{c.title ?? id}</option>;})}</select></label>
      <button disabled={!references.length||busy} onClick={dossier}>Criar dossiê com estas fontes</button><button disabled={!messages.length} onClick={download}>Exportar evidências JSON</button><button disabled={!messages.length} onClick={downloadMarkdown}>Exportar relatório</button></div>
      <p className="muted">Para pesquisar um tema, comece pela pergunta. Cotações exigem ativo, mercado e período; datas dos documentos são de publicação. Hipóteses interpretativas precisam de revisão.</p>
    </section>
    {messages.length > 0 && <section className="card"><h3>Consultar outro especialista</h3><p>A pergunta e as referências serão copiadas para uma nova investigação. O histórico original permanece salvo.</p>{specialists.filter(s => s.id !== specialist&&(!manualIds.length||s.id!=="quality")).map(s => <button key={s.id} disabled={busy} onClick={() => handoff(s.id)}>{s.title}</button>)}</section>}
    {manualIds.length>0&&<section className="card" aria-label="Fontes escolhidas para o assistente"><h3>Investigar a seleção</h3><p>{manualIds.length} passagens escolhidas serão relidas nas fontes, sem filtro adicional de publicação. Confira as datas e descreva o horizonte na pergunta. A escolha é do autor e não confirma pertinência, apoio ou comparabilidade.</p><button disabled={busy} onClick={()=>setManualIds([])}>Usar busca automática</button></section>}
    {error && <p role="alert" className="status error">{error}</p>}{status && <p role="status">{status}</p>}
    <div aria-live="polite">{messages.filter(m => m.role !== "system").map(m => <article className="card" key={m.id}><strong>{m.role === "user" ? "Você" : m.role === "tool" ? "Fonte OFL" : "Agente"}</strong><p style={{whiteSpace: "pre-wrap",overflowWrap:"anywhere"}}>{messageContent(m)}</p>{m.toolResults?.map((result, i) => <div key={i}>{result.widget && <Evidence table={result.widget}/>} {result.refusal?.explanation && <p>{result.refusal.explanation}</p>}{result.error != null && <p>Uma leitura falhou. Não há evidência para essa parte da consulta.</p>}</div>)}</article>)}</div>
    {specialist!=="quality"&&catalogRows.length>0&&<AgentDatasetChoices key={conversationId} rows={catalogRows} onPrepare={send} busy={busy}/>}
    <form className="card" onSubmit={e => {e.preventDefault(); void send();}}><label>Como trabalhar?<select value={interpretation?"interpretation":"sources"} disabled={busy} onChange={e=>setInterpretation(e.target.value==="interpretation")}><option value="sources">Coletar e conferir fontes</option><option value="interpretation">Fontes e comparação assistida (experimental)</option></select></label><p className="muted">A coleta documental pode ser levada ao dossiê para sua análise. A comparação assistida sugere relações entre trechos; confira país, horizonte e apoio nas fontes antes de concluir.</p><div className="filters"><label>Para quem é a pesquisa?<select value={audience} onChange={e=>setAudience(e.target.value)}><option value="analista">Analista financeiro</option><option value="assessor ou consultor">Assessor ou consultor</option><option value="pessoa de dados">Pessoa de dados</option><option value="pessoa estudando mercados e sua carteira">Estou estudando mercados e minha carteira</option></select></label><label>Período a partir de<input type="date" disabled={manualIds.length>0} value={scopeFrom} onChange={e=>setScopeFrom(e.target.value)}/></label><label>Período até<input type="date" disabled={manualIds.length>0} value={scopeTo} onChange={e=>setScopeTo(e.target.value)}/></label></div><p className="muted">Confira pergunta, público e período antes de executar. O agente deve esclarecer o objetivo quando faltarem informações.</p><label htmlFor="research-question">Pergunta de pesquisa</label><textarea id="research-question" value={question} disabled={busy} maxLength={manualIds.length?1600:8000} rows={4} onChange={e => setQuestion(e.target.value)} placeholder={specialist === "market" ? "Consulte PETR4 no mercado 010 de 2026-01-05 a 2026-01-05" : "O que você quer investigar?"}/><button disabled={busy || !question.trim()} type="submit">{busy ? "Consultando fontes…" : "Investigar"}</button>{busy&&<button type="button" onClick={()=>request.current?.abort()}>Cancelar consulta</button>}</form>
  </div>;
}
