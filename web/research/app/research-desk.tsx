"use client";
import {useRef, useState} from "react";
import Agents from "./agents";
import {downloadJson, researchApi, show} from "./workbench";
type Hit = {id: string; title: string; snippet: string; source: string; item_id: string; asset_id: string | null; page: number | null; collection: string; method: string; date_start: string | null; date_end: string | null; date_precision: string; content_tier: string | null; epistemic_status: string | null; source_url: string | null};
const labels: Record<string, string> = {news: "Notícia", manager_letter: "Carta de gestora", mub3: "MUB3", b3_bulletin: "Boletim B3"};
export default function ResearchDesk({onDossier}:{onDossier?:(draft:{question:string;references:string[];scope:Record<string,unknown>})=>void} = {}) {
  const [question, setQuestion] = useState(""); const [source, setSource] = useState("");
  const [from, setFrom] = useState(""); const [to, setTo] = useState("");
  const [hits, setHits] = useState<Hit[]>([]); const [selected, setSelected] = useState<Record<string, unknown> | null>(null);
  const [busy, setBusy] = useState(false); const [error, setError] = useState(""); const [searched, setSearched] = useState(false);
  const [mode,setMode] = useState("keyword"); const [chosen,setChosen] = useState<string[]>([]);const [chosenScopes,setChosenScopes]=useState<Record<string,Record<string,string>>>({});
  const [assistant, setAssistant] = useState(false); const [scope, setScope] = useState<Record<string, string>>({});
  const current = useRef(0); const inspection = useRef(0);
  async function search() {
    const version = ++current.current; ++inspection.current;
    setBusy(true); setError(""); setSelected(null);
    const params = {q: question, mode, limit: "20", ...(source ? {source} : {}), ...(from ? {date_from: from} : {}), ...(to ? {date_to: to} : {})};
    try {
      if (from && to && from > to) throw new Error("A data inicial precisa ser anterior à data final.");
      const result = await researchApi(`search?${new URLSearchParams(params)}`);
      if (version === current.current) {setHits(result.results.map((hit:Hit)=>({...hit,model_revision:result.model_revision,model:result.model,approximate:result.approximate}))); setScope(params); setSearched(true);}
    } catch(e) {if (version === current.current) setError(e instanceof Error ? e.message : "Falha na pesquisa");}
    finally {if (version === current.current) setBusy(false);}
  }
  async function inspect(hit: Hit) {
    const version = ++inspection.current; setError("");
    try {const data = await researchApi(`passages/${hit.id}`); if (version === inspection.current) setSelected(data.passage);}
    catch(e) {if (version === inspection.current) setError(e instanceof Error ? e.message : "Não foi possível abrir a evidência");}
  }
  return <>
    <section className="card"><h2>Investigue um tema nas fontes</h2><p>Busque notícias, cartas de gestoras e documentos. Os resultados são evidências para sua pesquisa; ainda não são uma análise ou uma conclusão automática.</p>
      <form onSubmit={e => {e.preventDefault(); void search();}}>
        <label htmlFor="evidence-question">Tema ou pergunta</label><input id="evidence-question" value={question} maxLength={1000} onChange={e => setQuestion(e.target.value)} placeholder="Inflação, juros, câmbio ou uma tese que você quer investigar"/>
        <div className="filters"><label>Modo de busca<select value={mode} onChange={e=>setMode(e.target.value)}><option value="keyword">Termos exatos</option><option value="hybrid">Termos e significado</option><option value="vector">Significado</option></select></label><label>Fontes<select value={source} onChange={e => setSource(e.target.value)}><option value="">Todas as fontes conectadas</option><option value="news">Notícias</option><option value="manager_letter">Cartas de gestoras</option><option value="b3_bulletin">Boletins B3</option><option value="mub3">Acervo MUB3</option></select></label>
          <label>Publicação a partir de<input type="date" value={from} onChange={e => setFrom(e.target.value)}/></label><label>Publicação até<input type="date" value={to} onChange={e => setTo(e.target.value)}/></label>
          <button disabled={busy || !question.trim()}>{busy ? "Buscando…" : "Buscar evidências"}</button></div>
      </form><p className="muted">O período se refere à data de publicação registrada, não ao pregão nem à coleta. Documentos sem data ficam fora dos filtros de período.</p>
    </section>
    {error && <p className="status error" role="alert">{error}</p>}
    {searched && <section className="card"><h3>{hits.length} evidências encontradas neste recorte</h3><p className="muted">Até 20 documentos por busca. Uma ausência de resultados não demonstra que um evento não aconteceu.</p><button disabled={!hits.length} onClick={() => downloadJson({scope, evidence: hits, qualification: "Retrieved evidence; not an analytical conclusion"}, "ofl-evidencias.json")}>Salvar referências</button></section>}
    {hits.map(hit => <article className="card" key={hit.id}><p className="eyebrow">{labels[hit.source] ?? hit.source} · {hit.collection || "coleção não informada"}</p><h3>{hit.title}</h3>
      <p>{hit.snippet}</p><p className="muted">Publicação: {hit.date_start ? `${hit.date_start}${hit.date_end !== hit.date_start ? ` a ${hit.date_end}` : ""}` : "data desconhecida"} · precisão: {show(hit.date_precision)} · extração: {hit.method}{hit.page ? ` · página ${hit.page}` : ""}</p>
      {hit.epistemic_status === "attributed_opinion" && <p>Opinião atribuída à gestora. Não representa um resultado verificado nem comprova desempenho.</p>}
      {hit.content_tier === "headline_only" && <p>Apenas manchete e resumo coletado. O texto integral não está disponível aqui.</p>}
      <button onClick={() => void inspect(hit)}>Inspecionar trecho</button><button onClick={()=>{setChosen(chosen.includes(hit.id)?chosen.filter(id=>id!==hit.id):[...chosen,hit.id]);setChosenScopes({...chosenScopes,[hit.id]:scope});}}>{chosen.includes(hit.id)?"Remover da seleção":"Selecionar para dossiê"}</button><details><summary>Por que este resultado?</summary><p>Posições e contribuições explicam a ordenação, não confiança financeira ou veracidade.</p><p>Modelo/revisão: {String((hit as unknown as Record<string,unknown>).model_revision??"não usado na busca por termos")}</p><p>Modo: {String((hit as unknown as Record<string,unknown>).search_mode??mode)} · método: {hit.method}</p><pre>{JSON.stringify((hit as unknown as Record<string,unknown>).ranking??{},null,2)}</pre></details>{hit.asset_id && <a className="evidence-link" href={`/api/research/assets/${hit.asset_id}`} target="_blank" rel="noreferrer">Abrir arquivo preservado</a>}
      {hit.source_url && /^https?:\/\//i.test(hit.source_url) && <a className="evidence-link" href={hit.source_url} target="_blank" rel="noreferrer">Abrir fonte</a>}
    </article>)}
    {chosen.length>0&&<section className="card"><h3>Referências para o dossiê</h3><p>Leve os trechos selecionados para um dossiê. Ao salvar, o servidor captura as passagens e sua origem.</p><button onClick={()=>onDossier?.({question:scope.q??question,references:chosen,scope:{selections:chosen.map(id=>({passage_id:id,query:chosenScopes[id]}))}})}>Criar dossiê com esta seleção</button><details><summary>Inspecionar referências</summary><textarea readOnly value={chosen.join(", ")}/></details><button onClick={()=>downloadJson({references:chosen,selections:chosen.map(id=>({passage_id:id,query:chosenScopes[id]}))},"ofl-selecao.json")}>Exportar seleção</button></section>}
    {selected && <section className="card" aria-label="Trecho inspecionado"><h3>{show(selected.title)}</h3><p style={{whiteSpace: "pre-wrap"}}>{show(selected.text)}</p><p className="muted">Referência: {show(selected.id)} · método: {show(selected.method)}{selected.active === false ? " · versão substituída" : ""}</p><button onClick={() => {++inspection.current; setSelected(null);}}>Fechar trecho</button></section>}
    <section className="card"><h3>Assistente de pesquisa</h3><p>O assistente planeja leituras, confere trechos e propõe interpretações para revisão. Consultas podem levar alguns minutos e podem ser canceladas. A busca direta e os dados também estão disponíveis sem usar o assistente.</p><button onClick={() => setAssistant(!assistant)}>{assistant ? "Fechar assistente" : "Abrir assistente"}</button></section>
    {assistant && <Agents onDossier={onDossier}/>}
  </>;
}
