"use client";

import {useEffect, useState} from "react";

type Row = Record<string, unknown>;
export async function researchApi(path: string, signal?: AbortSignal) {
  const response = await fetch(`/api/research/${path}`, {cache: "no-store", signal});
  const result = await response.json();
  if (!response.ok) throw new Error(result.detail ?? "Não foi possível consultar os dados");
  return result;
}
export function show(value: unknown): string {return value == null ? "—" : String(value);}
export function downloadJson(value: unknown, filename: string) {
  const url = URL.createObjectURL(new Blob([JSON.stringify(value, null, 2)], {type: "application/json"}));
  const a = document.createElement("a"); a.href = url; a.download = filename; a.click(); URL.revokeObjectURL(url);
}
export type IndexCoverage = {configured:boolean; eligible_passages:number; indexed_passages:number; pending_passages:number; rejected_passages?:number; fraction:number|null; status:"disabled"|"empty"|"partial"|"complete"; model_revision:string|null};
export function SemanticCoverage({coverage}:{coverage:IndexCoverage|null|undefined}) {
  if(!coverage)return null;
  return <aside className="card" aria-label="Cobertura da busca semântica"><h3>Cobertura da busca semântica</h3>
    {!coverage.configured?<p>A busca por significado não está configurada. A busca por termos continua disponível.</p>:coverage.status==="empty"?<p>Não há passagens disponíveis neste recorte.</p>:<><p>{coverage.indexed_passages.toLocaleString("pt-BR")} de {coverage.eligible_passages.toLocaleString("pt-BR")} passagens com vetores ({((coverage.fraction??0)*100).toLocaleString("pt-BR",{maximumFractionDigits:1})}%).</p>
    {coverage.status==="partial"?<p>A busca por significado cobre parte deste recorte. A busca combinada também procura por termos nas passagens disponíveis, inclusive nas que ainda não têm vetores.</p>:<p>Todas as passagens disponíveis neste recorte têm vetores para a versão atual do modelo.</p>}
    {coverage.rejected_passages?<p>{coverage.rejected_passages.toLocaleString("pt-BR")} passagens excedem o contexto do modelo e ficam sem vetores até serem divididas; continuam encontráveis por termos.</p>:null}</>}
    <p className="muted">Contagem das passagens ativas nas fontes e datas selecionadas; os termos da pergunta não reduzem essa contagem. Cobertura de vetores não mede relevância, veracidade nem a completude do histórico original.</p>
  </aside>;
}
export default function Workbench({navigate}: {navigate: (tab: string) => void}) {
  const [data, setData] = useState<{documents: Row[]; macro: {snapshot_id: string; generated_at: string; series_with_data: number} | null; date_quality: Row[]; index_coverage:IndexCoverage} | null>(null);
  const [error, setError] = useState("");
  useEffect(() => {
    const controller = new AbortController();
    researchApi("workbench", controller.signal).then(setData).catch(e => {if (!controller.signal.aborted) setError(e.message);});
    return () => controller.abort();
  }, []);
  const letters = data?.documents.filter(row => row.source === "manager_letter").reduce((n, r) => n + Number(r.items), 0) ?? 0;
  const news = data?.documents.filter(row => row.source === "news").reduce((n, r) => n + Number(r.items), 0) ?? 0;
  const undated = data?.date_quality.filter(row => row.date_precision === "unknown").reduce((n, r) => n + Number(r.items), 0) ?? 0;
  return <>
    <section className="card"><h2>Uma pergunta, evidências que você pode conferir</h2><p>Investigue mercados, encontre dados para seu trabalho e compare notícias com as leituras das gestoras. Escolha por onde começar.</p>
      <div className="journey-grid">
        <article><h3>Investigar mercados</h3><p>Localize notícias, cartas e documentos por tema e período. Inspecione o trecho e abra sua origem.</p><button onClick={() => navigate("Pesquisa")}>Encontrar evidências</button></article>
        <article><h3>Explorar dados</h3><p>Confira definições e cobertura, selecione uma série e exporte dados presos à versão consultada.</p><button onClick={() => navigate("Dados")}>Explorar séries</button></article>
        <article><h3>Entender uma carteira</h3><p>Informe posições avaliadas, examine a composição e teste choques explícitos por classe. Preserve valores, premissas e versões.</p><button onClick={() => navigate("Carteiras")}>Examinar carteira</button></article>
      </div>
    </section>
    {error && <p role="alert" className="status error">{error}</p>}
    <section className="card"><h2>Disponível neste workbench</h2>{!data && !error ? <p>Consultando cobertura…</p> : <>
      <p><strong>{data?.macro?.series_with_data ?? 0}</strong> séries com observações · <strong>{news}</strong> notícias catalogadas · <strong>{letters}</strong> cartas catalogadas</p>
      {data?.macro ? <p className="muted">Snapshot {data.macro.snapshot_id} · publicado {data.macro.generated_at}</p> : <p>O snapshot macro ainda não foi conectado ao catálogo privado.</p>}
      {undated > 0 && <p>{undated} documentos têm data de publicação desconhecida. Filtros de período não incluem esses documentos.</p>}
      <p className="muted">Estes números descrevem o que foi conectado e indexado aqui. O acervo bruto e a cobertura de cada série podem ser diferentes. Cartas expressam opiniões atribuídas; uma notícia não é uma observação financeira revisada.</p>
    </>}</section>
    <SemanticCoverage coverage={data?.index_coverage}/>
  </>;
}
