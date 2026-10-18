"use client";
import {useEffect, useState} from "react";
import {downloadJson, researchApi, show} from "./workbench";
type Series = {key: string; name: string; unit: string; frequency: string; source: string; domain: string; rows: number; first_date: string | null; last_date: string | null};
type Catalog = {snapshot_id: string; generated_at: string; series: Series[]; qualification: string};
type Observation = {date: string; value: string | null};
type Result = {snapshot_id: string; snapshot_generated_at: string; series: Series; rows: Observation[]; matching_rows: number; returned_rows: number; offset: number; has_more: boolean; scope: {date_from: string | null; date_to: string | null}; coverage: {first_observation: string | null; last_observation: string | null; null_values: number}; qualification: string};
const frequency: Record<string, string> = {daily: "diária", weekly: "semanal", monthly: "mensal", quarterly: "trimestral", annual: "anual"};
function SeriesPlot({rows, unit}: {rows: Observation[]; unit: string}) {
  const [cursor, setCursor] = useState<number | null>(null);
  const valid = rows.map((row, i) => ({...row, i, t: Date.parse(row.date), v: row.value === null ? null : Number(row.value)}));
  const values = valid.flatMap(row => row.v !== null && Number.isFinite(row.v) ? [row.v] : []);
  if (!values.length) return null;
  const scale = Math.max(1, ...values.map(Math.abs));
  let low = Math.min(...values) / scale; let high = Math.max(...values) / scale;
  if (low === high) {low -= 0.05; high += 0.05;}
  const first = valid[0].t, span = valid[valid.length - 1].t - first || 1;
  const points = valid.map(row => ({...row, x: 60 + (row.t - first) / span * 840, y: row.v === null ? null : 220 - (row.v / scale - low) / (high - low) * 190}));
  const path = points.map((point, i) => point.y === null ? "" : `${i > 0 && points[i - 1].y !== null ? "L" : "M"}${point.x},${point.y}`).join(" ");
  const current = points[Math.min(cursor ?? points.length - 1, points.length - 1)];
  return <figure className="series-plot"><svg viewBox="0 0 960 260" role="img" tabIndex={0} aria-label={`Série temporal em ${unit}. Use as setas para inspecionar observações; a tabela contém todos os valores exibidos.`}
    onKeyDown={e => {if (["ArrowLeft", "ArrowRight"].includes(e.key)) {e.preventDefault(); setCursor(Math.min(points.length - 1, Math.max(0, (cursor ?? points.length - 1) + (e.key === "ArrowLeft" ? -1 : 1))));}}}
    onPointerMove={e => {const rect = e.currentTarget.getBoundingClientRect(); const x = (e.clientX - rect.left) / rect.width * 960; setCursor(points.reduce((best, p, i) => Math.abs(p.x - x) < Math.abs(points[best].x - x) ? i : best, 0));}} onPointerLeave={() => setCursor(null)}>
    <line x1="60" y1="220" x2="900" y2="220" stroke="currentColor" opacity="0.2"/><path d={path} fill="none" stroke="#176d5c" strokeWidth="2.5"/>
    {current.y !== null && <circle cx={current.x} cy={current.y} r="5" fill="#176d5c"/>}<text x="60" y="248" fontSize="15">{rows[0].date}</text><text x="900" y="248" textAnchor="end" fontSize="15">{rows[rows.length - 1].date}</text>
  </svg><figcaption>{current.date}: <strong>{show(current.value)}</strong> {unit} · visualização do recorte exibido; não é cálculo de retorno.</figcaption></figure>;
}
export default function DataExplorer() {
  const [catalog, setCatalog] = useState<Catalog | null>(null); const [selected, setSelected] = useState("");
  const [query, setQuery] = useState(""); const [from, setFrom] = useState(""); const [to, setTo] = useState("");
  const [data, setData] = useState<Result | null>(null); const [busy, setBusy] = useState(false); const [error, setError] = useState("");
  useEffect(() => {
    const controller = new AbortController();
    researchApi("data/catalog", controller.signal).then(result => {setCatalog(result); setSelected(result.series.find((s: Series) => s.rows > 0)?.key ?? "");}).catch(e => {if (!controller.signal.aborted) setError(e.message);});
    return () => controller.abort();
  }, []);
  async function load(offset = 0, pinned = false) {
    if (!catalog || (!pinned && !selected)) return;
    setBusy(true); setError("");
    try {
      const scope = pinned && data ? data.scope : {date_from: from, date_to: to};
      const series = pinned && data ? data.series.key : selected;
      const params = new URLSearchParams({snapshot_id: pinned && data ? data.snapshot_id : catalog.snapshot_id, limit: "200", offset: String(offset), ...(scope.date_from ? {date_from: scope.date_from} : {}), ...(scope.date_to ? {date_to: scope.date_to} : {})});
      setData(await researchApi(`data/series/${series}?${params}`));
    } catch(e) {setError(e instanceof Error ? e.message : "Falha na consulta");} finally {setBusy(false);}
  }
  function filterCatalog(text: string) {
    setQuery(text);
    const matches = catalog?.series.filter(s => `${s.name} ${s.key} ${s.domain}`.toLocaleLowerCase().includes(text.toLocaleLowerCase())) ?? [];
    if (!matches.some(s => s.key === selected)) setSelected(matches[0]?.key ?? "");
  }
  const selectedMetadata = catalog?.series.find(s => s.key === selected);
  const shown = catalog?.series.filter(s => `${s.name} ${s.key} ${s.domain}`.toLocaleLowerCase().includes(query.toLocaleLowerCase())) ?? [];
  const exportParams = data ? new URLSearchParams({snapshot_id: data.snapshot_id, ...(data.scope.date_from ? {date_from: data.scope.date_from} : {}), ...(data.scope.date_to ? {date_to: data.scope.date_to} : {})}) : null;
  const base = data ? `data/series/${data.series.key}` : "";
  async function manifest() {
    try {downloadJson(await researchApi(`${base}/manifest?${exportParams}`), `ofl-${data?.series.key}-${data?.snapshot_id}.manifest.json`);}
    catch(e) {setError(e instanceof Error ? e.message : "Não foi possível exportar o manifesto");}
  }
  return <>
    <section className="card"><h2>Encontre e reproduza uma série</h2><p>Selecione uma série pelo nome, confira sua unidade e cobertura e consulte um período. A versão permanece a mesma durante esta exploração.</p>
      {catalog && <p className="muted">Snapshot {catalog.snapshot_id} · publicação {catalog.generated_at}</p>}
      <label>Encontrar uma série<input value={query} onChange={e => filterCatalog(e.target.value)} placeholder="Inflação, juros, câmbio…"/></label>
      <div className="filters"><label>Série<select value={selected} onChange={e => setSelected(e.target.value)}>{shown.map(s => <option key={s.key} value={s.key}>{s.name} ({s.key})</option>)}</select></label><label>Observação a partir de<input type="date" value={from} onChange={e => setFrom(e.target.value)}/></label><label>Observação até<input type="date" value={to} onChange={e => setTo(e.target.value)}/></label><button disabled={busy || !selected} onClick={() => void load()}>{busy ? "Consultando…" : "Consultar série"}</button></div>
      {selectedMetadata && <p>Unidade: {show(selectedMetadata.unit)} · frequência informada: {frequency[selectedMetadata.frequency] ?? selectedMetadata.frequency} · {selectedMetadata.rows} observações · {show(selectedMetadata.first_date)} a {show(selectedMetadata.last_date)}</p>}
      <p className="muted">Datas de observação não são datas de divulgação. O snapshot registra a versão publicada pelo OFL; não reconstrói automaticamente o que era conhecido no passado.</p>
    </section>
    {error && <p className="status error" role="alert">{error}</p>}
    {data && <section className="card"><h3>{data.series.name}</h3><p>Exibindo {data.returned_rows} de {data.matching_rows} observações do recorte, em ordem de data. {data.coverage.null_values} valores ausentes.</p>
      {data.rows.length > 0 && <SeriesPlot key={`${data.snapshot_id}:${data.series.key}:${data.rows[0].date}`} rows={data.rows} unit={data.series.unit}/>}
      <div className="data-actions"><a href={`/api/research/${base}/export?${exportParams}`}>Baixar CSV do recorte completo</a><button onClick={() => void manifest()}>Baixar manifesto</button></div>
      <p className="muted">O CSV usa a série, as datas e a versão da consulta exibida, mesmo se você editar os campos acima. Limite de exportação: 100 mil linhas. O manifesto inclui os checksums do CSV e do dataset de origem.</p>
      <details><summary>Reproduzir a leitura em Python</summary><pre>{`import hashlib, json\nimport pandas as pd\n\nmanifest = json.load(open("ofl-${data.series.key}-${data.snapshot_id}.manifest.json"))\npath = "ofl-${data.series.key}-${data.snapshot_id}.csv"\nassert hashlib.sha256(open(path, "rb").read()).hexdigest() == manifest["csv_sha256"]\ndf = pd.read_csv(path, dtype={"value": "string"}, parse_dates=["observation_date"])`}</pre><p>Os valores são representações do tipo numérico publicado na origem. A leitura como texto preserva o conteúdo do CSV; conversões e transformações posteriores devem ser explícitas.</p></details>
      <div style={{overflowX: "auto"}}><table><thead><tr><th>Data de observação</th><th>Valor</th><th>Unidade</th></tr></thead><tbody>{data.rows.map(row => <tr key={row.date}><td>{row.date}</td><td>{show(row.value)}</td><td>{data.series.unit}</td></tr>)}</tbody></table></div>
      {!data.rows.length && <p>Não há observações para o recorte solicitado nesta versão.</p>}
      <div className="filters"><button disabled={busy || data.offset === 0} onClick={() => void load(Math.max(0, data.offset - 200), true)}>Observações anteriores</button><button disabled={busy || !data.has_more} onClick={() => void load(data.offset + 200, true)}>Próximas observações</button></div>
    </section>}
  </>;
}
