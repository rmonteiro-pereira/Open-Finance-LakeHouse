"use client";
import {useState} from "react";
type Row=Record<string,unknown>;
export function AgentDatasetChoices({rows,onPrepare,busy}:{rows:Row[];onPrepare:(message:string)=>Promise<void>;busy:boolean}){
 const [selected,setSelected]=useState<string[]>([]),[from,setFrom]=useState(''),[to,setTo]=useState(''),[alignment,setAlignment]=useState('outer'),[operation,setOperation]=useState('level');
 const available=rows.filter(r=>typeof r.key==='string'&&typeof r.snapshot_id==='string'&&Number(r.rows)>0);
 const snapshots=[...new Set(available.filter(r=>selected.includes(String(r.key))).map(r=>r.snapshot_id))];
 if(!available.length)return null;
 const valid=selected.length>0&&selected.length<=8&&snapshots.length===1&&from&&to&&from<=to;
 return <section className="card"><h4>Confirmar receita do dataset</h4><p>Escolha até oito séries. Confira definição, unidade e frequência na tabela. Selic efetiva, anualizada e meta são medidas diferentes.</p>
 <fieldset disabled={busy}><legend>Séries disponíveis nesta consulta</legend>{available.map(r=><label key={`${r.snapshot_id}-${r.key}`} style={{display:'block'}}><input type="checkbox" checked={selected.includes(String(r.key))} onChange={e=>setSelected(s=>e.target.checked?[...s,String(r.key)]:s.filter(k=>k!==r.key))}/>{String(r.name??r.key)} ({String(r.key)}) · {String(r.unit)} · {String(r.frequency)} · versão {String(r.snapshot_id)}</label>)}</fieldset>
 <div className="filters"><label>Início do dataset<input type="date" value={from} onChange={e=>setFrom(e.target.value)}/></label><label>Fim do dataset<input type="date" value={to} onChange={e=>setTo(e.target.value)}/></label>
 <label>Alinhamento do dataset<select value={alignment} onChange={e=>setAlignment(e.target.value)}><option value="outer">Todas as datas observadas</option><option value="inner">Somente datas comuns</option></select></label>
 <label>Transformação do dataset<select value={operation} onChange={e=>setOperation(e.target.value)}><option value="level">Valor original</option><option value="difference">Diferença de uma observação</option><option value="percent_change">Variação percentual de uma observação</option><option value="index_100">Índice base 100</option><option value="rolling_mean">Média de três observações</option></select></label></div>
 <p>Valores ausentes permanecem nulos. Não há conversão de frequência. Defasagem: uma observação; média móvel: três observações dentro do período, sem aquecimento anterior. A transformação vale para todas as séries selecionadas.</p>
 {snapshots.length>1&&<p role="alert">Selecione séries de uma mesma versão.</p>}
 <button type="button" disabled={busy||!valid} onClick={()=>void onPrepare('OFL_DATASET_REQUEST:'+JSON.stringify({snapshot_id:snapshots[0],series:selected.map(series_id=>({series_id,operation,window:3,lag:1})),alignment,date_from:from,date_to:to}))}>Confirmar e preparar dataset pelo agente</button></section>;
}
function object(value:unknown):Row{try{const parsed=typeof value==='string'?JSON.parse(value):value;return parsed&&typeof parsed==='object'&&!Array.isArray(parsed)?parsed:{};}catch{return{};}}
export function AgentDatasetResult({row}:{row:Row}){
 const manifest=object(row.manifest),spec=object(manifest.specification);
 const [busy,setBusy]=useState(false),[error,setError]=useState('');
 if(!/^[a-f0-9]{64}$/.test(String(manifest.csv_sha256))||!Number.isInteger(manifest.rows)||Number(manifest.rows)<=0||typeof spec.snapshot_id!=='string'||!Array.isArray(spec.series))return null;
 async function download(){setBusy(true);setError('');try{
  const check=await fetch('/api/research/datasets/preview?'+new URLSearchParams({specification:JSON.stringify(spec)}),{cache:'no-store'});
  if(!check.ok)throw Error('Não foi possível revalidar a receita nesta sessão.');
  const current=await check.json();
  if(current.manifest?.csv_sha256!==manifest.csv_sha256)throw Error('O resultado mudou desde a preparação. Prepare novamente antes de baixar.');
  const response=await fetch('/api/research/datasets/bundle',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(spec),cache:'no-store'});
  if(!response.ok)throw Error('Não foi possível obter o pacote nesta sessão. Confira acesso, versão e disponibilidade.');
  const blob=await response.blob(),url=URL.createObjectURL(blob),link=document.createElement('a');link.href=url;link.download=`ofl-dataset-${spec.snapshot_id}.zip`;link.click();setTimeout(()=>URL.revokeObjectURL(url),1000);
 }catch(e){setError(e instanceof Error?e.message:'Falha ao baixar dataset');}finally{setBusy(false);}}
 return <section className="card"><h4>Dataset preparado · {String(manifest.rows)} linhas</h4><p>Versão: {String(spec.snapshot_id)}. Período: {String(spec.date_from)} a {String(spec.date_to)}. Alinhamento: {String(spec.alignment)}.</p><p>O pacote contém CSV, entradas, manifesto, recibo e programa Python para reproduzir e conferir o resultado offline.</p><p style={{overflowWrap:'anywhere'}}>SHA-256 esperado do CSV: {String(manifest.csv_sha256)}</p><button type="button" disabled={busy} onClick={()=>void download()}>{busy?'Obtendo pacote…':'Baixar dataset reproduzível do agente'}</button>{error&&<p role="alert">{error}</p>}</section>;
}
