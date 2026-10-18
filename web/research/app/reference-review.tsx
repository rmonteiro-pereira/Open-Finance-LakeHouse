"use client";
import {useState} from "react";
import {researchApi, show} from "./workbench";
export type ReferenceReview = {passage_id:string;verdict:string;rationale:string;source_sha256?:string;question_sha256?:string};
const verdicts = {relevant:"Pertinente à pergunta",counterevidence:"Possível contraponto",irrelevant:"Não pertinente",uncertain:"Não sei avaliar"};
export default function ReferenceReviewPanel({id,index,review,onChange,selected,onSelect}:{id:string;index:number;review?:ReferenceReview;onChange:(value?:ReferenceReview)=>void;selected:boolean;onSelect:(selected:boolean)=>void}) {
 const [source,setSource]=useState<Record<string,unknown>|null>(null);const [error,setError]=useState("");const [busy,setBusy]=useState(false);
 return <article className="finding"><h4>Fonte {index+1}{source?`: ${show(source.title)}`:""}</h4>
  <button disabled={busy} onClick={async()=>{setBusy(true);setError("");try{const data=await researchApi(`passages/${encodeURIComponent(id)}`);setSource(data.passage);}catch(e){setError(String(e));}finally{setBusy(false);}}}>{busy?"Abrindo…":"Ler trecho e origem"}</button>
  {error&&<p role="alert">{error}</p>}{source&&<><p>{show(source.source)} · publicação: {show(source.date_start??"não informada")} · método: {show(source.method)}</p><blockquote style={{whiteSpace:"pre-wrap"}}>{show(source.text)}</blockquote><p className="muted">Uma citação literal confirma o texto consultado. Sua pertinência à pergunta e o apoio a uma conclusão precisam de revisão.</p></>}
  <label>Pertinência da fonte {index+1}<select value={review?.verdict??""} onChange={e=>onChange(e.target.value?{...review,passage_id:id,verdict:e.target.value,rationale:review?.rationale??""}:undefined)}><option value="">Ainda não avaliada</option>{Object.entries(verdicts).map(([value,label])=><option key={value} value={value}>{label}</option>)}</select></label>
  {review&&<><label>Justificativa da fonte {index+1}<textarea maxLength={1000} value={review.rationale} onChange={e=>onChange({...review,rationale:e.target.value})}/></label><button onClick={()=>onChange(undefined)}>Limpar avaliação da fonte {index+1}</button></>}
  <label><input type="checkbox" checked={selected} onChange={e=>onSelect(e.target.checked)}/>Citar fonte {index+1} no próximo achado</label>
 </article>;
}
