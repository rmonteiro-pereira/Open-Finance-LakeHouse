/** Real source workflow, with optional model interpretation after evidence collection. */
import type {ModelResponse,ReplyStep,TurnContext} from "@inkwell/core";
import {composeDraft, type TurnEvidence} from "@inkwell/core/evidence";
import {verifyExcerpt} from "@inkwell/core/evidence";
import {DATASET_MARKER,datasetRequest} from "./dataset-request";
import {BRIEF_PROMPT,briefSources,composeBrief,parseBriefDraft,qualitativeInput,type BriefSource} from "./brief";
import {createHash} from "node:crypto";
type Row=Record<string,unknown>;
function row(value:unknown):Row{return value!==null&&typeof value==="object"&&!Array.isArray(value)?value as Row:{};}
function observations(step:ReplyStep){
 return step.evidence.delivered.map(({call,result})=>({source_tool:call.name,rows:row(result.widget).rows}));
}
function result(content:string,toolCalls:ModelResponse["toolCalls"]=[],usage:ModelResponse["usage"]={}):ModelResponse{return {content,toolCalls,usage,droppedToolCalls:[],provider:"ofl-source-workflow"};}
function call(name:string,args:Row){return{id:crypto.randomUUID(),name,arguments:args};}
const STOP=new Set("use encontre busque buscar pesquisar pesquisa noticias noticia notícias notícia cartas carta documentos documento evidências evidencia acervo sobre como para uma quero depois finalize trecho fontes fonte source mode keyword limit read_passage search_archive retornado hipóteses hipóteses apenas único unico literal curto json excerpts entender compreender qual quais excerpts com sem que dos das por entre preciso informe analise análise contexto explique compare comparar identifique identificar gestoras gestores possível possivel contraponto concluir falta permitem usando revisão rascunho".split(" "));
export function documentQuery(question:string){
 const explicit=/\bq=("(?:[^"\\]|\\.)*"|[^,;\n]+)/.exec(question);
 if(explicit){try{return String(JSON.parse(explicit[1])).slice(0,1000);}catch{return explicit[1].trim().slice(0,1000);}}
 const prompt=question.replace(/^.*?Pergunta:\s*/s,"");
 const topic=(/\bsobre\s+([^.!?;\n]+)/i.exec(prompt)?.[1]??prompt.split(/[.!?\n]/)[0])??"";
 const terms=[...new Set((topic.match(/[\p{L}\p{N}]+/gu)??[]).filter(w=>w.length>=3&&!STOP.has(w.toLocaleLowerCase())))].slice(0,8);
 return terms.map(w=>`"${w}"`).join(" ");
}
/** Prefer a topical window; an exact quote is still not a relevance judgment. */
export function excerptWindow(text:string,terms:string[]){
 const lower=text.toLocaleLowerCase();let start=0,best=0;
 for(const term of terms){
  let offset=0;
  for(let n=0;n<32;n++){
   const position=lower.indexOf(term.toLocaleLowerCase(),offset);if(position<0)break;
   const candidate=Math.max(0,position-80),window=lower.slice(candidate,candidate+320);
   const score=terms.filter(t=>window.includes(t.toLocaleLowerCase())).length;
   if(score>best){best=score;start=candidate;}offset=position+term.length;
  }
 }
 if(start>0){const boundary=text.indexOf(" ",start);if(boundary>=start&&boundary<start+40)start=boundary+1;}
 let end=Math.min(start+320,text.length);if(end<text.length&&text.lastIndexOf(" ",end)>start+200)end=text.lastIndexOf(" ",end);
 return text.slice(start,end).trim();
}

/** Catalog eligibility and clarification do not require generated planning. */
export async function guidedDataset(ctx:TurnContext,step:ReplyStep,question:string):Promise<ModelResponse|undefined>{
 if(!/dataset|prepar(?:ar|e|ação).*dados/i.test(question)||!step.tools.some(t=>t.name==="read_data_catalog"))return;
 if(step.evidence.failed.some(d=>['read_data_catalog','prepare_dataset'].includes(d.call.name)))return result('A leitura necessária ao preparo falhou. Nenhum dataset foi confirmado. Tente novamente e confira acesso e disponibilidade da versão.');
 const marker=question.indexOf(DATASET_MARKER);
 let spec:ReturnType<typeof datasetRequest.parse>|undefined;
 if(marker>=0){
  try{spec=datasetRequest.parse(JSON.parse(question.slice(marker+DATASET_MARKER.length).trim()));}
  catch{return result('A receita do dataset é inválida. Confirme séries, período, transformação e alinhamento no formulário do catálogo. Nenhum dataset foi preparado.');}
 }
 const prepared=step.evidence.delivered.find(d=>d.call.name==='prepare_dataset');
 if(prepared){
  const widget=row(prepared.result.widget),first=Array.isArray(widget.rows)?row(widget.rows[0]):{};
  let manifest:Row={};try{manifest=typeof first.manifest==='string'?row(JSON.parse(first.manifest)):row(first.manifest);}catch{/* No validated result. */}
  if(widget.kind!=='result_table'||!spec||JSON.stringify(datasetRequest.safeParse(manifest.specification).data)!==JSON.stringify(spec)||!/^[a-f0-9]{64}$/.test(String(manifest.csv_sha256))||!Number.isInteger(manifest.rows)||Number(manifest.rows)<=0)return result('Não foi possível validar um dataset com observações nesta consulta. Confira o erro ou ajuste o recorte. Nenhuma entrega foi confirmada.');
  return result(`Dataset preparado: ${manifest.rows} linhas, versão ${spec.snapshot_id}. A amostra e o manifesto estão abaixo. Baixe o pacote reproduzível no cartão do resultado. Valores ausentes permanecem nulos; alinhamento por data não converte frequências. As transformações usam observações do intervalo escolhido, sem aquecimento anterior. SHA-256 do CSV: ${manifest.csv_sha256}. Datas de observação não comprovam disponibilidade histórica; este dataset descritivo não é um backtest.`);
 }
 const delivered=step.evidence.delivered.filter(d=>d.call.name==="read_data_catalog");
 if(!delivered.length){
  if(spec)return result("Vou conferir a versão e as séries escolhidas no catálogo.",[call("read_data_catalog",{snapshot_id:spec.snapshot_id})]);
  const topics=[...new Set(question.toLowerCase().match(/\b(?:ipca|selic|cdi|pib)\b/g)??[])];
  return result('Vou conferir as séries, suas unidades, frequências e versões no catálogo antes de definir o preparo.',(topics.length?topics:[undefined]).map(q=>call("read_data_catalog",q?{q}:{})));
 }
 if(delivered.some(d=>row(d.result.widget).kind!=="result_table"))return result("Não foi possível conferir o catálogo nesta consulta. Não confirmei disponibilidade nem preparei um dataset; tente consultar Dados novamente.");
 const catalogs=delivered.map(d=>({query:d.call.arguments.q,rows:row(d.result.widget).rows}));
 const available=catalogs.flatMap(c=>Array.isArray(c.rows)?c.rows.map(row).filter(r=>Number(r.rows)>0):[]);
 if(spec){
  if(!spec.series.every(s=>available.some(r=>r.key===s.series_id&&r.snapshot_id===spec!.snapshot_id)))return result('Uma série escolhida não está disponível com dados na versão conferida. Atualize o catálogo e confirme a receita; nenhum dataset foi preparado.');
  if(!step.tools.some(t=>t.name==='prepare_dataset'))return result('Preparação indisponível para este especialista. Use Dados e métodos.');
  return result('Vou preparar a receita confirmada, preservando valores ausentes e as frequências originais.',[call('prepare_dataset',spec)]);
 }
 if(!available.length)return result('Não encontrei séries populadas correspondentes no catálogo consultado. Nenhum dataset foi preparado. Confira cobertura e nomes antes de definir a análise.');
 const missing=catalogs.some(c=>!Array.isArray(c.rows)||!c.rows.some(r=>Number(row(r).rows)>0));
 return result((missing?'Parte das séries solicitadas não apareceu com dados neste catálogo. ':'')+'As séries disponíveis estão nas tabelas, com unidade, frequência, versão e intervalo observado. Confirme as séries, o período, a transformação e o alinhamento no formulário do catálogo abaixo. As frequências originais são preservadas e valores ausentes permanecem nulos; não há reamostragem ou preenchimento implícito. Não realizei transformação nem exportação. Datas de observação não comprovam disponibilidade histórica.');
}

/** Observations inform routing only. Canonical API re-reads establish evidence. */
export async function guidedDocuments(ctx:TurnContext,step:ReplyStep,question:string,resolve:(id:string,signal?:AbortSignal)=>Promise<Row|undefined>):Promise<ModelResponse|undefined>{
 const request={tools:step.tools,signal:ctx.deadline.signal};
 if(!request.tools?.some(t=>t.name==="search_archive")||!/not[ií]ci|cartas?|document|acervo|evid[eê]ncia|search_archive|coletar e conferir fontes/i.test(question))return;
 if(step.evidence.failed.some(d=>["search_archive","read_passage"].includes(d.call.name)))return result("Uma leitura necessária à pesquisa falhou. As tabelas disponíveis são parciais; não confirmei uma comparação. Tente novamente e confira acesso e disponibilidade das fontes.");
 const observed=observations(step);
 const dates=question.match(/\d{4}-\d{2}-\d{2}/g)??[];
 const scoped=/Escopo solicitado:\s*(\d{4}-\d{2}-\d{2}|início não definido)\s+a\s*(\d{4}-\d{2}-\d{2}|fim não definido)/.exec(question);
 const date_from=scoped&&/^\d/.test(scoped[1])?scoped[1]:!scoped&&dates.length>=2?dates[0]:undefined;
 const date_to=scoped&&/^\d/.test(scoped[2])?scoped[2]:!scoped&&dates.length>=2?dates[1]:undefined;
 const wantsMacro=request.tools?.some(t=>t.name==="read_data_catalog")&&/s[eé]ries?|macro|indicadores|dados/i.test(question);
 const catalogs=observed.filter(o=>o.source_tool==="read_data_catalog").flatMap(o=>Array.isArray(o.rows)?o.rows.map(row):[]);
 const numeric=observed.some(o=>o.source_tool==="read_macro_series");
 const requestedSeries=catalogs.filter(r=>typeof r.key==="string"&&question.toLowerCase().split(/[^a-z0-9_-]+/).includes(r.key.toLowerCase())&&Number(r.rows)>0);
 const macroReads=!numeric&&requestedSeries.length===1&&request.tools?.some(t=>t.name==="read_macro_series")?[call("read_macro_series",{series_id:requestedSeries[0].key,snapshot_id:requestedSeries[0].snapshot_id,limit:30,...(date_from?{date_from}:!date_to&&requestedSeries[0].last_date?{date_from:requestedSeries[0].last_date}:{}),...(date_to?{date_to}:!date_from&&requestedSeries[0].last_date?{date_to:requestedSeries[0].last_date}:{})})]:[];
 const searches=observed.filter(o=>o.source_tool==="search_archive");
 const passages=observed.filter(o=>o.source_tool==="read_passage").flatMap(o=>Array.isArray(o.rows)?o.rows.map(row):[]);
 if(!searches.length&&!passages.length){
  const q=documentQuery(question);if(!q)return result('Qual tema você quer investigar nas notícias, cartas ou documentos?');
  const explicit=/\bsource=(news|manager_letter|mub3|b3_bulletin)\b/.exec(question)?.[1];
  const news=/not[ií]ci|source=news/i.test(question),letters=/cartas?|source=manager_letter/i.test(question);
  const sources=explicit?[explicit]:news&&letters?["news","manager_letter"]:news?["news"]:letters?["manager_letter"]:/mub3/i.test(question)?["mub3"]:/boletim|b3/i.test(question)?["b3_bulletin"]:[undefined];
  const limit=Math.max(1,Math.min(2,Number(/\blimit=(\d+)/.exec(question)?.[1]??2)));
  const reads=sources.map(source=>call("search_archive",{q,mode:"keyword",limit,...(source?{source}:{}),...(date_from?{date_from}:{}),...(date_to?{date_to}:{})}));
  if(wantsMacro)reads.push(call("read_data_catalog",{...(question.match(/\b(ipca|selic|pib|cdi)\b/i)?.[1]?{q:question.match(/\b(ipca|selic|pib|cdi)\b/i)![1]}:{})}));
  return result('Vou buscar as fontes e abrir os trechos no contexto antes de propor uma leitura.',reads);
 }
 if(!passages.length){
  const hits=searches.flatMap(o=>Array.isArray(o.rows)?o.rows.map(row):[]);
  // Prefer source diversity when the question asks for news and manager letters.
  const chosen:Row[]=[];for(const hit of hits){if(!chosen.some(r=>r.source===hit.source))chosen.push(hit);}
  for(const hit of hits)if(chosen.length<2&&!chosen.includes(hit))chosen.push(hit);
  const ids=[...new Set(chosen.slice(0,/um [uú]nico trecho|\blimit=1\b/i.test(question)?1:2).map(h=>h.passage_id).filter((v):v is string=>typeof v==="string"&&/^[a-zA-Z0-9_-]{1,128}$/.test(v)))];
  if(!ids.length&&macroReads.length)return result("Vou consultar a série identificada no catálogo.",macroReads);
  if(!ids.length&&numeric)return result('Não encontrei evidências documentais neste recorte; as observações macro consultadas estão nas tabelas com versão, datas e limitações.');
  if(!ids.length)return result('Não encontrei evidências neste recorte. Isso não demonstra que um evento não ocorreu. Revise o tema, as fontes e a cobertura disponível.');
  return result('Vou abrir o contexto das passagens encontradas. As séries identificadas usam a versão e as datas exibidas nas tabelas.',[...ids.map(passage_id=>call("read_passage",{passage_id})),...macroReads]);
 }
 const ids=[...new Set(passages.map(p=>p.id).filter((v):v is string=>typeof v==="string"&&/^[a-zA-Z0-9_-]{1,128}$/.test(v)))].slice(0,2);
 const canonical:BriefSource[]=[];
 const revision=(text:string)=>createHash("sha256").update(text).digest("hex");
 for(const id of ids){
  const original=passages.find(p=>p.id===id);const text=original?.text;
  if(typeof text!=="string"||text.trim().length<12)continue;
  const terms=(documentQuery(question).match(/[\p{L}\p{N}]+/gu)??[]).filter(t=>t.length>=3);
  const quote=excerptWindow(text,terms);
  let current:Row|undefined;
  let binding;try{binding=await verifyExcerpt({sourceId:id,revision:revision(text),quote},async(sourceId,signal)=>{current=await resolve(sourceId,signal);const text=current?.text;return current?.active!==false&&typeof text==="string"?{id:sourceId,revision:revision(text),text}:undefined;},request.signal);}catch{if(request.signal?.aborted)throw Error("Research request interrupted");continue;}
  const delivered=step.evidence.delivered.find(d=>d.call.name==="read_passage"&&d.call.arguments.passage_id===id);
  if(binding.status==="verified"&&delivered)canonical.push({...current,id,quote:binding.quote,revision:binding.revision,toolCallId:delivered.call.id});
 }
 if(!canonical.length)return result('As passagens não estão disponíveis para conferência nesta versão ou política de acesso. Nenhuma conclusão foi validada.');
 const facts=new Map(canonical.map((p,i)=>[`source.s${i}.quote`,[{key:`source.s${i}.quote`,value:p.quote,toolCallId:p.toolCallId}]]));
 const evidence:TurnEvidence={...step.evidence,facts};
 const composed=composeDraft({sections:canonical.map((p,i)=>({id:`source${i}`,prose:[{kind:"claim" as const,ref:`quote${i}`}]})),claims:canonical.map((p,i)=>({id:`quote${i}`,key:`source.s${i}.quote`}))},evidence,{strict:true});
 if(!composed.ok)return result('Não foi possível vincular os trechos às evidências deste turno. Nenhuma conclusão foi validada.');
 let usage:ModelResponse['usage']={};let provider="ofl-source-workflow";
 const interpret=!/sem hip[oó]teses|apenas (?:os )?trechos|somente (?:as )?fontes|n[aã]o (?:gere|proponha) interpreta/i.test(question);
 let draft:ReturnType<typeof parseBriefDraft>;
 let generation:'not_requested'|'valid'|'invalid'|'unavailable'='not_requested';
 if(interpret)try{
  const generated=await ctx.call("interpretation",{maxTokens:48,responseFormat:"json",messages:[{role:"system",content:BRIEF_PROMPT},{role:"user",content:JSON.stringify({question:qualitativeInput(question.replace(/^[\s\S]*?Pergunta:\s*/,"")),sources:briefSources(canonical)})}]});
  usage=generated.usage;provider=generated.provider??"model";
  draft=parseBriefDraft(generated.content,canonical);generation=draft?'valid':'invalid';
 }catch{if(request.signal?.aborted)throw Error('Research request interrupted');generation='unavailable';}
 const quotes=composed.composition.sections.map(section=>section.text.split("\n").map(line=>"> "+line).join("\n"));
 let content=composeBrief(question,canonical,quotes,draft,generation);
 if(canonical.length<ids.length)content+="\n\nParte das fontes lidas não pôde ser revalidada e foi excluída deste brief.";
 if(wantsMacro&&!numeric)content+='\n\nO catálogo macro acompanha as fontes. Selecione a série e o período em Dados para completar a análise quantitativa.';
 return {...result(content,[],usage),provider};
}
