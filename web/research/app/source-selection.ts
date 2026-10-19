export const SOURCE_MARKER='OFL_SOURCE_REQUEST:';
export type SourceSelection={version:1;question:string;passage_ids:string[]};
export function readSourceSelection(message:string):SourceSelection|undefined{
 const index=message.indexOf(SOURCE_MARKER);if(index<0)return;
 try{
  const s=JSON.parse(message.slice(index+SOURCE_MARKER.length));
  if(!s||Object.keys(s).sort().join(',')!=='passage_ids,question,version'||s.version!==1||typeof s.question!=='string'||!s.question.trim()||s.question.trim().length>1600||!Array.isArray(s.passage_ids)||s.passage_ids.length<1||s.passage_ids.length>4||s.passage_ids.some((id:unknown)=>typeof id!=='string'||!/^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/.test(id))||new Set(s.passage_ids).size!==s.passage_ids.length)return;
  return {...s,question:s.question.trim()};
 }catch{return;}
}
export function sourceSelectionMessage(question:string,passage_ids:string[]){
 const message=SOURCE_MARKER+JSON.stringify({version:1,question,passage_ids});
 if(!readSourceSelection(message))throw Error('Informe uma pergunta de até 1.600 caracteres e selecione de uma a quatro passagens distintas.');
 return message;
}
