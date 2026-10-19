import {test,expect} from 'bun:test';
import {composeBrief,parseBriefDraft,type BriefSource} from './brief';
const source:BriefSource={id:'p1',item_id:'i1',quote:'A inflação exige atenção ao horizonte.',revision:'a'.repeat(64),toolCallId:'call1',source:'news',title:'Agenda econômica',collection:'feed',date_start:'2026-10-15',date_end:'2026-10-15',date_precision:'day',content_tier:'headline_only'};
const letter:BriefSource={...source,id:'p2',item_id:'i2',source:'manager_letter',title:'Carta Outubro 2025',collection:'gestora',date_start:null,date_end:null,date_precision:'unknown',content_tier:'private_original'};
test('brief distinguishes reported headline, manager opinion and unknown publication without manufacturing a counterpoint',()=>{
 const text=composeBrief('Compare notícias e cartas sobre inflação no Brasil.',[source,letter],['> '+source.quote,'> '+letter.quote],undefined,'unavailable');
 expect(text).toContain('Somente manchete/resumo disponível');expect(text).toContain('Opinião atribuída à gestora');expect(text).toContain('Origem catalogada: gestora');expect(text).toContain('Publicação: não catalogada');expect(text).toContain('não é possível ordenar todas as fontes');expect(text).toContain('Não há contraponto estabelecido');expect(text).toContain('modelo não retornou');expect(text).toContain('correspondem à comparação solicitada');expect(text).toContain('não é resultado realizado');
});
test('possible contrast requires two distinct source items, not two passages from one document',()=>{
 const d=JSON.stringify({references:[1,2],relation:'contrast'});
 expect(parseBriefDraft(d,[source,{...letter,item_id:'i1'}])).toBeUndefined();
 expect(parseBriefDraft(d,[source,letter])?.relation).toBe('contrast');
 const text=composeBrief('Compare as fontes.',[source,letter],['> '+source.quote,'> '+letter.quote],parseBriefDraft(d,[source,letter]),'valid');
 expect(text).toContain('não uma contradição comprovada');expect(text).toContain('[p1, p2]');
});
test('model-generated prose never enters the brief; malformed, invented and duplicate references are rejected',()=>{
 const base={references:[1],relation:'context'};
 for(const d of [null,{...base,references:[99]},{...base,references:[1,1]},{...base,text:'A inflação será 3% neste período.'},{...base,text:'Compre este fundo para ter retorno garantido.'},{...base,extra:'unchecked prose'},{...base,relation:'fact'}])expect(parseBriefDraft(JSON.stringify(d),[source,letter])).toBeUndefined();
 expect(parseBriefDraft('{"text":"incomplete', [source])).toBeUndefined();
});
