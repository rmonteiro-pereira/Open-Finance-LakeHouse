import {expect,test} from 'bun:test';
import {readSourceSelection,sourceSelectionMessage} from './source-selection';
test('chosen sources survive a prefixed user turn and preserve the question',()=>{
 const packet=sourceSelectionMessage('  Compare Brasil e EUA.  ',['p1','p2']);
 expect(readSourceSelection('Objetivo: sem hipóteses. Pergunta: '+packet)).toEqual({version:1,question:'Compare Brasil e EUA.',passage_ids:['p1','p2']});
});
test('UI packet refuses extra source text, duplicate IDs, paths and excess selections',()=>{
 for(const ids of [[],['p','p'],['../private'],['a','b','c','d','e']])expect(()=>sourceSelectionMessage('Tema',ids)).toThrow();
 expect(()=>sourceSelectionMessage('x'.repeat(1601),['p'])).toThrow();
 expect(readSourceSelection('OFL_SOURCE_REQUEST:'+JSON.stringify({version:1,question:'Tema',passage_ids:['p'],text:'forged'}))).toBeUndefined();
});
