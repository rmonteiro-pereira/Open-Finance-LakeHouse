/** Native Ollama adapter: honor explicit thinking and keep-alive controls. */
import type {ModelPort, ModelResponse} from "@inkwell/core";
export function ollamaModel(baseUrl:string, noThinking:boolean, transport:(input:URL | RequestInfo,init?:RequestInit)=>Promise<Response>=fetch):ModelPort {
  const base=new URL(baseUrl);
  if(!["http:","https:"].includes(base.protocol)||base.username||base.password||base.search||base.hash)throw Error("Invalid model endpoint");
  const complete:ModelPort["complete"]=async request=>{
    let data:Record<string,unknown>;
    try {
      const result=await transport(`${baseUrl.replace(/\/$/,"")}/api/chat`,{method:"POST",headers:{"Content-Type":"application/json"},redirect:"error",signal:request.signal,
        body:JSON.stringify({model:request.model,stream:false,...(noThinking?{think:false}:{}),keep_alive:-1,options:{num_predict:Math.min(request.maxTokens??1024,2048)},
          messages:request.messages.map(m=>({role:m.role,content:m.content,...(m.toolCalls?.length?{tool_calls:m.toolCalls.map(c=>({id:c.id,type:"function",function:{name:c.name,arguments:c.arguments}}))}:{})})),
          ...(request.tools?.length?{tools:request.tools.map(t=>({type:"function",function:t}))}:{}),...(request.responseFormat==="json"?{format:"json"}:{})})});
      if(!result.ok)throw Error("model access failed");
      const text=await result.text();if(text.length>2_000_000)throw Error("model response exceeds bound");data=JSON.parse(text);
    }catch{throw Error("Model service unavailable or request interrupted");}
    const message=data.message as {content?:unknown;tool_calls?:{function?:{name?:unknown;arguments?:unknown}}[]}|undefined;
    if(data.done!==true||!message||typeof message.content!=="string")throw Error("Invalid model response");
    const toolCalls:ModelResponse["toolCalls"]=[], droppedToolCalls:ModelResponse["droppedToolCalls"]=[];
    for(const call of (Array.isArray(message.tool_calls)?message.tool_calls:[]).slice(0,12)){
      const fn=call.function;
      if(typeof fn?.name!=="string"||!fn.arguments||typeof fn.arguments!=="object"||Array.isArray(fn.arguments)){droppedToolCalls.push({name:typeof fn?.name==="string"?fn.name:"unknown",reason:"invalid native tool arguments"});continue;}
      toolCalls.push({id:crypto.randomUUID(),name:fn.name,arguments:fn.arguments as Record<string,unknown>});
    }
    return {content:message.content,toolCalls,droppedToolCalls,model:request.model,provider:"ollama",usage:{promptTokens:typeof data.prompt_eval_count==="number"?data.prompt_eval_count:undefined,completionTokens:typeof data.eval_count==="number"?data.eval_count:undefined}};
  };
  return {complete,async *stream(request){const result=await complete(request);if(result.content)yield{type:"content" as const,text:result.content};for(const [index,call]of result.toolCalls.entries())yield{type:"tool_call" as const,index,id:call.id,name:call.name,argsDelta:JSON.stringify(call.arguments)};return result;}};
}
