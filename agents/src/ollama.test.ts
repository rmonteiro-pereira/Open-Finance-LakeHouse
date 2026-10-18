import {test,expect} from "bun:test";
import {ollamaModel} from "./ollama";
test("native Ollama explicitly disables thinking and preserves tool argument precision",async()=>{
 const model=ollamaModel("http://model.test",true,async(_url,init)=>{const body=JSON.parse(String(init?.body));expect(body.think).toBe(false);expect(body.keep_alive).toBe(-1);expect(body.options.num_predict).toBe(512);return Response.json({done:true,message:{content:"",tool_calls:[{function:{name:"read_quotes",arguments:{price:"9007199254740993.12"}}}]},eval_count:12});});
 const response=await model.complete({model:"local",maxTokens:512,messages:[{role:"user",content:"quotes"}]});expect(response.toolCalls[0]?.arguments.price).toBe("9007199254740993.12");expect(response.usage.completionTokens).toBe(12);
});
test("native model failures do not expose backend errors",async()=>{
 const model=ollamaModel("http://model.test",true,async()=>new Response("secret backend details",{status:500}));await expect(model.complete({model:"local",messages:[]})).rejects.toThrow("Model service unavailable");
});
