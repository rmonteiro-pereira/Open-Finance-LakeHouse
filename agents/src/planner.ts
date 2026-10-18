import type {ModelPort} from "@inkwell/core";
import type {Specialist} from "./agents";
const CLARIFY: Record<Specialist, string> = {
  market: "Informe o ativo, o código de mercado e o intervalo de datas para consultar cotações. Também posso consultar a cobertura ou observações aprovadas de um assunto.",
  archive: "Informe uma pergunta ou termos para buscar no acervo. Para abrir um item específico, use uma referência retornada pela busca.",
  quality: "Peça a cobertura, o processamento recente ou informe o identificador de um candidato OCR para consultar seu histórico de revisão.",
};
/** The model plans reads. Financial assertions are delivered only from source rows. */
export function evidencePlanner(model: ModelPort, kind: Specialist): ModelPort {
  const complete: ModelPort["complete"] = async (request) => {
    const result = await model.complete(request);
    return {...result, content: result.toolCalls.length ? "Vou consultar as fontes OFL. Confira as evidências e suas limitações nas tabelas." : CLARIFY[kind]};
  };
  return {complete, async *stream(request) {
    const result = await complete(request);
    if (result.content) yield {type: "content" as const, text: result.content};
    for (const [index, call] of result.toolCalls.entries()) yield {type: "tool_call" as const, index, id: call.id, name: call.name, argsDelta: JSON.stringify(call.arguments)};
    return result;
  }};
}
