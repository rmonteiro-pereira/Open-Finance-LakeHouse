import { NextRequest } from "next/server";
export const dynamic = "force-dynamic";
async function proxy(request: NextRequest, context: {params: Promise<{path: string[]}>}) {
  const base = process.env.OFL_RESEARCH_AGENT_URL;
  const token = process.env.OFL_RESEARCH_GATEWAY_TOKEN;
  const identity = request.headers.get("tailscale-user-login");
  const origin = process.env.OFL_RESEARCH_ORIGIN;
  if (!token || !identity) return Response.json({detail: "Acesso pela rede privada necessário"}, {status: 401});
  if (!base) return Response.json({detail: "Os agentes OFL ainda não estão configurados neste ambiente"}, {status: 503});
  if (request.method !== "GET" && (!origin || request.headers.get("origin") !== origin)) return Response.json({detail: "Origem inválida"}, {status: 403});
  const {path} = await context.params;
  if (!path.length || path.some(part => !/^[a-zA-Z0-9_-]+$/.test(part))) return new Response(null, {status: 400});
  const endpoint = path.length === 1 && path[0] === "catalog" ? "" : `/${path.join("/")}`;
  const headers = new Headers({"X-OFL-Gateway-Token": token, "X-OFL-Identity": identity});
  for (const name of ["content-type", "origin", "accept", "x-chat-stream"]) {
    const value = request.headers.get(name); if (value) headers.set(name, value);
  }
  try {
    const result = await fetch(`${base.replace(/\/$/, "")}/api/agents${endpoint}${request.nextUrl.search}`, {
      method: request.method, headers, body: request.method === "GET" ? undefined : request.body,
      ...({duplex: "half"} as Record<string, string>), cache: "no-store", redirect: "error", signal: AbortSignal.any([request.signal, AbortSignal.timeout(Math.min(600_000, Math.max(10_000, Number(process.env.OFL_AGENT_REQUEST_TIMEOUT_MS) || 120_000)))]),
    });
    return new Response(result.body, {status: result.status, headers: {"Content-Type": result.headers.get("content-type") ?? "application/json", "Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"}});
  } catch { return Response.json({detail: "O agente está indisponível. Tente novamente."}, {status: 503}); }
}
export const GET = proxy;
export const POST = proxy;
export const DELETE = proxy;
