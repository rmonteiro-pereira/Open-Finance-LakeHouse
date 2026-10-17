import { NextRequest } from "next/server";

export const dynamic = "force-dynamic";

async function proxy(
  request: NextRequest,
  context: { params: Promise<{ path: string[] }> },
) {
  const base = process.env.OFL_RESEARCH_API_URL;
  const token = process.env.OFL_RESEARCH_GATEWAY_TOKEN;
  // Tailscale Serve must overwrite this header; this server must not be public.
  const identity = request.headers.get("tailscale-user-login");
  if (!base || !token || !identity)
    return Response.json(
      { detail: "Acesso pela rede privada necessário" },
      { status: 401 },
    );
  const { path } = await context.params;
  if (path.some((part) => !/^[a-zA-Z0-9_-]+$/.test(part)))
    return new Response(null, { status: 400 });
  const origin = process.env.OFL_RESEARCH_ORIGIN;
  if (
    request.method !== "GET" &&
    (!origin || request.headers.get("origin") !== origin)
  ) {
    return Response.json({ detail: "Origem inválida" }, { status: 403 });
  }
  const headers = new Headers({
    "X-OFL-Gateway-Token": token,
    "X-OFL-Identity": identity,
  });
  for (const name of ["content-type", "origin", "idempotency-key", "range"]) {
    const value = request.headers.get(name);
    if (value) headers.set(name, value);
  }
  const target = `${base.replace(/\/$/, "")}/api/v1/${path.join("/")}${request.nextUrl.search}`;
  try {
    const result = await fetch(target, {
      method: request.method,
      headers,
      body: request.method === "GET" ? undefined : request.body,
      // Node's streaming request bodies require duplex.
      ...({ duplex: "half" } as Record<string, string>),
      cache: "no-store",
      signal: AbortSignal.timeout(120_000),
    });
    const outgoing = new Headers({
      "Cache-Control": "no-store",
      "X-Content-Type-Options": "nosniff",
    });
    for (const name of [
      "content-type",
      "content-disposition",
      "content-range",
      "accept-ranges",
    ]) {
      const value = result.headers.get(name);
      if (value) outgoing.set(name, value);
    }
    return new Response(result.body, {
      status: result.status,
      headers: outgoing,
    });
  } catch {
    return Response.json(
      { detail: "Serviço de pesquisa indisponível" },
      { status: 503 },
    );
  }
}
export { proxy as GET, proxy as POST, proxy as PUT };
