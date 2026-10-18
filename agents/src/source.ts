/** No arbitrary SQL, filesystem, network destinations, or writes available to agents. */
import { ToolFailure } from "@inkwell/core";
export type Params = Record<string, string | number | undefined>;
export interface ResearchSource { read(path: string, params?: Params, signal?: AbortSignal): Promise<unknown>; }
const PATH = /^(me|coverage|quotes|observations|releases|jobs|imports|items(?:\/[a-zA-Z0-9_-]+)?|search|candidates(?:\/[a-zA-Z0-9_-]+)?)$/;
export function oflSource(base: string, gatewayToken: string, identity: string, transport: (input: URL | RequestInfo, init?: RequestInit) => Promise<Response> = fetch): ResearchSource {
  const url = new URL(base);
  if (!["http:", "https:"].includes(url.protocol) || url.username || url.password || url.search || url.hash) throw new Error("Invalid OFL service URL");
  if (!gatewayToken || !identity) throw new Error("OFL identity and gateway credentials required");
  return { async read(path, params = {}, signal) {
    if (!PATH.test(path)) throw new ToolFailure("source_query");
    const target = new URL(`${base.replace(/\/$/, "")}/api/v1/${path}`);
    for (const [key, value] of Object.entries(params)) if (value !== undefined && value !== "") target.searchParams.set(key, String(value));
    try {
      const result = await transport(target, {headers: {"X-OFL-Gateway-Token": gatewayToken, "X-OFL-Identity": identity}, signal: signal ? AbortSignal.any([signal, AbortSignal.timeout(45_000)]) : AbortSignal.timeout(45_000), redirect: "error", cache: "no-store"});
      if (!result.ok) throw new Error("source unavailable");
      return await result.json();
    } catch { throw new ToolFailure("source_access"); }
  }};
}
