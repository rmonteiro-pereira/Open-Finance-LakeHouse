"use client";

import { useRef, useState } from "react";
import Workbench from "./workbench";
import ResearchDesk from "./research-desk";
import DataExplorer from "./data-explorer";
import { sha256 } from "@noble/hashes/sha2.js";
import { bytesToHex } from "@noble/hashes/utils.js";

type Row = Record<string, unknown>;
type Hit = {
  id: string;
  item_id: string;
  title: string;
  snippet: string;
  source: string;
  method: string;
  page: number | null;
  asset_id: string | null;
};
type Asset = {
  id: string;
  name: string;
  media_type: string;
  state: string;
  error: string | null;
};
type Item = {
  item: Row;
  assets: Asset[];
  pages: {
    id: string;
    page: number | null;
    text: string;
    method: string;
    asset_id: string | null;
  }[];
};
type Candidate = {
  id: string;
  asset_id: string;
  page: number;
  cells: (string | null)[][];
  current_revision: number;
  bbox: number[];
  state: string;
};
type Field = { column?: number; constant?: string };
type Mapping = Record<string, Field | string | number>;
const base = "/api/research/";

async function api<T>(
  path: string,
  body?: unknown,
  method = "POST",
): Promise<T> {
  const result = await fetch(
    base + path,
    body === undefined
      ? { cache: "no-store" }
      : {
          method,
          headers: {
            "Content-Type": "application/json",
            "Idempotency-Key": crypto.randomUUID(),
          },
          body: JSON.stringify(body),
        },
  );
  const data = await result.json();
  if (!result.ok)
    throw new Error(
      typeof data.detail === "string"
        ? data.detail
        : JSON.stringify(data.detail),
    );
  return data;
}
function display(value: unknown): string {
  return value == null
    ? "—"
    : typeof value === "object"
      ? JSON.stringify(value)
      : String(value);
}
function Grid({ rows, columns }: { rows: Row[]; columns: string[] }) {
  return (
    <div className="scroll">
      <table>
        <thead>
          <tr>
            {columns.map((c) => (
              <th key={c}>{c}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i}>
              {columns.map((c) => (
                <td key={c}>{display(row[c])}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {!rows.length && <p className="muted">Nenhum registro disponível.</p>}
    </div>
  );
}

function RegionImage({
  src,
  onSelect,
}: {
  src: string;
  onSelect: (box: number[]) => void;
}) {
  const image = useRef<HTMLImageElement>(null);
  const [start, setStart] = useState<number[] | null>(null);
  const [box, setBox] = useState<number[] | null>(null);
  const [size, setSize] = useState([1, 1]);
  function point(event: React.PointerEvent<HTMLImageElement>) {
    const img = image.current!;
    const rect = img.getBoundingClientRect();
    const scale = Math.min(
      rect.width / img.naturalWidth,
      rect.height / img.naturalHeight,
    );
    return [
      Math.round(
        Math.max(
          0,
          Math.min(
            img.naturalWidth,
            (event.clientX -
              rect.left -
              (rect.width - img.naturalWidth * scale) / 2) /
              scale,
          ),
        ),
      ),
      Math.round(
        Math.max(
          0,
          Math.min(
            img.naturalHeight,
            (event.clientY -
              rect.top -
              (rect.height - img.naturalHeight * scale) / 2) /
              scale,
          ),
        ),
      ),
    ];
  }
  return (
    <>
      <div style={{ position: "relative" }}>
        <img
          ref={image}
          className="preview"
          src={src}
          alt="Página de origem; arraste para selecionar uma região"
          style={{ display: "block", touchAction: "none" }}
          onLoad={() =>
            setSize([image.current!.naturalWidth, image.current!.naturalHeight])
          }
          onPointerDown={(e) => {
            e.preventDefault();
            e.currentTarget.setPointerCapture(e.pointerId);
            setStart(point(e));
          }}
          onPointerUp={(e) => {
            if (!start) return;
            const end = point(e);
            const next = [
              Math.min(start[0], end[0]),
              Math.min(start[1], end[1]),
              Math.max(start[0], end[0]),
              Math.max(start[1], end[1]),
            ];
            if (next[2] - next[0] > 5 && next[3] - next[1] > 5) setBox(next);
            setStart(null);
          }}
        />
        {box && (
          <svg
            viewBox={`0 0 ${size[0]} ${size[1]}`}
            style={{
              position: "absolute",
              inset: 0,
              width: "100%",
              height: "100%",
              pointerEvents: "none",
            }}
          >
            <rect
              x={box[0]}
              y={box[1]}
              width={box[2] - box[0]}
              height={box[3] - box[1]}
              fill="#16a17833"
              stroke="#16a178"
              strokeWidth="4"
            />
          </svg>
        )}
      </div>
      <p className="muted">
        Arraste sobre a página para delimitar uma nova tabela.
      </p>
      {box && (
        <button onClick={() => onSelect(box)}>Usar região marcada</button>
      )}
    </>
  );
}

export default function Workspace() {
  const [tab, setTab] = useState("Início");
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [symbol, setSymbol] = useState("PETR4");
  const [market, setMarket] = useState("010");
  const [from, setFrom] = useState("");
  const [to, setTo] = useState("");
  const [quotes, setQuotes] = useState<Row[]>([]);
  const [release, setRelease] = useState<string | null>(null);
  const [q, setQ] = useState("");
  const [mode, setMode] = useState("keyword");
  const [source, setSource] = useState("");
  const [itemType, setItemType] = useState("");
  const [hits, setHits] = useState<Hit[]>([]);
  const [item, setItem] = useState<Item | null>(null);
  const [candidates, setCandidates] = useState<Candidate[]>([]);
  const [candidate, setCandidate] = useState<Candidate | null>(null);
  const [reviewState, setReviewState] = useState("needs_review");
  const [cells, setCells] = useState<(string | null)[][]>([]);
  const [mapping, setMapping] = useState<Mapping>({
    date: { column: 0 },
    subject: { constant: "" },
    metric: { constant: "close" },
    value: { column: 1 },
    unit: { constant: "price" },
    currency: { constant: "BRL" },
    market: { constant: "010" },
    header_rows: 1,
    decimal_separator: ",",
    thousands_separator: ".",
  });
  const [jobs, setJobs] = useState<Row[]>([]);
  const [imports, setImports] = useState<Row[]>([]);
  const [supersedes, setSupersedes] = useState<Record<string, string>>({});
  const [coverage, setCoverage] = useState<Row | null>(null);
  const [pipelineEnabled, setPipelineEnabled] = useState(false);
  const [uploadSource, setUploadSource] = useState("b3_cotahist");
  const [observations, setObservations] = useState<Row[]>([]);
  async function work(task: () => Promise<void>) {
    setBusy(true);
    setError("");
    setMessage("");
    try {
      await task();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Falha na solicitação");
    } finally {
      setBusy(false);
    }
  }
  const filters = () =>
    Object.fromEntries(
      Object.entries({ symbol, market, date_from: from, date_to: to }).filter(
        ([, v]) => v,
      ),
    );
  async function loadMarkets() {
    const data = await api<{ rows: Row[]; release: string | null }>(
      "quotes?" + new URLSearchParams({ ...filters(), limit: "1000" }),
    );
    setQuotes(data.rows);
    setRelease(data.release);
  }
  async function loadOps() {
    const [j, i, c, capabilities] = await Promise.all([
      api<Row[]>("jobs"),
      api<Row[]>("imports"),
      api<Row>("coverage"),
      api<{ pipeline_enabled: boolean }>("me"),
    ]);
    setJobs(j);
    setImports(i);
    setCoverage(c);
    setPipelineEnabled(capabilities.pipeline_enabled);
  }
  async function selectCandidate(c: Candidate) {
    const detail = await api<{
      candidate: Candidate;
      reviews: { cells: (string | null)[][]; mapping: Mapping }[];
    }>(`candidates/${c.id}`);
    setCandidate(detail.candidate);
    const latest = detail.reviews.at(-1);
    setCells(latest?.cells ?? detail.candidate.cells);
    if (latest) setMapping(latest.mapping);
  }
  async function upload(file: File) {
    const hash = sha256.create();
    const chunkSize = 4 * 1024 * 1024;
    for (let at = 0; at < file.size; at += chunkSize) {
      hash.update(
        new Uint8Array(await file.slice(at, at + chunkSize).arrayBuffer()),
      );
      setMessage(
        `Verificando arquivo: ${Math.round((Math.min(at + chunkSize, file.size) / file.size) * 100)}%`,
      );
    }
    const checksum = bytesToHex(hash.digest());
    const localKey = `ofl-upload:${uploadSource}:${checksum}`;
    let id = localStorage.getItem(localKey);
    let offset = 0;
    if (id) {
      try {
        const previous = await api<{ received: number; state: string }>(
          `uploads/${id}`,
        );
        offset = previous.received;
        if (previous.state === "complete") {
          setMessage("Arquivo já recebido.");
          return;
        }
      } catch {
        id = null;
      }
    }
    if (!id) {
      const registered = await api<{ upload_id: string }>("uploads", {
        name: file.name,
        source: uploadSource,
        size: file.size,
        sha256: checksum,
      });
      id = registered.upload_id;
      localStorage.setItem(localKey, id);
    }
    for (let at = offset; at < file.size; at += chunkSize) {
      const result = await fetch(`${base}uploads/${id}?offset=${at}`, {
        method: "PUT",
        body: file.slice(at, at + chunkSize),
      });
      if (!result.ok) throw new Error((await result.json()).detail);
      setMessage(
        `Enviando: ${Math.round((Math.min(at + chunkSize, file.size) / file.size) * 100)}%`,
      );
    }
    const result = await api<{ job_id: string }>(`uploads/${id}/complete`, {});
    localStorage.removeItem(localKey);
    setMessage(`Arquivo recebido. Processamento: ${result.job_id}`);
    await loadOps();
  }
  const values = quotes
    .map((r) => Number(r.close_per_unit))
    .filter(Number.isFinite);
  const low = Math.min(...values),
    high = Math.max(...values);
  const chart = quotes
    .filter((r) => r.close_per_unit != null)
    .map(
      (r, i) =>
        `${(i / Math.max(1, quotes.length - 1)) * 1000},${180 - ((Number(r.close_per_unit) - low) / Math.max(0.001, high - low)) * 160}`,
    )
    .join(" ");
  return (
    <>
      <header>
        <strong>
          OFL <span> / Pesquisa</span>
        </strong>
        <span>Mercados históricos · Acervo documental · Acesso privado</span>
      </header>
      <main>
        <h1>Entenda mercados. Trabalhe com evidências.</h1>
        <p className="muted">
          Dados com origem identificada. Documentos, páginas e valores revisados
          no mesmo lugar.
        </p>
        <nav aria-label="Áreas">
          {["Início", "Pesquisa", "Dados", "Mercados", "Acervo", "Revisão", "Operações"].map((t) => (
            <button
              key={t}
              aria-current={tab === t}
              onClick={() => {
                setTab(t);
                setError("");
              }}
            >
              {t}
            </button>
          ))}
        </nav>
        {error && (
          <div role="alert" className="status error">
            {error}
          </div>
        )}
        {message && (
          <div role="status" className="status">
            {message}
          </div>
        )}
        {tab === "Início" && <Workbench navigate={setTab} />}
        {tab === "Pesquisa" && <ResearchDesk />}
        {tab === "Dados" && <DataExplorer />}
        {tab === "Mercados" && (
          <>
            <section className="card">
              <div className="filters">
                <label>
                  Ativo
                  <input
                    value={symbol}
                    onChange={(e) => setSymbol(e.target.value.toUpperCase())}
                  />
                </label>
                <label>
                  Mercado
                  <select
                    value={market}
                    onChange={(e) => setMarket(e.target.value)}
                  >
                    {[
                      ["010", "À vista"],
                      ["020", "Fracionário"],
                      ["021", "Código 021 (não classificado)"],
                      ["030", "Termo"],
                      ["070", "Opções de compra"],
                      ["080", "Opções de venda"],
                      ["012", "Exercício de compra"],
                      ["013", "Exercício de venda"],
                      ["017", "Leilão"],
                    ].map(([v, l]) => (
                      <option key={v} value={v}>
                        {l}
                      </option>
                    ))}
                  </select>
                </label>
                <label>
                  De
                  <input
                    type="date"
                    value={from}
                    onChange={(e) => setFrom(e.target.value)}
                  />
                </label>
                <label>
                  Até
                  <input
                    type="date"
                    value={to}
                    onChange={(e) => setTo(e.target.value)}
                  />
                </label>
                <button
                  className="primary"
                  disabled={busy}
                  onClick={() => work(loadMarkets)}
                >
                  Consultar
                </button>
                <button
                  disabled={busy || !release}
                  onClick={() =>
                    work(async () => {
                      const result = await api<{ job_id: string }>("exports", {
                        filters: filters(),
                      });
                      setMessage(
                        `Exportação solicitada: ${result.job_id}. Acompanhe em Operações.`,
                      );
                    })
                  }
                >
                  Exportar Parquet
                </button>
              </div>
              <p className="muted">
                Cotações sem ajuste por proventos ou inflação. Moedas e fatores
                de cotação originais são preservados. Até 1.000 registros por
                consulta.
              </p>
              {quotes.length > 1 &&
                new Set(
                  quotes.map(
                    (r) =>
                      `${r.currency}|${r.quotation_factor}|${r.isin}|${r.distribution}|${r.expiry_raw}`,
                  ),
                ).size === 1 && (
                  <svg
                    className="chart"
                    role="img"
                    aria-label="Último preço por unidade, sem ajuste"
                    viewBox="0 0 1000 200"
                  >
                    <polyline
                      points={chart}
                      fill="none"
                      stroke="var(--accent)"
                      strokeWidth="3"
                    />
                  </svg>
                )}
              <Grid
                rows={quotes}
                columns={[
                  "date",
                  "symbol",
                  "tpmerc",
                  "currency",
                  "quotation_factor",
                  "open",
                  "high",
                  "low",
                  "close",
                  "close_per_unit",
                  "quantity",
                  "source_hash",
                  "source_line",
                ]}
              />
              {release && <p className="muted">Versão consultada: {release}</p>}
            </section>
            <section className="card">
              <h2>Valores extraídos e aprovados</h2>
              <button
                disabled={busy}
                onClick={() =>
                  work(async () =>
                    setObservations(await api<Row[]>("observations")),
                  )
                }
              >
                Consultar valores revisados
              </button>
              <Grid
                rows={observations}
                columns={[
                  "date_text",
                  "date",
                  "date_end",
                  "date_precision",
                  "subject",
                  "market",
                  "metric",
                  "value",
                  "unit",
                  "currency",
                  "revision",
                  "evidence",
                ]}
              />
            </section>
          </>
        )}
        {tab === "Acervo" && (
          <>
            <section className="card">
              <form
                className="filters"
                onSubmit={(e) => {
                  e.preventDefault();
                  work(async () => {
                    const params = new URLSearchParams({
                      q,
                      mode,
                      ...(source ? { source } : {}),
                      ...(itemType ? { item_type: itemType } : {}),
                    });
                    setHits(
                      (await api<{ results: Hit[] }>("search?" + params))
                        .results,
                    );
                    setItem(null);
                  });
                }}
              >
                <label>
                  Buscar documentos
                  <input
                    required
                    value={q}
                    onChange={(e) => setQ(e.target.value)}
                    placeholder="História do pregão, instituições, pessoas…"
                  />
                </label>
                <label>
                  Busca
                  <select
                    value={mode}
                    onChange={(e) => setMode(e.target.value)}
                  >
                    <option value="keyword">Palavras e identificadores</option>
                    <option value="hybrid">Palavras e significado</option>
                    <option value="vector">Significado</option>
                  </select>
                </label>
                <label>
                  Fonte
                  <select
                    value={source}
                    onChange={(e) => setSource(e.target.value)}
                  >
                    <option value="">Todas</option>
                    <option value="news">Notícias</option>
                    <option value="manager_letter">Cartas de gestoras</option>
                    <option value="mub3">MUB3</option>
                    <option value="b3_bulletin">Boletins B3</option>
                  </select>
                </label>
                <label>
                  Tipo
                  <input
                    value={itemType}
                    onChange={(e) => setItemType(e.target.value)}
                    placeholder="photograph, document…"
                  />
                </label>
                <button className="primary" disabled={busy}>
                  Buscar
                </button>
                <button
                  type="button"
                  disabled={busy}
                  onClick={() =>
                    work(async () => {
                      const rows = await api<Row[]>("items");
                      setHits(
                        rows.map((r) => ({
                          id: String(r.id),
                          item_id: String(r.id),
                          title: String(r.title),
                          snippet: String(r.description),
                          source: String(r.source),
                          method: "metadata",
                          page: null,
                          asset_id: null,
                        })),
                      );
                    })
                  }
                >
                  Explorar catálogo
                </button>
              </form>
            </section>
            <div className="results card">
              {hits.map((hit) => (
                <article key={hit.id}>
                  <h3>
                    <button
                      onClick={() =>
                        work(async () =>
                          setItem(await api<Item>(`items/${hit.item_id}`)),
                        )
                      }
                    >
                      {hit.title}
                    </button>
                  </h3>
                  <p>{hit.snippet}</p>
                  <span className="tag">
                    {hit.source} ·{" "}
                    {hit.method === "ocr"
                      ? "Texto OCR"
                      : hit.method === "metadata"
                        ? "Catálogo"
                        : "Texto"}
                    {hit.page ? ` · página ${hit.page}` : ""}
                  </span>
                </article>
              ))}
              {!hits.length && (
                <p className="muted">
                  Busque um assunto ou explore os registros recebidos.
                </p>
              )}
            </div>
            {item && (
              <section className="card">
                <h2>{display(item.item.title)}</h2>
                <p>{display(item.item.description)}</p>
                <p className="muted">
                  {display(item.item.date_text)} ·{" "}
                  {display(item.item.collection)}
                </p>
                {item.assets.map((asset) => (
                  <div key={asset.id}>
                    <p>
                      <a
                        href={`${base}assets/${asset.id}`}
                        target="_blank"
                        rel="noreferrer"
                      >
                        {asset.name}
                      </a>{" "}
                      <span className="tag">{asset.state}</span>
                    </p>
                    {asset.error && <p>{asset.error}</p>}
                  </div>
                ))}
                {item.pages
                  .filter((p) => p.asset_id)
                  .map((page) => (
                    <article key={page.id}>
                      <h3>
                        Página {page.page ?? "—"} · {page.method}
                      </h3>
                      <p style={{ whiteSpace: "pre-wrap" }}>{page.text}</p>
                      {page.page && (
                        <>
                          <a
                            href={`${base}assets/${page.asset_id}/pages/${page.page}`}
                            target="_blank"
                            rel="noreferrer"
                          >
                            Abrir imagem da página
                          </a>
                          <button
                            onClick={() =>
                              work(async () => {
                                const result = await api<{
                                  candidate_id: string;
                                }>("candidates", {
                                  asset_id: page.asset_id,
                                  page: page.page,
                                  bbox: [0, 0, 1, 1],
                                  cells: [
                                    ["data", "valor"],
                                    ["", ""],
                                  ],
                                });
                                const detail = await api<{
                                  candidate: Candidate;
                                }>(`candidates/${result.candidate_id}`);
                                await selectCandidate(detail.candidate);
                                setTab("Revisão");
                              })
                            }
                          >
                            Criar tabela para revisão
                          </button>
                        </>
                      )}
                    </article>
                  ))}
              </section>
            )}
          </>
        )}
        {tab === "Revisão" && (
          <>
            <section className="card">
              <h2>Tabelas para revisão</h2>
              <label>
                Estado
                <select
                  value={reviewState}
                  onChange={(e) => setReviewState(e.target.value)}
                >
                  <option value="needs_review">Pendentes</option>
                  <option value="approved">Aprovadas</option>
                  <option value="rejected">Rejeitadas</option>
                </select>
              </label>
              <button
                disabled={busy}
                onClick={() =>
                  work(async () =>
                    setCandidates(
                      await api<Candidate[]>(`candidates?state=${reviewState}`),
                    ),
                  )
                }
              >
                Atualizar fila
              </button>
              <div className="actions">
                {candidates.map((c) => (
                  <button
                    key={c.id}
                    onClick={() => work(() => selectCandidate(c))}
                  >
                    Página {c.page} · {c.id.slice(0, 8)}
                  </button>
                ))}
              </div>
            </section>
            {candidate && (
              <section className="card">
                <h2>Confira a fonte antes de aprovar</h2>
                <div className="split">
                  <div>
                    <RegionImage
                      key={candidate.id}
                      src={`${base}assets/${candidate.asset_id}/pages/${candidate.page}`}
                      onSelect={(bbox) =>
                        work(async () => {
                          const created = await api<{ candidate_id: string }>(
                            "candidates",
                            {
                              asset_id: candidate.asset_id,
                              page: candidate.page,
                              bbox,
                              cells,
                            },
                          );
                          const detail = await api<{ candidate: Candidate }>(
                            `candidates/${created.candidate_id}`,
                          );
                          await selectCandidate(detail.candidate);
                        })
                      }
                    />
                    <p className="muted">Região: {candidate.bbox.join(", ")}</p>
                    <a
                      href={`${base}assets/${candidate.asset_id}`}
                      target="_blank"
                      rel="noreferrer"
                    >
                      Abrir original
                    </a>
                  </div>
                  <div>
                    <div className="scroll">
                      <table>
                        <tbody>
                          {cells.map((row, i) => (
                            <tr key={i}>
                              {row.map((value, j) => (
                                <td key={j}>
                                  <input
                                    aria-label={`Linha ${i + 1}, coluna ${j + 1}`}
                                    value={value ?? ""}
                                    onChange={(e) =>
                                      setCells((previous) =>
                                        previous.map((r, ri) =>
                                          ri === i
                                            ? r.map((v, ci) =>
                                                ci === j ? e.target.value : v,
                                              )
                                            : r,
                                        ),
                                      )
                                    }
                                  />
                                </td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                    <div className="actions">
                      <button
                        onClick={() =>
                          setCells([
                            ...cells,
                            Array(cells[0]?.length ?? 2).fill(""),
                          ])
                        }
                      >
                        Adicionar linha
                      </button>
                      <button
                        onClick={() =>
                          setCells(cells.map((row) => [...row, ""]))
                        }
                      >
                        Adicionar coluna
                      </button>
                    </div>
                  </div>
                </div>
                <h3>Identifique as colunas e unidades</h3>
                <div className="mapping">
                  {[
                    "date",
                    "subject",
                    "metric",
                    "value",
                    "unit",
                    "currency",
                    "market",
                  ].map((key) => {
                    const field = (mapping[key] ?? { constant: "" }) as Field;
                    return (
                      <label key={key}>
                        {
                          (
                            {
                              date: "Data ou período",
                              subject: "Ativo ou assunto",
                              metric: "Medida",
                              value: "Valor",
                              unit: "Unidade",
                              currency: "Moeda",
                              market: "Código de mercado",
                            } as Record<string, string>
                          )[key]
                        }
                        <select
                          value={
                            field.column === undefined
                              ? "constant"
                              : String(field.column)
                          }
                          onChange={(e) =>
                            setMapping({
                              ...mapping,
                              [key]:
                                e.target.value === "constant"
                                  ? { constant: "" }
                                  : { column: Number(e.target.value) },
                            })
                          }
                        >
                          <option value="constant">Valor fixo</option>
                          {(cells[0] ?? []).map((v, i) => (
                            <option key={i} value={i}>
                              Coluna {i + 1}: {v}
                            </option>
                          ))}
                        </select>
                        {field.column === undefined && (
                          <input
                            value={field.constant ?? ""}
                            onChange={(e) =>
                              setMapping({
                                ...mapping,
                                [key]: { constant: e.target.value },
                              })
                            }
                          />
                        )}
                      </label>
                    );
                  })}
                  <label>
                    Formato da data
                    <select
                      value={String(mapping.date_format ?? "%Y-%m-%d")}
                      onChange={(e) =>
                        setMapping({ ...mapping, date_format: e.target.value })
                      }
                    >
                      <option value="%Y-%m-%d">AAAA-MM-DD</option>
                      <option value="%d/%m/%Y">DD/MM/AAAA</option>
                      <option value="%Y-%m">AAAA-MM (mês)</option>
                      <option value="%m/%Y">MM/AAAA (mês)</option>
                      <option value="%Y">AAAA (ano)</option>
                    </select>
                  </label>
                  <label>
                    Linhas de cabeçalho
                    <input
                      type="number"
                      min="0"
                      value={Number(mapping.header_rows ?? 1)}
                      onChange={(e) =>
                        setMapping({
                          ...mapping,
                          header_rows: Number(e.target.value),
                        })
                      }
                    />
                  </label>
                  <label>
                    Separador decimal
                    <select
                      value={String(mapping.decimal_separator)}
                      onChange={(e) =>
                        setMapping({
                          ...mapping,
                          decimal_separator: e.target.value,
                          thousands_separator:
                            e.target.value === "," ? "." : ",",
                        })
                      }
                    >
                      <option value=",">Vírgula</option>
                      <option value=".">Ponto</option>
                    </select>
                  </label>
                </div>
                <div className="actions">
                  {[
                    ["approved", "Aprovar valores"],
                    ["needs_review", "Salvar revisão"],
                    ["rejected", "Rejeitar"],
                  ].map(([state, label]) => (
                    <button
                      key={state}
                      className={state === "approved" ? "primary" : ""}
                      disabled={busy}
                      onClick={() =>
                        work(async () => {
                          const result = await api<{
                            revision: number;
                            observations: number;
                          }>(`candidates/${candidate.id}/review`, {
                            expected_revision: candidate.current_revision,
                            cells,
                            mapping,
                            state,
                          });
                          setCandidate({
                            ...candidate,
                            current_revision: result.revision,
                            state,
                          });
                          setMessage(
                            `Revisão ${result.revision} salva. ${result.observations} valores aprovados.`,
                          );
                        })
                      }
                    >
                      {label}
                    </button>
                  ))}
                </div>
              </section>
            )}
          </>
        )}
        {tab === "Operações" && (
          <>
            <section className="card">
              <h2>Receber arquivos</h2>
              <p className="muted">
                COTAHIST ZIP/TXT, boletins, documentos ou pacote ZIP com
                catalog.jsonl e seus arquivos. Envios podem ser retomados
                selecionando o mesmo arquivo.
              </p>
              <div className="filters">
                <label>
                  Fonte
                  <select
                    value={uploadSource}
                    onChange={(e) => setUploadSource(e.target.value)}
                  >
                    <option value="b3_cotahist">COTAHIST</option>
                    <option value="b3_bulletin">Boletim B3</option>
                    <option value="mub3">MUB3</option>
                  </select>
                </label>
                <label>
                  Arquivo
                  <input
                    type="file"
                    disabled={busy}
                    onChange={(e) => {
                      const file = e.target.files?.[0];
                      if (file) work(() => upload(file));
                    }}
                  />
                </label>
                <button disabled={busy} onClick={() => work(loadOps)}>
                  Atualizar operações
                </button>
              </div>
            </section>
            <section className="card">
              <h2>Processamento</h2>
              <div className="actions">
                {[
                  ["embed", "Preparar busca semântica"],
                  ["build_observations", "Publicar valores revisados"],
                ].map(([action, label]) => (
                  <button
                    key={action}
                    disabled={busy}
                    onClick={() =>
                      work(async () => {
                        const job = await api<{ job_id: string }>("jobs", {
                          action,
                        });
                        setMessage(`Processamento solicitado: ${job.job_id}`);
                        await loadOps();
                      })
                    }
                  >
                    {label}
                  </button>
                ))}
                {[
                  "ofl_research_worker",
                  "ofl_silver",
                  "ofl_gold",
                  "ofl_backfill",
                ].map((dag) => (
                  <button
                    key={dag}
                    disabled={busy || !pipelineEnabled}
                    onClick={() =>
                      work(async () => {
                        const job = await api<{ job_id: string }>("jobs", {
                          action: "pipeline",
                          dag,
                        });
                        setMessage(`Execução solicitada: ${job.job_id}`);
                        await loadOps();
                      })
                    }
                  >
                    Executar {dag}
                  </button>
                ))}
              </div>
              {!pipelineEnabled && (
                <p className="muted">
                  A execução dos pipelines externos ainda não está conectada.
                </p>
              )}
              <Grid
                rows={imports}
                columns={["name", "source", "state", "size", "counts", "error"]}
              />
              {imports
                .filter((row) => row.state === "failed")
                .map((row) => (
                  <div className="actions" key={String(row.id)}>
                    <label>
                      Reprocessar {display(row.name)}
                      <select
                        value={supersedes[String(row.id)] ?? ""}
                        onChange={(e) =>
                          setSupersedes({
                            ...supersedes,
                            [String(row.id)]: e.target.value,
                          })
                        }
                      >
                        <option value="">Sem substituir outro arquivo</option>
                        {imports
                          .filter(
                            (previous) =>
                              previous.state === "published" &&
                              previous.source === row.source,
                          )
                          .map((previous) => (
                            <option
                              key={String(previous.id)}
                              value={String(previous.id)}
                            >
                              Substituir {display(previous.name)} ·{" "}
                              {String(previous.id).slice(0, 8)}
                            </option>
                          ))}
                      </select>
                    </label>
                    <button
                      disabled={busy}
                      onClick={() =>
                        work(async () => {
                          await api(`imports/${row.id}/reprocess`, {
                            supersedes: supersedes[String(row.id)] || null,
                          });
                          await loadOps();
                        })
                      }
                    >
                      Reprocessar arquivo
                    </button>
                  </div>
                ))}
              <h3>Fila de tarefas</h3>
              {jobs.map((job) => (
                <div key={String(job.id)}>
                  <p>
                    <span className="tag">{display(job.state)}</span>{" "}
                    {display(job.kind)} · {display(job.id)}
                  </p>
                  {job.error != null && <p>{display(job.error)}</p>}
                  {job.state === "failed" && (
                    <button
                      disabled={busy}
                      onClick={() =>
                        work(async () => {
                          await api(`jobs/${job.id}/retry`, {});
                          await loadOps();
                        })
                      }
                    >
                      Tentar novamente
                    </button>
                  )}
                  {job.kind === "export" && job.state === "done" && (
                    <a href={`${base}exports/${job.id}`}>Baixar Parquet</a>
                  )}
                  <pre>{display(job.progress)}</pre>
                </div>
              ))}
            </section>
            {coverage && (
              <section className="card">
                <h2>Cobertura recebida</h2>
                {["quote_years", "items", "assets", "reviews"].map((key) => (
                  <div key={key}>
                    <h3>{key}</h3>
                    <Grid
                      rows={(coverage[key] ?? []) as Row[]}
                      columns={Object.keys(
                        ((coverage[key] as Row[]) ?? [])[0] ?? {},
                      )}
                    />
                  </div>
                ))}
                <p className="muted">
                  Anos ainda não recebidos:{" "}
                  {((coverage.missing_years ?? []) as number[]).join(", ") ||
                    "nenhum"}
                  . Anos sem arquivo recebido não contam como cobertura
                  completa. Catálogo, arquivos e OCR têm coberturas
                  independentes.
                </p>
              </section>
            )}
          </>
        )}
        <footer className="muted">
          Fontes e revisões preservadas · Novos dados privados · Sem coleta
          automática dos sites de origem
        </footer>
      </main>
    </>
  );
}
