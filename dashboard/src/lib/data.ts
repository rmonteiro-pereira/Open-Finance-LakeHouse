import "server-only";
import { promises as fs } from "node:fs";
import path from "node:path";
import { cache } from "react";

/**
 * Typed access to the OFL gold-mart snapshots in public/data/*.json.
 *
 * The snapshots are produced by snapshot/gen_synthetic.py (mock) or
 * snapshot/export.py (live DuckDB delta_scan) — identical shapes, so nothing
 * here changes when you swap to real data. Read at build time (RSC) → static.
 */

const DATA_DIR = path.join(process.cwd(), "public", "data");

const readJson = cache(async <T>(name: string): Promise<T> => {
  const raw = await fs.readFile(path.join(DATA_DIR, `${name}.json`), "utf-8");
  return JSON.parse(raw) as T;
});

/** like readJson but returns a fallback when the file is absent (e.g. a mart not
 * yet exported, or local/Vercel without the live snapshot). */
async function readJsonOr<T>(name: string, fallback: T): Promise<T> {
  try {
    return await readJson<T>(name);
  } catch {
    return fallback;
  }
}

// ---- row shapes (mirror DASHBOARD_HANDOFF.md §5) ------------------------- //
export type MacroRow = {
  month: string;
  selic_target: number;
  ipca_mom: number;
  usd_brl: number;
  debt_to_gdp_pct: number;
};

export type RealInterestRow = {
  month: string;
  selic_target: number;
  ipca_accum_12m: number;
  real_interest_rate: number;
};

export type InflationRow = {
  month: string;
  ipca_mom: number;
  ipca15_mom: number;
  inpc_mom: number;
  igpm_mom: number;
  igpm_12m: number;
  igpdi_mom: number;
};

export type FxRow = {
  series_id: string;
  date: string;
  rate: number;
  daily_return_pct: number;
  vol_21d: number;
  mtd_return_pct: number;
};

export type YieldRow = {
  date: string;
  bond: string;
  maturity: string;
  years_to_maturity: number;
  yield: number;
  buy_rate: number;
  sell_price: number;
  bond_type: "ipca_plus" | "prefixado" | "selic";
};

export type EquityRow = {
  symbol: string;
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  daily_return_pct: number;
  sma_21: number;
  vol_21d: number;
  high_52w: number;
  low_52w: number;
};

/** mart_futures_curve — B3 derivatives term structure (DI family). */
export type FuturesCurveRow = {
  date: string;
  asset: string;
  symbol: string;
  days_to_maturity: number;
  maturity: string | null;
  settlement_rate: number;
  settlement_price: number | null;
  open_interest: number | null;
};

/** mart_open_interest — daily open interest by underlying (positioning). */
export type OpenInterestRow = {
  date: string;
  asset: string;
  segment: string;
  total_open_interest: number;
  total_open_interest_var: number | null;
  n_contracts: number | null;
};

/** mart_equity_universe — one compact row per B3-listed name (whole exchange). */
export type EquityUniverseRow = {
  symbol: string;
  name: string;
  sector: string;
  close: number;
  daily_return_pct: number;
  vol_21d: number;
  high_52w: number;
  low_52w: number;
  spark: number[];
};

export type SeriesRow = {
  series_id: string;
  name: string;
  domain: string;
  source: string;
  category: string;
  unit: string;
  frequency: string;
  fact: string;
};

/** silver `fact_observation` — long-format, one row per (series_id, date). */
export type ObservationRow = {
  series_id: string;
  date: string;
  value: number;
  source: string;
};

/** a catalog entry: series metadata joined to its observation history. */
export type CatalogEntry = SeriesRow & {
  dates: string[];
  values: number[];
  latest: number | null;
  prev: number | null;
  /** % change vs previous point (null when not meaningful) */
  changePct: number | null;
  hasSeries: boolean;
  /** dedicated page that covers this series, when it isn't in fact_observation */
  href?: string;
};

export type SnapshotMeta = {
  generated_from: "synthetic" | "live";
  seed?: number;
  start?: string;
  end?: string;
  endpoint?: string;
  bucket?: string;
  note?: string;
};

// ---- loaders ------------------------------------------------------------ //
export const getMacro = () => readJson<MacroRow[]>("mart_macro_dashboard");
export const getRealInterest = () => readJson<RealInterestRow[]>("mart_real_interest");
export const getInflation = () => readJson<InflationRow[]>("mart_inflation_panel");
export const getFx = () => readJson<FxRow[]>("mart_fx");
export const getYieldCurve = () => readJson<YieldRow[]>("mart_yield_curve");
export const getEquities = () => readJson<EquityRow[]>("mart_equity_daily");
export const getEquityUniverse = () => readJsonOr<EquityUniverseRow[]>("mart_equity_universe", []);
export const getFuturesCurve = () => readJsonOr<FuturesCurveRow[]>("mart_futures_curve", []);
export const getOpenInterest = () => readJsonOr<OpenInterestRow[]>("mart_open_interest", []);
export const getSeries = () => readJson<SeriesRow[]>("dim_series");
export const getObservations = () => readJson<ObservationRow[]>("fact_observation");

/** maps a non-observation series_id to the dedicated page that visualises it. */
const SERIES_PAGE: Record<string, string> = {
  tesouro_direto: "/yield-curve",
  anbima: "/yield-curve",
  b3: "/equities",
  b3_cotahist: "/equities",
  yahoo_etf: "/equities",
  yahoo_commodity: "/equities",
  yahoo_currency: "/fx",
  yahoo_global: "/equities",
};

/**
 * The full 48-series catalog: every `dim_series` row joined to its
 * `fact_observation` history (when it has one). Series whose values live in a
 * multi-symbol fact (treasury / security_price) carry an `href` to the page
 * that plots them instead. Built once per request (React cache).
 */
export const getCatalog = cache(async (): Promise<CatalogEntry[]> => {
  const [series, obs] = await Promise.all([getSeries(), getObservations()]);
  const byId = new Map<string, ObservationRow[]>();
  for (const o of obs) {
    const arr = byId.get(o.series_id);
    if (arr) arr.push(o);
    else byId.set(o.series_id, [o]);
  }
  return series.map((s) => {
    const rows = (byId.get(s.series_id) ?? []).sort((a, b) => a.date.localeCompare(b.date));
    const values = rows.map((r) => r.value);
    const dates = rows.map((r) => r.date);
    const latest = values.length ? values[values.length - 1] : null;
    const prev = values.length > 1 ? values[values.length - 2] : null;
    const changePct =
      latest != null && prev != null && prev !== 0 ? ((latest - prev) / Math.abs(prev)) * 100 : null;
    return {
      ...s,
      dates,
      values,
      latest,
      prev,
      changePct,
      hasSeries: values.length > 1,
      href: SERIES_PAGE[s.series_id],
    };
  });
});

export const getMeta = cache(async (): Promise<SnapshotMeta> => {
  try {
    return await readJson<SnapshotMeta>("_meta");
  } catch {
    return { generated_from: "synthetic" };
  }
});
