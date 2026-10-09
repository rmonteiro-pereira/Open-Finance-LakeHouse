// The site's only data source: the snapshot `ofl publish` pushes after each gold run.
// Nothing here talks to the cluster.
import { parquetReadObjects } from "hyparquet";
import { compressors } from "hyparquet-compressors";
import { cacheLife } from "next/cache";

export type Tier = "open" | "derived" | "private";

export type Mart = {
  name: string;
  path: string;
  redistribution: Tier;
  download: boolean;
  rows: number;
  bytes: number;
  sha256: string;
  columns: { name: string; type: string }[];
  date_column?: string;
  min_date?: string;
  max_date?: string;
  inputs: string[];
};

export type Manifest = {
  schema_version: number;
  run_id: string;
  generated_at: string;
  marts: Mart[];
  observations: { path: string; rows: number; series: number; bytes: number } | null;
  catalog: string;
  skipped: { name: string; reason: "private" | "missing" | "empty" }[];
};

export type Series = {
  key: string;
  name: string;
  domain: string;
  source: string;
  unit: string;
  frequency: string;
  redistribution: Tier;
  rows: number | null;
  first_date: string | null;
  last_date: string | null;
};

/** One time series as parallel arrays: epoch milliseconds and values. */
export type Line = { t: number[]; v: (number | null)[] };

export type Dashboard = {
  manifest: Manifest;
  series: Series[];
  baseUrl: string;
  macro: Record<"selic_target" | "ipca_12m" | "real_rate" | "debt_to_gdp", Line>;
  usdBrl: Line;
};

function baseUrl(): string {
  const url = process.env.SNAPSHOT_URL;
  if (!url) throw new Error("SNAPSHOT_URL is not set: point it at the bucket `ofl publish` writes to.");
  return url.replace(/\/+$/, "");
}

async function getJson<T>(path: string): Promise<T> {
  const res = await fetch(`${baseUrl()}/${path}`);
  if (!res.ok) throw new Error(`snapshot: ${path} answered ${res.status}`);
  return res.json() as Promise<T>;
}

async function getRows(path: string, columns: string[]): Promise<Record<string, unknown>[]> {
  const res = await fetch(`${baseUrl()}/${path}`);
  if (!res.ok) throw new Error(`snapshot: ${path} answered ${res.status}`);
  return parquetReadObjects({ file: await res.arrayBuffer(), columns, compressors });
}

function toLine(rows: Record<string, unknown>[], dateColumn: string, valueColumn: string): Line {
  const line: Line = { t: [], v: [] };
  for (const row of rows) {
    const value = row[valueColumn];
    line.t.push((row[dateColumn] as Date).getTime());
    line.v.push(value == null ? null : Number(value));
  }
  return line;
}

export async function getDashboard(): Promise<Dashboard> {
  "use cache";
  // The snapshot changes once a day; ten minutes is how late the site may notice.
  cacheLife({ stale: 300, revalidate: 600, expire: 86400 });

  const latest = await getJson<{ manifest: string }>("latest.json");
  const manifest = await getJson<Manifest>(latest.manifest);
  const mart = (name: string) => {
    const found = manifest.marts.find((m) => m.name === name);
    if (!found) throw new Error(`snapshot ${manifest.run_id} has no ${name}`);
    return found.path;
  };

  const [catalog, real, macro, fx] = await Promise.all([
    getJson<{ series: Series[] }>(manifest.catalog),
    getRows(mart("mart_real_interest"), ["month", "selic_target", "ipca_accum_12m", "real_interest_rate"]),
    getRows(mart("mart_macro_dashboard"), ["month", "debt_to_gdp_pct"]),
    getRows(mart("mart_fx"), ["series_id", "date", "rate"]),
  ]);

  return {
    manifest,
    series: catalog.series,
    baseUrl: baseUrl(),
    macro: {
      selic_target: toLine(real, "month", "selic_target"),
      ipca_12m: toLine(real, "month", "ipca_accum_12m"),
      real_rate: toLine(real, "month", "real_interest_rate"),
      debt_to_gdp: toLine(macro, "month", "debt_to_gdp_pct"),
    },
    usdBrl: toLine(
      fx.filter((row) => row.series_id === "usd_brl"),
      "date",
      "rate",
    ),
  };
}

// How old the newest observation may be before a series counts as late. A monthly figure
// is dated the first of its month and released six weeks or more after the month ends,
// so the newest one is routinely over three months old just before the next release.
const MAX_AGE_DAYS: Record<string, number> = { daily: 7, weekly: 21, monthly: 110, quarterly: 200, annual: 500 };

export type Status = "fresh" | "late" | "marts only" | "withheld" | "no data";

/** Where a series stands in this snapshot: its licence tier first, then how recent it is. */
export function status(series: Series, asOf: string): Status {
  if (series.redistribution === "private") return "withheld";
  if (series.redistribution === "derived") return "marts only";
  if (!series.last_date) return "no data";
  const ageDays = (Date.parse(asOf) - Date.parse(series.last_date)) / 86_400_000;
  return ageDays <= (MAX_AGE_DAYS[series.frequency] ?? 75) ? "fresh" : "late";
}
