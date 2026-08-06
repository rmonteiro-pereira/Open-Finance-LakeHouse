import "server-only";
import { promises as fs } from "node:fs";
import path from "node:path";
import { cache } from "react";

/**
 * The site's only data source: a published release, materialised into public/data by
 * `snapshot/from_release.py`.
 *
 * There is no fallback. A missing file throws and the build fails, on purpose — a silent
 * default renders an empty chart, and an empty chart is indistinguishable from a quiet
 * market. The previous loader had `readJsonOr(name, fallback)`, which is exactly that
 * failure mode with a helpful-sounding name.
 */

const DATA_DIR = path.join(process.cwd(), "public", "data");

const readJson = cache(async <T>(name: string): Promise<T> => {
  const file = path.join(DATA_DIR, `${name}.json`);
  try {
    return JSON.parse(await fs.readFile(file, "utf-8")) as T;
  } catch (cause) {
    throw new Error(
      `${name}.json is missing. The site builds from a published release — run ` +
        `\`python snapshot/from_release.py --release <dir>\` first. No fallback exists by design.`,
      { cause },
    );
  }
});

export type SeriesMeta = {
  series_id: string;
  name: string;
  domain: string;
  unit: string;
  basis: string | null;
  scale: number;
  day_count: string | null;
  horizon: string | null;
  frequency: string;
  provider: string;
  freshness_budget_hours: number;
  last_observation_date: string | null;
};

export type Meta = {
  generated_from: string;
  release_id: string;
  release_class: string;
  generated_at: string;
  series: SeriesMeta[];
  gates: { name: string; status: string; table: string | null; detail: string }[];
  tables: { name: string; status: string; rows: number | null; primary_key: string[]; sha256: string }[];
};

export type Observation = { series_id: string; date: string; value: number };
export type Percentile = {
  series_id: string;
  date: string;
  window_label: string;
  pct_rank: number;
  n_obs: number;
  percentile_allowed: boolean;
};
export type TreasuryRow = {
  instrument_id: string;
  bond: string;
  maturity: string;
  date: string;
  buy_rate: number;
  sell_rate: number;
};

export const getMeta = () => readJson<Meta>("_meta");
export const getObservations = () => readJson<Observation[]>("fact_observation");
export const getPercentiles = () => readJson<Percentile[]>("mart_series_percentile");
export const getTreasury = () => readJson<TreasuryRow[]>("fact_tesouro_direto");

export async function seriesMeta(seriesId: string): Promise<SeriesMeta | undefined> {
  return (await getMeta()).series.find((s) => s.series_id === seriesId);
}

export async function latest(seriesId: string): Promise<Observation | undefined> {
  const rows = (await getObservations()).filter((o) => o.series_id === seriesId);
  return rows.sort((a, b) => (a.date < b.date ? 1 : -1))[0];
}

export type Freshness = { state: "green" | "amber" | "red"; ageHours: number | null; asOf: string | null };

/**
 * The verdict is computed HERE, from inputs the manifest published — never read from it.
 *
 * A manifest that declares its own `sla_state: "ok"` is correct until the moment it stops
 * being republished, which is precisely the moment the reader needs it to say otherwise.
 * Carrying `last_observation_date` + `freshness_budget_hours` instead means a manifest
 * three days old still answers red without anyone touching it.
 */
export function freshness(meta: SeriesMeta | undefined, now = new Date()): Freshness {
  if (!meta?.last_observation_date) return { state: "red", ageHours: null, asOf: null };
  const asOf = new Date(`${meta.last_observation_date}T00:00:00Z`);
  const ageHours = (now.getTime() - asOf.getTime()) / 3_600_000;
  const budget = meta.freshness_budget_hours;
  const state = ageHours <= budget ? "green" : ageHours <= budget * 2 ? "amber" : "red";
  return { state, ageHours, asOf: meta.last_observation_date };
}
