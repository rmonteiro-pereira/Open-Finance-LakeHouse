import { freshness, type SeriesMeta } from "@/lib/release";

/**
 * A number, its unit, its as-of date and its freshness — as one indivisible thing.
 *
 * Every figure on this site renders through here, which is what makes "no number without
 * a freshness chip" a property a test can check rather than a habit. The `data-slot` and
 * `data-series-id` attributes exist for the same reason: a marked slot is assertable, a
 * free-form sentence is not.
 */

const DOT: Record<string, string> = {
  green: "bg-emerald-500",
  amber: "bg-amber-500",
  red: "bg-rose-500",
};

export function FreshnessChip({ meta }: { meta: SeriesMeta | undefined }) {
  const f = freshness(meta);
  return (
    <span
      data-slot="freshness"
      data-state={f.state}
      className="inline-flex items-center gap-1.5 text-xs text-muted-foreground"
      title={
        f.asOf
          ? `dado de ${f.asOf}; orçamento de ${meta?.freshness_budget_hours}h`
          : "sem observação publicada para esta série"
      }
    >
      <span className={`h-1.5 w-1.5 rounded-full ${DOT[f.state]}`} aria-hidden />
      {f.asOf ?? "sem dado"}
    </span>
  );
}

export function Stat({
  label,
  value,
  meta,
  suffix,
  note,
}: {
  label: string;
  value: number | string | null | undefined;
  meta: SeriesMeta | undefined;
  suffix?: string;
  note?: string;
}) {
  const shown =
    value === null || value === undefined
      ? "—"
      : typeof value === "number"
        ? value.toLocaleString("pt-BR", { maximumFractionDigits: 2 })
        : value;

  return (
    <div data-slot="stat" data-series-id={meta?.series_id ?? "unknown"} className="space-y-1">
      <div className="text-xs uppercase tracking-wide text-muted-foreground">{label}</div>
      <div className="flex items-baseline gap-1.5">
        <span data-slot="value" className="font-mono text-3xl tabular-nums">
          {shown}
        </span>
        {/* The unit is rendered from the series' declared tuple, never typed inline: a
            hardcoded "% a.a." beside a per-day rate is the defect this whole layer
            exists to make impossible. */}
        <span data-slot="unit" className="text-sm text-muted-foreground">
          {suffix ?? unitLabel(meta)}
        </span>
      </div>
      <FreshnessChip meta={meta} />
      {note ? <div className="text-xs text-muted-foreground">{note}</div> : null}
    </div>
  );
}

export function unitLabel(meta: SeriesMeta | undefined): string {
  if (!meta) return "";
  const base = meta.unit === "percent" ? "%" : meta.unit === "brl" ? "R$" : meta.unit === "usd" ? "US$" : "";
  const per =
    meta.basis === "per_year"
      ? " a.a."
      : meta.basis === "per_month"
        ? " a.m."
        : meta.basis === "per_day"
          ? " a.d."
          : meta.basis === "mom"
            ? " m/m"
            : meta.basis === "yoy"
              ? " a/a"
              : meta.basis === "pct_of_gdp"
                ? " do PIB"
                : "";
  const scale = meta.scale && meta.scale !== 1 ? ` ×${meta.scale.toLocaleString("pt-BR")}` : "";
  return `${base}${per}${scale}`;
}

/**
 * Guard: two series may share an axis only if their whole quantity tuple matches.
 *
 * Keyed on `unit` alone this would pass for the SELIC daily rate against monthly IPCA —
 * both are `percent` — which is the exact confusion the tuple was introduced to end.
 */
export function assertComparable(series: (SeriesMeta | undefined)[]): void {
  const tuples = new Set(
    series.filter(Boolean).map((s) => `${s!.unit}|${s!.basis}|${s!.scale}`),
  );
  if (tuples.size > 1) {
    throw new Error(
      `refusing to plot incomparable quantities on one axis: ${[...tuples].join(" vs ")}`,
    );
  }
}
