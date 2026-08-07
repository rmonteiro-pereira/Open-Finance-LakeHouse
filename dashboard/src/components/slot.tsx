import { freshness, STATE_LABEL, type SeriesMeta } from "@/lib/release";

/**
 * A figure, its unit and its as-of date, as one indivisible thing.
 *
 * Every number on this site renders through here, which is what makes "no figure without
 * an as-of date" a property a test can check rather than a habit a page can forget. The
 * `data-slot` / `data-series-id` marks exist for the same reason: a marked slot is
 * assertable, a free-form sentence is a fluent way to be wrong.
 */

/** Only `ok` and `late` carry colour. The rest are facts, and facts do not blink. */
const DOT: Partial<Record<string, string>> = {
  ok: "var(--ok)",
  late: "var(--late)",
};

export function FreshnessChip({ meta }: { meta: SeriesMeta | undefined }) {
  const f = freshness(meta);
  const colour = DOT[f.state];

  return (
    <span
      data-slot="freshness"
      data-state={f.state}
      className="inline-flex items-baseline gap-1.5 text-xs text-[var(--ink-muted)]"
      title={
        f.asOf
          ? `dado de ${f.asOf}; orçamento de ${meta?.freshness_budget_hours}h`
          : STATE_LABEL[f.state]
      }
    >
      {colour ? (
        <span
          aria-hidden
          className="inline-block h-1 w-1 translate-y-[-1px] rounded-full"
          style={{ background: colour }}
        />
      ) : null}
      <span className={colour ? "tnum" : undefined}>{f.asOf ?? STATE_LABEL[f.state]}</span>
    </span>
  );
}

export function Stat({
  label,
  value,
  meta,
  suffix,
  note,
  size = "lg",
}: {
  label: string;
  value: number | string | null | undefined;
  meta: SeriesMeta | undefined;
  suffix?: string;
  note?: string;
  size?: "lg" | "sm";
}) {
  const shown =
    value === null || value === undefined
      ? "—"
      : typeof value === "number"
        ? value.toLocaleString("pt-BR", { maximumFractionDigits: 2 })
        : value;

  return (
    <div data-slot="stat" data-series-id={meta?.series_id ?? "unknown"} className="space-y-1.5">
      <div className="text-xs text-[var(--ink-muted)]">{label}</div>
      <div className="flex items-baseline gap-1.5">
        {/*
          Proportional figures, NOT tabular. `tabular-nums` gives every digit the width of
          a zero, which makes a standalone display number read loose; tabular belongs in
          columns that align vertically (table rows, axis ticks).
        */}
        <span
          data-slot="value"
          className={`font-[family-name:var(--font-mono)] tracking-tight ${size === "lg" ? "text-4xl" : "text-2xl"}`}
        >
          {shown}
        </span>
        {/*
          The unit comes from the series' declared tuple, never typed inline. A hardcoded
          "% a.a." beside a per-day rate is the defect this whole layer exists to prevent.
        */}
        <span data-slot="unit" className="text-sm text-[var(--ink-muted)]">
          {suffix ?? unitLabel(meta)}
        </span>
      </div>
      <FreshnessChip meta={meta} />
      {note ? <div className="text-xs text-[var(--ink-muted)]">{note}</div> : null}
    </div>
  );
}

export function unitLabel(meta: SeriesMeta | undefined): string {
  if (!meta) return "";
  const base =
    meta.unit === "percent" ? "%" : meta.unit === "brl" ? "R$" : meta.unit === "usd" ? "US$" : "";
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
 * Keyed on `unit` alone this would pass the SELIC daily rate against monthly IPCA, since
 * both are `percent` — the exact confusion the tuple was introduced to end.
 */
export function assertComparable(series: (SeriesMeta | undefined)[]): void {
  const tuples = new Set(series.filter(Boolean).map((s) => `${s!.unit}|${s!.basis}|${s!.scale}`));
  if (tuples.size > 1) {
    throw new Error(
      `refusing to plot incomparable quantities on one axis: ${[...tuples].join(" vs ")}`,
    );
  }
}
