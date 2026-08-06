import { ArrowDownRight, ArrowUpRight, Minus } from "lucide-react";
import { cn } from "@/lib/utils";
import { Sparkline } from "@/components/charts/sparkline";

export function StatCard({
  label,
  value,
  unit,
  delta,
  deltaLabel,
  series,
  accent = "var(--chart-1)",
  invertDelta = false,
  featured = false,
  style,
}: {
  label: string;
  value: string;
  unit?: string;
  delta?: number;
  deltaLabel?: string;
  series?: number[];
  accent?: string;
  /** when true, a negative delta is "good" (e.g. falling inflation) */
  invertDelta?: boolean;
  /** the page's lead metric — larger, brighter, the first thing the eye lands on */
  featured?: boolean;
  style?: React.CSSProperties;
}) {
  const dir = delta == null || delta === 0 ? 0 : delta > 0 ? 1 : -1;
  const good = invertDelta ? dir < 0 : dir > 0;
  const tone = dir === 0 ? "muted" : good ? "positive" : "negative";
  const DeltaIcon = dir === 0 ? Minus : dir > 0 ? ArrowUpRight : ArrowDownRight;

  return (
    <div
      style={style}
      className={cn(
        "rise group relative overflow-hidden rounded-xl border bg-card p-4 transition-colors",
        featured ? "border-primary/40 ring-1 ring-primary/15" : "border-border hover:border-border/70",
      )}
    >
      <div className="flex items-start justify-between gap-2">
        <p className="text-[11px] font-medium uppercase tracking-[0.16em] text-muted-foreground">
          {label}
        </p>
        {delta != null && (
          <span
            className={cn(
              "inline-flex items-center gap-0.5 rounded-full px-1.5 py-0.5 text-[11px] font-medium tnum",
              tone === "positive" && "bg-positive/10 text-positive",
              tone === "negative" && "bg-negative/10 text-negative",
              tone === "muted" && "bg-muted text-muted-foreground",
            )}
          >
            <DeltaIcon className="size-3" />
            {deltaLabel}
          </span>
        )}
      </div>

      <div className="mt-2 flex items-end justify-between gap-3">
        <div className="flex items-baseline gap-1">
          <span
            className={cn(
              "font-display font-semibold leading-none tracking-tight tnum",
              featured ? "text-[2.2rem] text-foreground" : "text-[1.65rem] text-foreground/95",
            )}
          >
            {value}
          </span>
          {unit && <span className="pb-0.5 text-xs text-muted-foreground">{unit}</span>}
        </div>
        {series && series.length > 1 && (
          <div className={cn("shrink-0", featured ? "h-10 w-28" : "h-9 w-24 opacity-90")}>
            <Sparkline data={series} color={accent} />
          </div>
        )}
      </div>
    </div>
  );
}
