"use client";

import Link from "next/link";
import { useMemo, useState } from "react";
import { ArrowUpRight, Search } from "lucide-react";
import { Sparkline } from "@/components/charts/sparkline";
import { fmtCompact, fmtNum } from "@/lib/format";
import { cn } from "@/lib/utils";

export type CatalogCard = {
  series_id: string;
  name: string;
  domain: string;
  source: string;
  category: string;
  unit: string;
  frequency: string;
  fact: string;
  spark: number[];
  latest: number | null;
  changePct: number | null;
  href?: string;
};

const DOMAIN_LABEL: Record<string, string> = {
  rates: "Rates",
  inflation: "Inflation",
  fx: "FX",
  fiscal: "Fiscal",
  credit: "Credit",
  market: "Market",
  equities: "Equities",
};
// Preferred ordering for the domains we know; any *new* domain the lakehouse
// grows is appended automatically (sorted), so the catalog never needs editing.
const KNOWN_ORDER = ["rates", "inflation", "fx", "fiscal", "credit", "market", "equities"];

const labelOf = (d: string) => DOMAIN_LABEL[d] ?? d.charAt(0).toUpperCase() + d.slice(1);

/** known domains first (in our order), then any unknown ones alphabetically. */
function orderDomains(present: string[]): string[] {
  const set = new Set(present);
  const known = KNOWN_ORDER.filter((d) => set.has(d));
  const extra = present.filter((d) => !KNOWN_ORDER.includes(d)).sort();
  return [...known, ...extra];
}

/** unit-aware value formatting for the catalog tiles. */
function fmtValue(unit: string, v: number | null): string {
  if (v == null) return "—";
  const u = unit.toLowerCase();
  if (u.includes("mn")) return fmtCompact(v);
  if (u === "index" || u === "points") return fmtNum(v, 0);
  if (u === "brl") return fmtNum(v, 4);
  if (u.startsWith("%")) return `${fmtNum(v, 2)}%`;
  return fmtNum(v, 2);
}

export function CatalogExplorer({ cards }: { cards: CatalogCard[] }) {
  const [q, setQ] = useState("");
  const [domain, setDomain] = useState<string>("all");

  const counts = useMemo(() => {
    const c: Record<string, number> = {};
    for (const card of cards) c[card.domain] = (c[card.domain] ?? 0) + 1;
    return c;
  }, [cards]);

  const filtered = useMemo(() => {
    const needle = q.trim().toLowerCase();
    return cards.filter((c) => {
      if (domain !== "all" && c.domain !== domain) return false;
      if (!needle) return true;
      return (
        c.name.toLowerCase().includes(needle) ||
        c.series_id.toLowerCase().includes(needle) ||
        c.source.toLowerCase().includes(needle)
      );
    });
  }, [cards, q, domain]);

  const groups = useMemo(() => {
    const g = new Map<string, CatalogCard[]>();
    for (const c of filtered) {
      const arr = g.get(c.domain);
      if (arr) arr.push(c);
      else g.set(c.domain, [c]);
    }
    return orderDomains([...g.keys()]).map((d) => [d, g.get(d)!] as const);
  }, [filtered]);

  const domains = useMemo(() => ["all", ...orderDomains(Object.keys(counts))], [counts]);

  return (
    <div className="rise" style={{ animationDelay: "80ms" }}>
      {/* controls */}
      <div className="mb-6 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div className="flex flex-wrap gap-1.5">
          {domains.map((d) => {
            const active = domain === d;
            const n = d === "all" ? cards.length : counts[d];
            return (
              <button
                key={d}
                onClick={() => setDomain(d)}
                className={cn(
                  "inline-flex items-center gap-1.5 rounded-full border px-3 py-1.5 text-xs font-medium transition-colors",
                  active
                    ? "border-primary/40 bg-primary/15 text-foreground"
                    : "border-border bg-card text-muted-foreground hover:border-border/80 hover:text-foreground",
                )}
              >
                {d === "all" ? "All" : labelOf(d)}
                <span className={cn("tnum text-[10px]", active ? "text-primary" : "text-muted-foreground/70")}>
                  {n}
                </span>
              </button>
            );
          })}
        </div>
        <div className="relative sm:w-64">
          <Search className="pointer-events-none absolute left-3 top-1/2 size-3.5 -translate-y-1/2 text-muted-foreground" />
          <input
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="Search series…"
            className="w-full rounded-md border border-border bg-card py-2 pl-9 pr-3 text-sm text-foreground placeholder:text-muted-foreground focus:border-primary/50 focus:outline-none focus:ring-2 focus:ring-ring/30"
          />
        </div>
      </div>

      {groups.length === 0 && (
        <p className="py-16 text-center text-sm text-muted-foreground">No series match “{q}”.</p>
      )}

      <div className="flex flex-col gap-8">
        {groups.map(([d, items]) => (
          <section key={d}>
            <div className="mb-3 flex items-baseline gap-2">
              <h2 className="text-sm font-semibold tracking-tight text-foreground">{labelOf(d)}</h2>
              <span className="tnum text-xs text-muted-foreground">{items.length}</span>
            </div>
            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
              {items.map((c) => (
                <Card key={c.series_id} card={c} />
              ))}
            </div>
          </section>
        ))}
      </div>
    </div>
  );
}

function Card({ card }: { card: CatalogCard }) {
  const up = card.changePct != null && card.changePct > 0;
  const down = card.changePct != null && card.changePct < 0;
  return (
    <div className="group relative flex flex-col rounded-lg border border-border bg-card p-4 transition-colors hover:border-border/60">
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <p className="truncate text-sm font-medium leading-tight text-foreground" title={card.name}>
            {card.name}
          </p>
          <p className="mt-0.5 font-mono text-[11px] text-muted-foreground">{card.series_id}</p>
        </div>
        {card.changePct != null && (
          <span
            className={cn(
              "shrink-0 rounded-full px-1.5 py-0.5 text-[10px] font-medium tnum",
              up && "bg-positive/10 text-positive",
              down && "bg-negative/10 text-negative",
              !up && !down && "bg-muted text-muted-foreground",
            )}
          >
            {up ? "+" : ""}
            {fmtNum(card.changePct, 1)}%
          </span>
        )}
      </div>

      <div className="mt-2 flex items-end justify-between gap-3">
        <div className="flex items-baseline gap-1">
          <span className="font-semibold tabular-nums tracking-tight tnum text-foreground">
            {fmtValue(card.unit, card.latest)}
          </span>
          {card.latest != null && card.unit && !card.unit.startsWith("%") && (
            <span className="text-[11px] text-muted-foreground">{card.unit}</span>
          )}
        </div>
        {card.spark.length > 1 ? (
          <div className="h-9 w-24 shrink-0">
            <Sparkline data={card.spark} color="var(--chart-1)" />
          </div>
        ) : card.href ? (
          <Link
            href={card.href}
            className="inline-flex shrink-0 items-center gap-0.5 text-[11px] font-medium text-primary hover:underline"
          >
            view <ArrowUpRight className="size-3" />
          </Link>
        ) : null}
      </div>

      <div className="mt-3 flex flex-wrap items-center gap-1.5 border-t border-border/60 pt-3 text-[10px] text-muted-foreground">
        <span className="rounded bg-muted px-1.5 py-0.5">{card.frequency}</span>
        <span className="rounded bg-muted px-1.5 py-0.5">{card.source}</span>
        <span className="ml-auto font-mono text-muted-foreground/70">{card.fact.replace("fact_", "")}</span>
      </div>
    </div>
  );
}
