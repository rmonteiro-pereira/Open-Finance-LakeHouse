"use client";

import { useMemo, useState } from "react";
import { ArrowDown, ArrowUp, Search } from "lucide-react";
import { Sparkline } from "@/components/charts/sparkline";
import { fmtNum, fmtSignedPct, signClass } from "@/lib/format";
import { cn } from "@/lib/utils";
import type { EquityUniverseRow } from "@/lib/data";

type SortKey = "name" | "close" | "daily_return_pct" | "vol_21d";

export function EquityUniverseTable({ rows }: { rows: EquityUniverseRow[] }) {
  const [q, setQ] = useState("");
  const [sector, setSector] = useState("all");
  const [sort, setSort] = useState<SortKey>("name");
  const [dir, setDir] = useState<1 | -1>(1);

  const sectors = useMemo(() => {
    const set = new Set(rows.map((r) => r.sector));
    return ["all", ...Array.from(set).sort()];
  }, [rows]);

  const view = useMemo(() => {
    const needle = q.trim().toLowerCase();
    const out = rows.filter((r) => {
      if (sector !== "all" && r.sector !== sector) return false;
      if (!needle) return true;
      return r.name.toLowerCase().includes(needle) || r.symbol.toLowerCase().includes(needle);
    });
    out.sort((a, b) => {
      const av = a[sort];
      const bv = b[sort];
      const cmp = typeof av === "string" ? av.localeCompare(bv as string) : (av as number) - (bv as number);
      return cmp * dir;
    });
    return out;
  }, [rows, q, sector, sort, dir]);

  const toggleSort = (k: SortKey) => {
    if (sort === k) setDir((d) => (d === 1 ? -1 : 1));
    else {
      setSort(k);
      setDir(k === "name" ? 1 : -1);
    }
  };

  const Th = ({ k, children, className }: { k: SortKey; children: React.ReactNode; className?: string }) => (
    <th className={cn("px-3 py-2 font-medium", className)}>
      <button
        onClick={() => toggleSort(k)}
        className={cn(
          "inline-flex items-center gap-1 transition-colors hover:text-foreground",
          sort === k ? "text-foreground" : "text-muted-foreground",
        )}
      >
        {children}
        {sort === k && (dir === 1 ? <ArrowUp className="size-3" /> : <ArrowDown className="size-3" />)}
      </button>
    </th>
  );

  return (
    <div>
      <div className="flex flex-col gap-3 px-1 pb-4 sm:flex-row sm:items-center sm:justify-between">
        <div className="flex flex-wrap gap-1.5">
          {sectors.map((s) => {
            const active = sector === s;
            const n = s === "all" ? rows.length : rows.filter((r) => r.sector === s).length;
            return (
              <button
                key={s}
                onClick={() => setSector(s)}
                className={cn(
                  "inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs font-medium transition-colors",
                  active
                    ? "border-primary/40 bg-primary/15 text-foreground"
                    : "border-border bg-card text-muted-foreground hover:text-foreground",
                )}
              >
                {s === "all" ? "All sectors" : s}
                <span className={cn("tnum text-[10px]", active ? "text-primary" : "text-muted-foreground/70")}>
                  {n}
                </span>
              </button>
            );
          })}
        </div>
        <div className="relative sm:w-56">
          <Search className="pointer-events-none absolute left-3 top-1/2 size-3.5 -translate-y-1/2 text-muted-foreground" />
          <input
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="Search ticker…"
            className="w-full rounded-md border border-border bg-card py-2 pl-9 pr-3 text-sm text-foreground placeholder:text-muted-foreground focus:border-primary/50 focus:outline-none focus:ring-2 focus:ring-ring/30"
          />
        </div>
      </div>

      <div className="max-h-[560px] overflow-auto rounded-md border border-border/60">
        <table className="w-full text-sm">
          <thead className="sticky top-0 z-10 bg-card/95 text-left text-xs backdrop-blur">
            <tr className="border-b border-border">
              <Th k="name" className="pl-4">Instrument</Th>
              <Th k="close" className="text-right [&>button]:ml-auto">Close</Th>
              <Th k="daily_return_pct" className="text-right [&>button]:ml-auto">Day</Th>
              <Th k="vol_21d" className="text-right [&>button]:ml-auto">21d vol</Th>
              <th className="px-3 py-2 text-right font-medium text-muted-foreground">52w range</th>
              <th className="px-3 py-2 pr-4 text-right font-medium text-muted-foreground">Trend</th>
            </tr>
          </thead>
          <tbody>
            {view.map((r) => {
              const rangePos =
                r.high_52w === r.low_52w ? 100 : ((r.close - r.low_52w) / (r.high_52w - r.low_52w)) * 100;
              return (
                <tr key={r.symbol} className="border-b border-border/40 last:border-0 hover:bg-accent/40">
                  <td className="py-2 pl-4">
                    <div className="font-medium text-foreground">{r.name}</div>
                    <div className="font-mono text-[11px] text-muted-foreground">
                      {r.symbol} · {r.sector}
                    </div>
                  </td>
                  <td className="px-3 text-right font-mono tabular-nums">{fmtNum(r.close, 2)}</td>
                  <td className={cn("px-3 text-right font-mono tabular-nums", signClass(r.daily_return_pct))}>
                    {fmtSignedPct(r.daily_return_pct, 2)}
                  </td>
                  <td className="px-3 text-right font-mono tabular-nums text-muted-foreground">
                    {fmtNum(r.vol_21d, 1)}%
                  </td>
                  <td className="px-3">
                    <div className="flex items-center justify-end gap-2">
                      <span className="hidden font-mono text-[10px] tabular-nums text-muted-foreground sm:inline">
                        {fmtNum(r.low_52w, 2)}
                      </span>
                      <div className="relative h-1.5 w-16 overflow-hidden rounded-full bg-border">
                        <div
                          className="absolute top-1/2 size-2 -translate-y-1/2 rounded-full bg-primary ring-2 ring-card"
                          style={{ left: `calc(${Math.max(0, Math.min(100, rangePos))}% - 4px)` }}
                        />
                      </div>
                      <span className="hidden font-mono text-[10px] tabular-nums text-muted-foreground sm:inline">
                        {fmtNum(r.high_52w, 2)}
                      </span>
                    </div>
                  </td>
                  <td className="py-1.5 pr-4">
                    <div className="ml-auto h-7 w-20">
                      <Sparkline
                        data={r.spark}
                        color={r.daily_return_pct >= 0 ? "var(--positive)" : "var(--negative)"}
                      />
                    </div>
                  </td>
                </tr>
              );
            })}
            {view.length === 0 && (
              <tr>
                <td colSpan={6} className="py-12 text-center text-sm text-muted-foreground">
                  No tickers match “{q}”.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      <p className="px-1 pt-3 text-xs text-muted-foreground">
        {view.length} of {rows.length} listed names · source{" "}
        <span className="font-mono text-foreground/70">gold/mart_equity_universe</span> (b3_cotahist).
      </p>
    </div>
  );
}
