"use client";

import { useMemo, useState } from "react";
import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { fmtCompact, fmtDate, fmtSigned } from "@/lib/format";
import { cn } from "@/lib/utils";
import type { OpenInterestRow } from "@/lib/data";

const ASSET_LABEL: Record<string, string> = {
  DI1: "DI futures",
  DOL: "US dollar (full)",
  WDO: "Mini dollar",
  IND: "Ibovespa (full)",
  WIN: "Mini Ibovespa",
  BGI: "Live cattle",
  CCM: "Corn",
  ETH: "Ethanol",
  ICF: "Coffee",
  DAP: "DI × IPCA",
};

export function OpenInterestChart({ rows, assets }: { rows: OpenInterestRow[]; assets: string[] }) {
  const [asset, setAsset] = useState(assets[0]);

  const series = useMemo(
    () => rows.filter((r) => r.asset === asset).sort((a, b) => a.date.localeCompare(b.date)),
    [rows, asset],
  );
  const last = series[series.length - 1];

  return (
    <div>
      <div className="flex flex-wrap items-center justify-between gap-3 px-1 pb-4">
        <div className="flex flex-wrap gap-1.5">
          {assets.map((a) => (
            <button
              key={a}
              onClick={() => setAsset(a)}
              className={cn(
                "rounded-md border px-2.5 py-1 text-xs font-medium transition-colors",
                a === asset
                  ? "border-primary/40 bg-primary/15 text-primary"
                  : "border-border bg-secondary/60 text-muted-foreground hover:bg-accent hover:text-foreground",
              )}
            >
              {a}
            </button>
          ))}
        </div>
        {last && (
          <div className="text-right">
            <div className="font-mono text-sm font-semibold tabular-nums text-foreground">
              {fmtCompact(last.total_open_interest)}
              <span className="ml-1 text-[11px] font-normal text-muted-foreground">contracts</span>
            </div>
            <div className="text-[11px] text-muted-foreground">
              {ASSET_LABEL[asset] ?? asset}
              {last.total_open_interest_var != null && (
                <span className={cn("ml-1 tnum", last.total_open_interest_var >= 0 ? "text-positive" : "text-negative")}>
                  {fmtSigned(last.total_open_interest_var, 0)}
                </span>
              )}
            </div>
          </div>
        )}
      </div>

      <ResponsiveContainer width="100%" height={300}>
        <AreaChart data={series} margin={{ top: 8, right: 14, bottom: 4, left: 6 }}>
          <defs>
            <linearGradient id="oi-fill" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="var(--chart-1)" stopOpacity={0.3} />
              <stop offset="100%" stopColor="var(--chart-1)" stopOpacity={0.02} />
            </linearGradient>
          </defs>
          <CartesianGrid stroke="var(--border)" strokeDasharray="2 4" vertical={false} />
          <XAxis
            dataKey="date"
            tick={{ fill: "var(--muted-foreground)", fontSize: 11 }}
            tickLine={false}
            axisLine={{ stroke: "var(--border)" }}
            tickFormatter={(v) => fmtDate(String(v))}
            minTickGap={48}
          />
          <YAxis
            tick={{ fill: "var(--muted-foreground)", fontSize: 11 }}
            tickLine={false}
            axisLine={false}
            width={48}
            tickFormatter={(v) => fmtCompact(Number(v))}
          />
          <Tooltip
            cursor={{ stroke: "var(--muted-foreground)", strokeDasharray: "3 3" }}
            content={({ active, payload, label }) => {
              if (!active || !payload?.length) return null;
              const p = payload[0].payload as OpenInterestRow;
              return (
                <div className="min-w-[170px] rounded-lg border border-border bg-popover/95 px-3 py-2 text-xs shadow-xl backdrop-blur">
                  <div className="mb-1 font-medium text-foreground">{fmtDate(String(label))}</div>
                  <Row k="Open interest" v={fmtCompact(p.total_open_interest)} />
                  {p.n_contracts != null && <Row k="Live maturities" v={String(p.n_contracts)} />}
                  {p.total_open_interest_var != null && <Row k="Δ vs prev" v={fmtSigned(p.total_open_interest_var, 0)} />}
                </div>
              );
            }}
          />
          <Area
            type="monotone"
            dataKey="total_open_interest"
            stroke="var(--chart-1)"
            strokeWidth={2}
            fill="url(#oi-fill)"
            dot={false}
            animationDuration={500}
            name="Open interest"
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}

function Row({ k, v }: { k: string; v: string }) {
  return (
    <div className="flex items-center justify-between gap-4">
      <span className="text-muted-foreground">{k}</span>
      <span className="font-mono tabular-nums text-foreground">{v}</span>
    </div>
  );
}
