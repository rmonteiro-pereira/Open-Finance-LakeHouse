"use client";

import { useMemo, useState } from "react";
import {
  Area,
  CartesianGrid,
  ComposedChart,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { fmtDate, fmtNum } from "@/lib/format";
import { cn } from "@/lib/utils";
import type { EquityRow } from "@/lib/data";

const LABELS: Record<string, string> = {
  "^BVSP": "Ibovespa",
  "^GSPC": "S&P 500",
  "^IXIC": "Nasdaq",
  PETR4: "Petrobras",
  VALE3: "Vale",
  ITUB4: "Itaú",
  BBDC4: "Bradesco",
  BBAS3: "Banco do Brasil",
  B3SA3: "B3",
  ABEV3: "Ambev",
  WEGE3: "WEG",
  MGLU3: "Magazine Luiza",
  PRIO3: "PetroRio",
  RENT3: "Localiza",
};

export function EquitiesChart({ rows, symbols }: { rows: EquityRow[]; symbols: string[] }) {
  const [symbol, setSymbol] = useState(symbols[0]);
  const series = useMemo(
    () => rows.filter((r) => r.symbol === symbol).sort((a, b) => a.date.localeCompare(b.date)),
    [rows, symbol],
  );

  return (
    <div>
      <div className="flex flex-wrap gap-1.5 px-1 pb-4">
        {symbols.map((s) => (
          <button
            key={s}
            onClick={() => setSymbol(s)}
            className={cn(
              "rounded-md border px-2.5 py-1 text-xs font-medium transition-colors",
              s === symbol
                ? "border-primary/40 bg-primary/15 text-primary"
                : "border-border bg-secondary/60 text-muted-foreground hover:bg-accent hover:text-foreground",
            )}
          >
            {LABELS[s] ?? s}
          </button>
        ))}
      </div>

      <ResponsiveContainer width="100%" height={360}>
        <ComposedChart data={series} margin={{ top: 8, right: 14, bottom: 4, left: 4 }}>
          <defs>
            <linearGradient id="eq-band" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="var(--chart-2)" stopOpacity={0.12} />
              <stop offset="100%" stopColor="var(--chart-2)" stopOpacity={0.02} />
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
            width={56}
            domain={["auto", "auto"]}
            tickFormatter={(v) => fmtNum(Number(v), 0)}
          />
          <Tooltip
            cursor={{ stroke: "var(--muted-foreground)", strokeDasharray: "3 3" }}
            content={({ active, payload, label }) => {
              if (!active || !payload?.length) return null;
              const p = payload[0].payload as EquityRow;
              return (
                <div className="min-w-[180px] rounded-lg border border-border bg-popover/95 px-3 py-2 text-xs shadow-xl backdrop-blur">
                  <div className="mb-1 font-medium text-foreground">{fmtDate(String(label))}</div>
                  <Row k="Close" v={fmtNum(p.close, 2)} />
                  <Row k="SMA-21" v={fmtNum(p.sma_21, 2)} />
                  <Row k="52w high" v={fmtNum(p.high_52w, 2)} />
                  <Row k="52w low" v={fmtNum(p.low_52w, 2)} />
                  <Row k="21d vol" v={`${fmtNum(p.vol_21d, 1)}%`} />
                </div>
              );
            }}
          />
          {/* 52-week range band */}
          <Area
            dataKey={(d: EquityRow) => [d.low_52w, d.high_52w]}
            stroke="none"
            fill="url(#eq-band)"
            isAnimationActive={false}
            activeDot={false}
            legendType="none"
            name="52w range"
          />
          <Line
            type="monotone"
            dataKey="sma_21"
            stroke="var(--chart-4)"
            strokeWidth={1.25}
            strokeDasharray="4 3"
            dot={false}
            isAnimationActive={false}
            name="SMA-21"
          />
          <Line
            type="monotone"
            dataKey="close"
            stroke="var(--chart-1)"
            strokeWidth={2}
            dot={false}
            activeDot={{ r: 3, strokeWidth: 0 }}
            animationDuration={600}
            name="Close"
          />
        </ComposedChart>
      </ResponsiveContainer>

      <div className="mt-3 flex flex-wrap items-center justify-center gap-4 border-t border-border/60 pt-3 text-xs text-muted-foreground">
        <span className="inline-flex items-center gap-1.5">
          <span className="h-0.5 w-4 rounded bg-chart-1" /> Close
        </span>
        <span className="inline-flex items-center gap-1.5">
          <span className="h-0.5 w-4 rounded bg-chart-4" /> SMA-21
        </span>
        <span className="inline-flex items-center gap-1.5">
          <span className="size-2.5 rounded-sm bg-chart-2/30" /> 52-week range
        </span>
      </div>
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
