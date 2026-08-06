"use client";

import { useMemo, useState } from "react";
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { fmtDate, fmtNum } from "@/lib/format";
import { cn } from "@/lib/utils";
import type { FuturesCurveRow } from "@/lib/data";

const ASSET_LABEL: Record<string, string> = {
  DI1: "DI × pré (nominal)",
  DAP: "DAP — DI × IPCA (real)",
  DDI: "DDI — cupom cambial",
};

export function FuturesCurveChart({ rows, assets }: { rows: FuturesCurveRow[]; assets: string[] }) {
  const [asset, setAsset] = useState(assets[0]);

  const { data, curDate, ghostDate } = useMemo(() => {
    const ser = rows.filter((r) => r.asset === asset);
    const dates = [...new Set(ser.map((r) => r.date))].sort();
    const cur = dates[dates.length - 1];
    // ghost ≈ a snapshot ~3 months back (or the earliest we have)
    const ghost = dates[Math.max(0, dates.length - 4)] !== cur ? dates[Math.max(0, dates.length - 4)] : undefined;
    const merged = new Map<number, { dtm: number; current?: number; ghost?: number }>();
    const put = (key: "current" | "ghost", date?: string) => {
      if (!date) return;
      for (const r of ser) {
        if (r.date !== date) continue;
        const dtm = Math.round(r.days_to_maturity);
        const e = merged.get(dtm) ?? { dtm };
        e[key] = r.settlement_rate;
        merged.set(dtm, e);
      }
    };
    put("current", cur);
    put("ghost", ghost);
    const out = [...merged.values()].sort((a, b) => a.dtm - b.dtm);
    return { data: out, curDate: cur, ghostDate: ghost };
  }, [rows, asset]);

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
        <span className="text-xs text-muted-foreground">{ASSET_LABEL[asset] ?? asset}</span>
      </div>

      <ResponsiveContainer width="100%" height={360}>
        <LineChart data={data} margin={{ top: 8, right: 16, bottom: 16, left: 4 }}>
          <CartesianGrid stroke="var(--border)" strokeDasharray="2 4" vertical={false} />
          <XAxis
            dataKey="dtm"
            type="number"
            domain={["dataMin", "dataMax"]}
            tick={{ fill: "var(--muted-foreground)", fontSize: 11 }}
            tickLine={false}
            axisLine={{ stroke: "var(--border)" }}
            tickFormatter={(v) => `${(Number(v) / 365).toFixed(1)}y`}
            label={{ value: "maturity (years)", position: "insideBottom", offset: -8, fontSize: 11, fill: "var(--muted-foreground)" }}
          />
          <YAxis
            tick={{ fill: "var(--muted-foreground)", fontSize: 11 }}
            tickLine={false}
            axisLine={false}
            width={50}
            domain={["auto", "auto"]}
            tickFormatter={(v) => `${fmtNum(Number(v), 1)}%`}
          />
          <Tooltip
            cursor={{ stroke: "var(--muted-foreground)", strokeDasharray: "3 3" }}
            content={({ active, payload }) => {
              if (!active || !payload?.length) return null;
              const p = payload[0].payload as { dtm: number; current?: number; ghost?: number };
              return (
                <div className="min-w-[160px] rounded-lg border border-border bg-popover/95 px-3 py-2 text-xs shadow-xl backdrop-blur">
                  <div className="mb-1 font-medium text-foreground">{(p.dtm / 365).toFixed(2)} years</div>
                  {p.current != null && <Row k="Current" v={`${fmtNum(p.current, 3)}%`} />}
                  {p.ghost != null && <Row k="Prior" v={`${fmtNum(p.ghost, 3)}%`} />}
                </div>
              );
            }}
          />
          {ghostDate && (
            <Line
              type="monotone"
              dataKey="ghost"
              stroke="var(--muted-foreground)"
              strokeWidth={1.5}
              strokeDasharray="4 3"
              strokeOpacity={0.5}
              dot={false}
              connectNulls
              isAnimationActive={false}
              name="Prior"
            />
          )}
          <Line
            type="monotone"
            dataKey="current"
            stroke="var(--chart-1)"
            strokeWidth={2.25}
            dot={{ r: 2, strokeWidth: 0, fill: "var(--chart-1)" }}
            connectNulls
            animationDuration={500}
            name="Current"
          />
        </LineChart>
      </ResponsiveContainer>

      <div className="mt-3 flex flex-wrap items-center justify-center gap-4 border-t border-border/60 pt-3 text-xs text-muted-foreground">
        <span className="inline-flex items-center gap-1.5">
          <span className="h-0.5 w-4 rounded bg-chart-1" /> Current{curDate ? ` · ${fmtDate(curDate)}` : ""}
        </span>
        {ghostDate && (
          <span className="inline-flex items-center gap-1.5">
            <span className="h-0.5 w-4 rounded bg-muted-foreground/50" /> Prior · {fmtDate(ghostDate)}
          </span>
        )}
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
