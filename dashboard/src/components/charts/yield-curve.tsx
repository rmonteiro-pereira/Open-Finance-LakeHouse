"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis,
} from "recharts";
import { Pause, Play } from "lucide-react";
import { fmtMonth, fmtMonthShort, fmtNum } from "@/lib/format";
import type { YieldRow } from "@/lib/data";

const TYPES = [
  { key: "ipca_plus", label: "IPCA+", color: "var(--chart-1)" },
  { key: "prefixado", label: "Prefixado", color: "var(--chart-2)" },
  { key: "selic", label: "Selic / floater", color: "var(--chart-5)" },
] as const;

export function YieldCurveChart({ rows }: { rows: YieldRow[] }) {
  const dates = useMemo(
    () => [...new Set(rows.map((r) => r.date))].sort(),
    [rows],
  );
  const [idx, setIdx] = useState(dates.length - 1);
  const [playing, setPlaying] = useState(false);
  const timer = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    if (!playing) return;
    timer.current = setInterval(() => {
      setIdx((i) => {
        if (i >= dates.length - 1) return 0;
        return i + 1;
      });
    }, 700);
    return () => {
      if (timer.current) clearInterval(timer.current);
    };
  }, [playing, dates.length]);

  const date = dates[idx];
  const curve = useMemo(() => rows.filter((r) => r.date === date), [rows, date]);

  const byType = useMemo(
    () =>
      TYPES.map((t) => ({
        ...t,
        points: curve
          .filter((r) => r.bond_type === t.key)
          .sort((a, b) => a.years_to_maturity - b.years_to_maturity)
          .map((r) => ({ x: r.years_to_maturity, y: r.yield, bond: r.bond })),
      })),
    [curve],
  );

  // history: average yield per type over time (the term-structure drift)
  const history = useMemo(() => {
    return dates.map((d) => {
      const dayRows = rows.filter((r) => r.date === d);
      const row: Record<string, number | string> = { date: d };
      for (const t of TYPES) {
        const ys = dayRows.filter((r) => r.bond_type === t.key).map((r) => r.yield);
        if (ys.length) row[t.key] = +(ys.reduce((s, v) => s + v, 0) / ys.length).toFixed(2);
      }
      return row;
    });
  }, [rows, dates]);

  // previous snapshot, drawn as a faded "ghost" so the shift is legible
  const prevDate = idx > 0 ? dates[idx - 1] : null;
  const prevByType = useMemo(() => {
    if (!prevDate) return [];
    const prev = rows.filter((r) => r.date === prevDate);
    return TYPES.map((t) => ({
      ...t,
      points: prev
        .filter((r) => r.bond_type === t.key)
        .sort((a, b) => a.years_to_maturity - b.years_to_maturity)
        .map((r) => ({ x: r.years_to_maturity, y: r.yield, bond: r.bond })),
    }));
  }, [rows, prevDate]);

  const yields = curve.map((r) => r.yield);
  const yMin = Math.floor(Math.min(...yields) - 1);
  const yMax = Math.ceil(Math.max(...yields) + 1);

  return (
    <div>
      {/* control bar */}
      <div className="flex flex-wrap items-center gap-3 px-1 pb-4">
        <button
          onClick={() => setPlaying((p) => !p)}
          className="inline-flex items-center gap-1.5 rounded-md border border-border bg-secondary px-3 py-1.5 text-xs font-medium text-foreground transition-colors hover:bg-accent"
        >
          {playing ? <Pause className="size-3.5" /> : <Play className="size-3.5" />}
          {playing ? "Pause" : "Play"}
        </button>
        <div className="flex min-w-0 flex-1 items-center gap-3">
          <input
            type="range"
            min={0}
            max={dates.length - 1}
            value={idx}
            onChange={(e) => {
              setPlaying(false);
              setIdx(Number(e.target.value));
            }}
            className="h-1 w-full cursor-pointer appearance-none rounded-full bg-border accent-primary"
            aria-label="Snapshot date"
          />
          <span className="w-24 shrink-0 text-right font-mono text-xs tabular-nums text-foreground">
            {fmtMonth(date)}
          </span>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-2 lg:grid-cols-5">
        {/* the curve */}
        <div className="lg:col-span-3">
          <ResponsiveContainer width="100%" height={380}>
            <ScatterChart margin={{ top: 10, right: 16, bottom: 18, left: 4 }}>
              <CartesianGrid stroke="var(--border)" strokeDasharray="2 4" />
              <XAxis
                type="number"
                dataKey="x"
                name="Years to maturity"
                domain={[0, "dataMax"]}
                tick={{ fill: "var(--muted-foreground)", fontSize: 11 }}
                tickLine={false}
                axisLine={{ stroke: "var(--border)" }}
                tickFormatter={(v) => `${v}y`}
                label={{ value: "years to maturity", position: "insideBottom", offset: -8, fill: "var(--muted-foreground)", fontSize: 10 }}
              />
              <YAxis
                type="number"
                dataKey="y"
                domain={[yMin, yMax]}
                tick={{ fill: "var(--muted-foreground)", fontSize: 11 }}
                tickLine={false}
                axisLine={false}
                width={42}
                tickFormatter={(v) => `${v}%`}
              />
              <ZAxis range={[60, 60]} />
              <Tooltip
                cursor={{ strokeDasharray: "3 3", stroke: "var(--muted-foreground)" }}
                content={({ active, payload }) => {
                  if (!active || !payload?.length) return null;
                  const p = payload[0].payload as { x: number; y: number; bond: string };
                  return (
                    <div className="rounded-lg border border-border bg-popover/95 px-3 py-2 text-xs shadow-xl backdrop-blur">
                      <div className="font-medium text-foreground">{p.bond}</div>
                      <div className="mt-0.5 text-muted-foreground">
                        {fmtNum(p.x, 1)}y · <span className="font-mono text-foreground">{fmtNum(p.y, 2)}%</span>
                      </div>
                    </div>
                  );
                }}
              />
              {/* previous month, faded */}
              {prevByType.map((t) => (
                <Scatter
                  key={`prev-${t.key}`}
                  name={`${t.label} (prev)`}
                  data={t.points}
                  fill={t.color}
                  fillOpacity={0.18}
                  line={{ stroke: t.color, strokeWidth: 1, strokeDasharray: "3 3", strokeOpacity: 0.3 }}
                  lineType="joint"
                  shape="circle"
                  isAnimationActive={false}
                  legendType="none"
                />
              ))}
              {byType.map((t) => (
                <Scatter
                  key={t.key}
                  name={t.label}
                  data={t.points}
                  fill={t.color}
                  line={{ stroke: t.color, strokeWidth: 2 }}
                  lineType="joint"
                  isAnimationActive={false}
                />
              ))}
            </ScatterChart>
          </ResponsiveContainer>
        </div>

        {/* history of average yields */}
        <div className="lg:col-span-2">
          <ResponsiveContainer width="100%" height={380}>
            <LineChart data={history} margin={{ top: 10, right: 12, bottom: 18, left: 4 }}>
              <CartesianGrid stroke="var(--border)" strokeDasharray="2 4" vertical={false} />
              <XAxis
                dataKey="date"
                tick={{ fill: "var(--muted-foreground)", fontSize: 10 }}
                tickLine={false}
                axisLine={{ stroke: "var(--border)" }}
                tickFormatter={(v) => fmtMonthShort(String(v))}
                minTickGap={28}
              />
              <YAxis
                tick={{ fill: "var(--muted-foreground)", fontSize: 11 }}
                tickLine={false}
                axisLine={false}
                width={36}
                tickFormatter={(v) => `${v}%`}
              />
              <Tooltip
                content={({ active, payload, label }) => {
                  if (!active || !payload?.length) return null;
                  return (
                    <div className="rounded-lg border border-border bg-popover/95 px-3 py-2 text-xs shadow-xl backdrop-blur">
                      <div className="mb-1 text-muted-foreground">{fmtMonth(String(label))}</div>
                      {payload.map((p) => {
                        const t = TYPES.find((x) => x.key === p.dataKey);
                        return (
                          <div key={String(p.dataKey)} className="flex items-center justify-between gap-3">
                            <span className="flex items-center gap-1.5 text-muted-foreground">
                              <span className="size-2 rounded-full" style={{ background: t?.color }} />
                              {t?.label}
                            </span>
                            <span className="font-mono text-foreground">{fmtNum(Number(p.value), 2)}%</span>
                          </div>
                        );
                      })}
                    </div>
                  );
                }}
              />
              {TYPES.map((t) => (
                <Line
                  key={t.key}
                  type="monotone"
                  dataKey={t.key}
                  stroke={t.color}
                  strokeWidth={1.75}
                  dot={false}
                  isAnimationActive={false}
                />
              ))}
              <Line
                type="monotone"
                dataKey={() => null}
                legendType="none"
                dot={false}
                isAnimationActive={false}
              />
            </LineChart>
          </ResponsiveContainer>
          <p className="px-2 pt-1 text-center text-[11px] text-muted-foreground">
            Average yield by class over time — drag the slider or hit play.
          </p>
        </div>
      </div>

      {/* legend */}
      <div className="mt-3 flex flex-wrap items-center justify-center gap-4 border-t border-border/60 pt-3">
        {TYPES.map((t) => (
          <span key={t.key} className="inline-flex items-center gap-1.5 text-xs text-muted-foreground">
            <span className="size-2.5 rounded-full" style={{ background: t.color }} />
            {t.label}
          </span>
        ))}
      </div>
    </div>
  );
}
