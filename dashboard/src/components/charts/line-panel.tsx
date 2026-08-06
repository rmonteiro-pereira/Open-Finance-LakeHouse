"use client";

import {
  Area,
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { fmtDate, fmtMonth, fmtMonthShort, fmtNum } from "@/lib/format";

export type SeriesDef = {
  key: string;
  label: string;
  color: string;
  kind?: "line" | "area";
  axis?: "left" | "right";
  unit?: string;
  digits?: number;
  dashed?: boolean;
  width?: number;
  /** recede a secondary series so a dense overlay stays legible (0–1) */
  opacity?: number;
};

type Props = {
  data: Record<string, number | string>[];
  xKey: string;
  xType: "month" | "date";
  series: SeriesDef[];
  height?: number;
  leftUnit?: string;
  rightUnit?: string;
  leftDomain?: [number | "auto" | "dataMin" | "dataMax", number | "auto" | "dataMin" | "dataMax"];
  rightDomain?: [number | "auto" | "dataMin" | "dataMax", number | "auto" | "dataMin" | "dataMax"];
  zeroLine?: boolean;
};

const axisTick = { fill: "var(--muted-foreground)", fontSize: 11 };

function TooltipBox({
  active,
  payload,
  label,
  xType,
  series,
}: {
  active?: boolean;
  payload?: { dataKey?: string | number; value?: number; color?: string }[];
  label?: string;
  xType: "month" | "date";
  series: SeriesDef[];
}) {
  if (!active || !payload?.length || label == null) return null;
  const heading = xType === "month" ? fmtMonth(String(label)) : fmtDate(String(label));
  return (
    <div className="min-w-[170px] rounded-lg border border-border bg-popover/95 px-3 py-2 shadow-xl backdrop-blur">
      <div className="mb-1.5 text-[11px] font-medium uppercase tracking-wider text-muted-foreground">
        {heading}
      </div>
      <div className="flex flex-col gap-1">
        {payload.map((p) => {
          const def = series.find((s) => s.key === p.dataKey);
          if (!def || p.value == null) return null;
          return (
            <div key={def.key} className="flex items-center justify-between gap-4 text-xs">
              <span className="flex items-center gap-1.5 text-muted-foreground">
                <span className="size-2 rounded-full" style={{ background: def.color }} />
                {def.label}
              </span>
              <span className="font-mono tabular-nums text-foreground">
                {fmtNum(p.value, def.digits ?? 2)}
                {def.unit ? <span className="text-muted-foreground"> {def.unit}</span> : null}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

export function LinePanelChart({
  data,
  xKey,
  xType,
  series,
  height = 300,
  leftUnit,
  rightUnit,
  leftDomain = ["auto", "auto"],
  rightDomain = ["auto", "auto"],
  zeroLine = false,
}: Props) {
  const hasRight = series.some((s) => s.axis === "right");
  const xFmt = xType === "month" ? fmtMonthShort : fmtDate;

  return (
    <ResponsiveContainer width="100%" height={height}>
      <ComposedChart data={data} margin={{ top: 8, right: hasRight ? 8 : 14, bottom: 4, left: 4 }}>
        <defs>
          {series.map((s) => (
            <linearGradient key={s.key} id={`grad-${s.key}`} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={s.color} stopOpacity={0.28} />
              <stop offset="100%" stopColor={s.color} stopOpacity={0.02} />
            </linearGradient>
          ))}
        </defs>
        <CartesianGrid stroke="var(--border)" strokeDasharray="2 4" vertical={false} />
        <XAxis
          dataKey={xKey}
          tickFormatter={(v) => xFmt(String(v))}
          tick={axisTick}
          tickLine={false}
          axisLine={{ stroke: "var(--border)" }}
          minTickGap={36}
          tickMargin={8}
        />
        <YAxis
          yAxisId="left"
          orientation="left"
          tick={axisTick}
          tickLine={false}
          axisLine={false}
          width={46}
          domain={leftDomain}
          tickFormatter={(v) => fmtNum(Number(v), 0)}
          label={
            leftUnit
              ? { value: leftUnit, angle: -90, position: "insideLeft", fill: "var(--muted-foreground)", fontSize: 10, dy: 20 }
              : undefined
          }
        />
        {hasRight && (
          <YAxis
            yAxisId="right"
            orientation="right"
            tick={axisTick}
            tickLine={false}
            axisLine={false}
            width={46}
            domain={rightDomain}
            tickFormatter={(v) => fmtNum(Number(v), 0)}
            label={
              rightUnit
                ? { value: rightUnit, angle: 90, position: "insideRight", fill: "var(--muted-foreground)", fontSize: 10, dy: -20 }
                : undefined
            }
          />
        )}
        <Tooltip
          cursor={{ stroke: "var(--muted-foreground)", strokeWidth: 1, strokeDasharray: "3 3" }}
          content={<TooltipBox xType={xType} series={series} />}
        />
        <Legend
          verticalAlign="top"
          align="left"
          height={28}
          iconType="plainline"
          wrapperStyle={{ fontSize: 12, paddingBottom: 8 }}
          formatter={(value) => <span className="text-muted-foreground">{value}</span>}
        />
        {zeroLine && (
          <Line
            yAxisId="left"
            dataKey={() => 0}
            stroke="var(--muted-foreground)"
            strokeWidth={1}
            strokeDasharray="4 4"
            dot={false}
            legendType="none"
            isAnimationActive={false}
            name="zero"
          />
        )}
        {series.map((s) =>
          s.kind === "area" ? (
            <Area
              key={s.key}
              yAxisId={s.axis ?? "left"}
              type="monotone"
              dataKey={s.key}
              name={s.label}
              stroke={s.color}
              strokeOpacity={s.opacity ?? 1}
              strokeWidth={s.width ?? 2}
              fill={`url(#grad-${s.key})`}
              fillOpacity={s.opacity ?? 1}
              dot={false}
              activeDot={{ r: 3, strokeWidth: 0 }}
              animationDuration={700}
            />
          ) : (
            <Line
              key={s.key}
              yAxisId={s.axis ?? "left"}
              type="monotone"
              dataKey={s.key}
              name={s.label}
              stroke={s.color}
              strokeOpacity={s.opacity ?? 1}
              strokeWidth={s.width ?? 2}
              strokeDasharray={s.dashed ? "5 4" : undefined}
              dot={false}
              activeDot={{ r: 3, strokeWidth: 0 }}
              animationDuration={700}
            />
          ),
        )}
      </ComposedChart>
    </ResponsiveContainer>
  );
}
