"use client";

import { bisectCenter, extent } from "d3-array";
import { scaleLinear, scaleUtc } from "d3-scale";
import { line as lineShape } from "d3-shape";
import { useEffect, useMemo, useRef, useState } from "react";

import type { Line } from "@/server/snapshot";

export type ChartSeries = { key: string; label: string; color: string; data: Line };

type Props = {
  title: string;
  subtitle: string;
  series: ChartSeries[];
  /** Visible window, epoch milliseconds. */
  from: number;
  format: (value: number) => string;
  /** Draw a baseline at zero when the data crosses it. */
  zeroLine?: boolean;
};

const HEIGHT = 300;
const MARGIN = { top: 12, right: 64, bottom: 28, left: 44 };
const LABEL_GAP = 14;

const dateLabel = new Intl.DateTimeFormat("en", { year: "numeric", month: "short", day: "numeric", timeZone: "UTC" });
const monthLabel = new Intl.DateTimeFormat("en", { year: "numeric", month: "short", timeZone: "UTC" });

function clip(data: Line, from: number): Line {
  const start = data.t.findIndex((t) => t >= from);
  return start <= 0 ? data : { t: data.t.slice(start), v: data.v.slice(start) };
}

function lastValue(data: Line): { t: number; v: number } | null {
  for (let i = data.v.length - 1; i >= 0; i--) {
    const v = data.v[i];
    if (v != null) return { t: data.t[i], v };
  }
  return null;
}

export function LineChart({ title, subtitle, series, from, format, zeroLine }: Props) {
  const frame = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(720);
  const [hover, setHover] = useState<number | null>(null);

  useEffect(() => {
    const node = frame.current;
    if (!node) return;
    const observer = new ResizeObserver(([entry]) => setWidth(entry.contentRect.width));
    observer.observe(node);
    return () => observer.disconnect();
  }, []);

  const visible = useMemo(() => series.map((s) => ({ ...s, data: clip(s.data, from) })), [series, from]);
  const daily = visible.some((s) => s.data.t.length > 1 && s.data.t[1] - s.data.t[0] < 20 * 86_400_000);

  const { x, y, paths, ends } = useMemo(() => {
    const times = visible.flatMap((s) => s.data.t);
    const values = visible.flatMap((s) => s.data.v).filter((v): v is number => v != null);
    const [low, high] = extent(values) as [number, number];
    const x = scaleUtc()
      .domain(extent(times) as [number, number])
      .range([MARGIN.left, width - MARGIN.right]);
    const y = scaleLinear()
      .domain([Math.min(low, zeroLine ? 0 : low), high])
      .nice(5)
      .range([HEIGHT - MARGIN.bottom, MARGIN.top]);
    const paths = visible.map((s) => {
      const points = s.data.t.map((t, i) => [t, s.data.v[i]] as [number, number | null]);
      const draw = lineShape<[number, number | null]>()
        .defined((p) => p[1] != null)
        .x((p) => x(p[0]))
        .y((p) => y(p[1] as number));
      return draw(points) ?? "";
    });
    const ends = visible.map((s) => lastValue(s.data));
    return { x, y, paths, ends };
  }, [visible, width, zeroLine]);

  // End labels only when they do not collide; otherwise the legend carries identity.
  const endYs = ends.map((e) => (e ? y(e.v) : null)).filter((v): v is number => v != null).sort((a, b) => a - b);
  const labelEnds = endYs.every((v, i) => i === 0 || v - endYs[i - 1] >= LABEL_GAP);

  const readout = useMemo(() => {
    if (hover == null) return null;
    const time = x.invert(hover).getTime();
    const anchor = visible[0].data;
    const snapped = anchor.t[bisectCenter(anchor.t, time)];
    return {
      time: snapped,
      rows: visible.map((s) => {
        const i = bisectCenter(s.data.t, snapped);
        return { key: s.key, label: s.label, color: s.color, value: s.data.v[i] };
      }),
    };
  }, [hover, visible, x]);

  const xTicks = x.ticks(Math.max(2, Math.floor(width / 110)));
  const yTicks = y.ticks(5);
  const monthTicks = xTicks.length > 1 && xTicks[1].getTime() - xTicks[0].getTime() < 300 * 86_400_000;
  const tipLeft = readout ? x(readout.time) : 0;
  const recent = useMemo(() => {
    const anchor = visible[0].data;
    const start = Math.max(0, anchor.t.length - 12);
    return anchor.t.slice(start).map((t, offset) => ({
      t,
      values: visible.map((s) => s.data.v[bisectCenter(s.data.t, t)] ?? null),
      i: start + offset,
    }));
  }, [visible]);

  return (
    <figure className="card chart">
      <figcaption>
        <h3>{title}</h3>
        <p>{subtitle}</p>
      </figcaption>
      {series.length > 1 && (
        <ul className="legend">
          {series.map((s) => (
            <li key={s.key}>
              <span className="key" style={{ background: s.color }} />
              {s.label}
            </li>
          ))}
        </ul>
      )}
      <div
        className="plot"
        ref={frame}
        onPointerMove={(e) => {
          const box = e.currentTarget.getBoundingClientRect();
          const px = Math.min(Math.max(e.clientX - box.left, MARGIN.left), width - MARGIN.right);
          setHover(px);
        }}
        onPointerLeave={() => setHover(null)}
      >
        <svg width={width} height={HEIGHT} role="img" aria-label={`${title}. ${subtitle}`}>
          {yTicks.map((tick) => (
            <g key={tick}>
              <line className={zeroLine && tick === 0 ? "baseline" : "grid"} x1={MARGIN.left} x2={width - MARGIN.right} y1={y(tick)} y2={y(tick)} />
              <text className="tick" x={MARGIN.left - 8} y={y(tick)} dy="0.32em" textAnchor="end">
                {format(tick)}
              </text>
            </g>
          ))}
          {xTicks.map((tick) => (
            <text key={tick.getTime()} className="tick" x={x(tick)} y={HEIGHT - 8} textAnchor="middle">
              {monthTicks ? monthLabel.format(tick) : tick.getUTCFullYear()}
            </text>
          ))}
          {readout && <line className="crosshair" x1={tipLeft} x2={tipLeft} y1={MARGIN.top} y2={HEIGHT - MARGIN.bottom} />}
          {paths.map((d, i) => (
            <path key={visible[i].key} className="series" d={d} stroke={visible[i].color} />
          ))}
          {ends.map(
            (end, i) =>
              end && (
                <g key={visible[i].key}>
                  <circle className="dot" cx={x(end.t)} cy={y(end.v)} r={4} fill={visible[i].color} />
                  {labelEnds && (
                    <text className="end" x={x(end.t) + 9} y={y(end.v)} dy="0.32em">
                      {format(end.v)}
                    </text>
                  )}
                </g>
              ),
          )}
          {readout?.rows.map(
            (row) =>
              row.value != null && (
                <circle key={row.key} className="dot" cx={tipLeft} cy={y(row.value)} r={4} fill={row.color} />
              ),
          )}
        </svg>
        {readout && (
          <div className="tip" style={tipLeft > width / 2 ? { right: width - tipLeft + 12 } : { left: tipLeft + 12 }}>
            <div className="tip-date">{(daily ? dateLabel : monthLabel).format(readout.time)}</div>
            {readout.rows.map((row) => (
              <div className="tip-row" key={row.key}>
                <span className="stroke" style={{ background: row.color }} />
                <strong>{row.value == null ? "n/a" : format(row.value)}</strong>
                <span>{row.label}</span>
              </div>
            ))}
          </div>
        )}
      </div>
      <details>
        <summary>Latest values as a table</summary>
        <div className="scroll">
          <table>
            <thead>
              <tr>
                <th>{daily ? "Date" : "Month"}</th>
                {series.map((s) => (
                  <th key={s.key} className="num">
                    {s.label}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {[...recent].reverse().map((row) => (
                <tr key={row.i}>
                  <td>{(daily ? dateLabel : monthLabel).format(row.t)}</td>
                  {row.values.map((value, i) => (
                    <td key={series[i].key} className="num">
                      {value == null ? "n/a" : format(value)}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </details>
    </figure>
  );
}
