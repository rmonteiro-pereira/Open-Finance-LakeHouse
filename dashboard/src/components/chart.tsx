import type { SeriesMeta } from "@/lib/release";

/**
 * Charts as server-rendered inline SVG. No chart library, no client runtime.
 *
 * The site is a static export read next to BACEN and IBGE tabs. A React chart runtime to
 * draw a 72-point line is weight without return; markup renders sharp, prints, and works
 * with JavaScript off. Per-point values ride native `<title>` tooltips, and every chart on
 * this site keeps its table twin — a tooltip must enhance, never gate.
 *
 * Mark specs are fixed and shared: 2px lines with round caps, markers r >= 4 carrying a
 * 2px surface ring, bars capped at 24px with a 4px rounded data-end and a 2px surface gap,
 * hairline solid grid (never dashed) one step off the surface.
 */

const INK = "var(--ink)";
const MUTED = "var(--ink-muted)";
const RULE = "var(--rule)";
const SURFACE = "var(--paper)";

export type Point = { x: number; y: number; label?: string };

function scale(vals: number[], pad = 0.06): [number, number] {
  const lo = Math.min(...vals);
  const hi = Math.max(...vals);
  if (lo === hi) return [lo - 1, hi + 1];
  const span = hi - lo;
  return [lo - span * pad, hi + span * pad];
}

function path(pts: { px: number; py: number }[]): string {
  return pts.map((p, i) => `${i === 0 ? "M" : "L"}${p.px.toFixed(2)} ${p.py.toFixed(2)}`).join(" ");
}

/**
 * Sparkline — the number's own history, beside the number.
 *
 * Tufte's form and Tufte's reason: a level with no history is an assertion. Line in the
 * de-emphasis hue, current point in the accent, no axes and no labels — the stat tile
 * carries those.
 */
export function Sparkline({
  values,
  width = 132,
  height = 30,
  label,
}: {
  values: number[];
  width?: number;
  height?: number;
  label?: string;
}) {
  if (values.length < 2) return null;
  const [lo, hi] = scale(values);
  const pad = 4;
  const pts = values.map((v, i) => ({
    px: pad + (i / (values.length - 1)) * (width - pad * 2),
    py: height - pad - ((v - lo) / (hi - lo)) * (height - pad * 2),
  }));
  const last = pts[pts.length - 1];

  return (
    <svg
      viewBox={`0 0 ${width} ${height}`}
      width={width}
      height={height}
      role="img"
      aria-label={label ?? "histórico da série"}
      className="overflow-visible"
    >
      <path
        d={path(pts)}
        fill="none"
        stroke={MUTED}
        strokeWidth={1.5}
        strokeLinecap="round"
        strokeLinejoin="round"
        vectorEffect="non-scaling-stroke"
      />
      {/* Surface ring, so the end-dot stays legible where it crosses the line. */}
      <circle cx={last.px} cy={last.py} r={4} fill={SURFACE} />
      <circle cx={last.px} cy={last.py} r={2.5} fill="var(--accent)" />
    </svg>
  );
}

/**
 * One series over time. Single series, so no legend: the heading names it.
 *
 * Only the endpoint and the extremes are labelled. A value on every point is chaos and
 * goes unread, and the axis plus the table below carry the rest.
 */
export function SeriesLine({
  points,
  meta,
  height = 220,
  unitSuffix = "",
}: {
  points: { date: string; value: number }[];
  meta?: SeriesMeta;
  height?: number;
  unitSuffix?: string;
}) {
  if (points.length < 2) return null;
  const W = 720;
  const padL = 8;
  const padR = 58;
  const padT = 14;
  const padB = 26;
  const values = points.map((p) => p.value);
  const [lo, hi] = scale(values);

  const pts = points.map((p, i) => ({
    px: padL + (i / (points.length - 1)) * (W - padL - padR),
    py: padT + (1 - (p.value - lo) / (hi - lo)) * (height - padT - padB),
    ...p,
  }));
  const last = pts[pts.length - 1];
  const maxPt = pts.reduce((a, b) => (b.value > a.value ? b : a));
  const minPt = pts.reduce((a, b) => (b.value < a.value ? b : a));
  const fmt = (v: number) => v.toLocaleString("pt-BR", { maximumFractionDigits: 2 });

  return (
    <figure className="space-y-2">
      <svg
        viewBox={`0 0 ${W} ${height}`}
        className="h-auto w-full overflow-visible"
        role="img"
        aria-label={`${meta?.name ?? "série"}: ${points.length} observações de ${points[0].date} a ${last.date}`}
      >
        {/* Hairline, solid, one step off the surface. Two lines, not a mesh. */}
        {[0, 1].map((k) => {
          const y = padT + k * (height - padT - padB);
          return <line key={k} x1={padL} x2={W - padR} y1={y} y2={y} stroke={RULE} strokeWidth={1} />;
        })}
        <text x={W - padR + 8} y={padT + 4} fontSize={11} fill={MUTED} className="tnum">
          {fmt(hi)}
        </text>
        <text x={W - padR + 8} y={height - padB + 4} fontSize={11} fill={MUTED} className="tnum">
          {fmt(lo)}
        </text>

        <path
          d={path(pts)}
          fill="none"
          stroke={INK}
          strokeWidth={2}
          strokeLinecap="round"
          strokeLinejoin="round"
          vectorEffect="non-scaling-stroke"
        />

        {/* Native tooltips: zero JS, and the table below is the ungated path. */}
        {pts.map((p) => (
          <circle key={p.date} cx={p.px} cy={p.py} r={7} fill="transparent">
            <title>{`${p.date}: ${fmt(p.value)}${unitSuffix}`}</title>
          </circle>
        ))}

        {[minPt, maxPt].map((p) => (
          <g key={`x-${p.date}`}>
            <circle cx={p.px} cy={p.py} r={4} fill={SURFACE} />
            <circle cx={p.px} cy={p.py} r={2} fill={MUTED} />
          </g>
        ))}

        <circle cx={last.px} cy={last.py} r={6} fill={SURFACE} />
        <circle cx={last.px} cy={last.py} r={3.5} fill="var(--accent)" />
        <text x={last.px + 10} y={last.py + 4} fontSize={12} fill={INK} className="tnum">
          {fmt(last.value)}
        </text>

        <text x={padL} y={height - 6} fontSize={11} fill={MUTED} className="tnum">
          {points[0].date}
        </text>
        <text x={W - padR} y={height - 6} fontSize={11} fill={MUTED} textAnchor="end" className="tnum">
          {last.date}
        </text>
      </svg>
    </figure>
  );
}

/**
 * The distribution behind a percentile — the evidence, not decoration.
 *
 * "Percentil 87 da década" is an assertion until the reader can see the decade. This is
 * the one chart on the site that is load-bearing for the product's central claim, and it
 * was missing while the claim was being made.
 */
export function DistributionStrip({
  values,
  current,
  windowLabel,
  bins = 28,
  height = 92,
}: {
  values: number[];
  current: number;
  windowLabel: string;
  bins?: number;
  height?: number;
}) {
  if (values.length < 8) return null;
  const W = 720;
  const padB = 22;
  const lo = Math.min(...values, current);
  const hi = Math.max(...values, current);
  const step = (hi - lo) / bins || 1;

  const counts = new Array(bins).fill(0);
  for (const v of values) {
    counts[Math.min(bins - 1, Math.max(0, Math.floor((v - lo) / step)))] += 1;
  }
  const peak = Math.max(...counts);
  const slot = W / bins;
  const barW = Math.min(24, slot - 2); // 2px surface gap between neighbours
  const currentX = ((current - lo) / (hi - lo || 1)) * (W - barW) + barW / 2;
  const fmt = (v: number) => v.toLocaleString("pt-BR", { maximumFractionDigits: 2 });

  return (
    <figure className="space-y-2">
      <svg
        viewBox={`0 0 ${W} ${height}`}
        className="h-auto w-full overflow-visible"
        role="img"
        aria-label={`distribuição de ${values.length} observações na janela ${windowLabel}; o valor de hoje é ${fmt(current)}`}
      >
        {counts.map((c, i) => {
          const h = c === 0 ? 0 : Math.max(2, (c / peak) * (height - padB - 6));
          const x = i * slot + (slot - barW) / 2;
          return (
            <rect
              key={i}
              x={x}
              y={height - padB - h}
              width={barW}
              height={h}
              rx={2}
              fill={RULE}
            >
              <title>{`${fmt(lo + i * step)} – ${fmt(lo + (i + 1) * step)}: ${c} observações`}</title>
            </rect>
          );
        })}
        <line x1={0} x2={W} y1={height - padB} y2={height - padB} stroke={RULE} strokeWidth={1} />

        <line
          x1={currentX}
          x2={currentX}
          y1={2}
          y2={height - padB}
          stroke="var(--accent)"
          strokeWidth={2}
          strokeLinecap="round"
        />
        <text
          x={currentX}
          y={height - 6}
          fontSize={11}
          fill={INK}
          textAnchor={currentX > W * 0.8 ? "end" : currentX < W * 0.2 ? "start" : "middle"}
          className="tnum"
        >
          hoje {fmt(current)}
        </text>
      </svg>
      <figcaption className="text-xs text-[var(--ink-muted)]">
        {values.length} observações da janela {windowLabel}. A barra marca onde o valor de hoje
        cai na distribuição: é a prova do percentil, não a ilustração dele.
      </figcaption>
    </figure>
  );
}

/**
 * The Tesouro curve: rate against years to maturity, which is what a curve IS.
 *
 * It shipped as a table of a curve. Two categorical series (IPCA+, Prefixado) in a pair
 * validated at ΔE 20 under protanopia; a legend is present because there are two, and the
 * endpoints are directly labelled because they separate cleanly at the right edge.
 */
export type CurvePoint = { instrumentId: string; label: string; years: number; rate: number; family: string };

export function CurveChart({ points, height = 260 }: { points: CurvePoint[]; height?: number }) {
  if (points.length < 2) return null;
  const W = 720;
  const padL = 34;
  const padR = 92;
  const padT = 16;
  const padB = 30;

  // Fixed order, never cycled: a hue is bound to a family name, so filtering one out
  // cannot repaint the survivors. Past the three slots a family would fold into "Outros"
  // rather than receive a generated hue.
  const SLOTS = ["var(--series-1)", "var(--series-2)", "var(--series-3)"];
  const families = [...new Set(points.map((p) => p.family))].sort();
  const colour = (f: string) => SLOTS[families.indexOf(f) % SLOTS.length];

  const [xLo, xHi] = scale(points.map((p) => p.years), 0.12);
  const [yLo, yHi] = scale(points.map((p) => p.rate), 0.18);
  const X = (v: number) => padL + ((v - xLo) / (xHi - xLo)) * (W - padL - padR);
  const Y = (v: number) => padT + (1 - (v - yLo) / (yHi - yLo)) * (height - padT - padB);
  const fmt = (v: number) => v.toLocaleString("pt-BR", { maximumFractionDigits: 2 });

  return (
    <figure className="space-y-3">
      <svg
        viewBox={`0 0 ${W} ${height}`}
        className="h-auto w-full overflow-visible"
        role="img"
        aria-label="curva do Tesouro Direto: taxa de venda por prazo até o vencimento"
      >
        {[yLo, (yLo + yHi) / 2, yHi].map((v) => (
          <g key={v}>
            <line x1={padL} x2={W - padR} y1={Y(v)} y2={Y(v)} stroke={RULE} strokeWidth={1} />
            <text x={padL - 8} y={Y(v) + 4} fontSize={11} fill={MUTED} textAnchor="end" className="tnum">
              {fmt(v)}
            </text>
          </g>
        ))}

        {families.map((f) => {
          const fam = points.filter((p) => p.family === f).sort((a, b) => a.years - b.years);
          const pts = fam.map((p) => ({ px: X(p.years), py: Y(p.rate) }));
          const end = fam[fam.length - 1];
          return (
            <g key={f}>
              {pts.length > 1 ? (
                <path
                  d={path(pts)}
                  fill="none"
                  stroke={colour(f)}
                  strokeWidth={2}
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  vectorEffect="non-scaling-stroke"
                />
              ) : null}
              {fam.map((p) => (
                <g key={p.instrumentId}>
                  <circle cx={X(p.years)} cy={Y(p.rate)} r={6} fill={SURFACE} />
                  <circle cx={X(p.years)} cy={Y(p.rate)} r={4} fill={colour(f)}>
                    <title>{`${p.label} · ${fmt(p.years)} anos · ${fmt(p.rate)}%`}</title>
                  </circle>
                </g>
              ))}
              {/* Direct label at the end: text in ink, identity from the dot beside it. */}
              <text x={X(end.years) + 12} y={Y(end.rate) + 4} fontSize={11} fill={INK}>
                {f}
              </text>
            </g>
          );
        })}

        <text x={W - padR} y={height - 6} fontSize={11} fill={MUTED} textAnchor="end">
          anos até o vencimento
        </text>
      </svg>

      <div className="flex flex-wrap gap-x-5 gap-y-1 text-xs text-[var(--ink-muted)]">
        {families.map((f) => (
          <span key={f} className="inline-flex items-center gap-1.5">
            <span
              aria-hidden
              className="inline-block h-0.5 w-4 rounded-full"
              style={{ background: colour(f) }}
            />
            {f}
          </span>
        ))}
      </div>
    </figure>
  );
}

/**
 * Monthly variation as columns. Paired with the accumulated line as SMALL MULTIPLES,
 * never as a second y-axis: two scales on one plot invent a correlation the data has not
 * got, and it is the single most common charting mistake.
 */
export function Columns({
  points,
  height = 140,
  unitSuffix = "",
}: {
  points: { date: string; value: number }[];
  height?: number;
  unitSuffix?: string;
}) {
  if (points.length < 2) return null;
  const W = 720;
  const padT = 12;
  const padB = 24;
  const padR = 48;
  const hi = Math.max(...points.map((p) => Math.abs(p.value)));
  const slot = (W - padR) / points.length;
  const barW = Math.min(24, slot - 2);
  const fmt = (v: number) => v.toLocaleString("pt-BR", { maximumFractionDigits: 2 });
  const last = points[points.length - 1];

  return (
    <figure>
      <svg
        viewBox={`0 0 ${W} ${height}`}
        className="h-auto w-full overflow-visible"
        role="img"
        aria-label={`variação mensal, ${points.length} meses`}
      >
        {points.map((p, i) => {
          const h = Math.max(1, (Math.abs(p.value) / hi) * (height - padT - padB));
          return (
            <rect
              key={p.date}
              x={i * slot + (slot - barW) / 2}
              y={height - padB - h}
              width={barW}
              height={h}
              rx={2}
              fill={i === points.length - 1 ? "var(--accent)" : RULE}
            >
              <title>{`${p.date}: ${fmt(p.value)}${unitSuffix}`}</title>
            </rect>
          );
        })}
        <line x1={0} x2={W - padR} y1={height - padB} y2={height - padB} stroke={RULE} strokeWidth={1} />
        <text x={W - padR + 8} y={height - padB} fontSize={12} fill={INK} className="tnum">
          {fmt(last.value)}
          {unitSuffix}
        </text>
        <text x={0} y={height - 6} fontSize={11} fill={MUTED} className="tnum">
          {points[0].date}
        </text>
        <text x={W - padR} y={height - 6} fontSize={11} fill={MUTED} textAnchor="end" className="tnum">
          {last.date}
        </text>
      </svg>
    </figure>
  );
}
