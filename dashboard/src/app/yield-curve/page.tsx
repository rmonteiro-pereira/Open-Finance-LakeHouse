import { PageHeader } from "@/components/page-header";
import { Panel } from "@/components/panel";
import { StatCard } from "@/components/stat-card";
import { YieldCurveChart } from "@/components/charts/yield-curve";
import { getYieldCurve } from "@/lib/data";
import { fmtNum } from "@/lib/format";

export const dynamic = "force-dynamic";

export default async function YieldCurvePage() {
  const rows = await getYieldCurve();
  const dates = [...new Set(rows.map((r) => r.date))].sort();
  const lastDate = dates[dates.length - 1];
  const latest = rows.filter((r) => r.date === lastDate);

  const avg = (type: string) => {
    const ys = latest.filter((r) => r.bond_type === type).map((r) => r.yield);
    return ys.length ? ys.reduce((s, v) => s + v, 0) / ys.length : 0;
  };

  const pre = latest.filter((r) => r.bond_type === "prefixado").sort((a, b) => a.years_to_maturity - b.years_to_maturity);
  const slope = pre.length >= 2 ? pre[pre.length - 1].yield - pre[0].yield : 0;
  // dead-band: a near-zero slope is "flat", not "inverted" — don't dramatise noise
  const slopeState = slope > 0.15 ? "up" : slope < -0.15 ? "inverted" : "flat";
  const slopeLabel =
    slopeState === "inverted"
      ? "Curve slope · inverted"
      : slopeState === "flat"
        ? "Curve slope · flat"
        : "Curve slope (long − short)";
  const slopeAccent =
    slopeState === "inverted" ? "var(--negative)" : slopeState === "flat" ? "var(--muted-foreground)" : "var(--chart-5)";

  return (
    <div className="mx-auto max-w-[1200px] px-5 py-8 md:px-8 lg:py-10">
      <PageHeader
        kicker="Brazil · Treasury"
        title="The curve, through time."
        lede="The Tesouro Direto term structure — nominal (prefixado), inflation-linked (IPCA+) and floating (Selic). Press play to watch the curve shift, steepen and flatten."
        asOf={lastDate}
      />

      <div className="mb-6 grid grid-cols-1 gap-3 sm:grid-cols-3">
        <StatCard
          style={{ animationDelay: "60ms" }}
          label="Prefixado · avg"
          value={fmtNum(avg("prefixado"), 2)}
          unit="% a.a."
          accent="var(--chart-2)"
        />
        <StatCard
          style={{ animationDelay: "120ms" }}
          label="IPCA+ real · avg"
          value={fmtNum(avg("ipca_plus"), 2)}
          unit="% + IPCA"
          accent="var(--chart-1)"
        />
        <StatCard
          featured
          style={{ animationDelay: "180ms" }}
          label={slopeLabel}
          value={fmtNum(slope, 2)}
          unit="pp"
          accent={slopeAccent}
        />
      </div>

      <Panel
        style={{ animationDelay: "260ms" }}
        title="Term structure"
        subtitle="Yield versus years to maturity. Each marker is a live bond; lines connect within a class."
      >
        <YieldCurveChart rows={rows} />
      </Panel>

      <p className="rise mt-6 text-xs text-muted-foreground" style={{ animationDelay: "340ms" }}>
        Source: <span className="font-mono text-foreground/70">gold/mart_yield_curve</span> · {dates.length} monthly
        snapshots. IPCA+ yields are real (add expected inflation for the nominal-equivalent).
      </p>
    </div>
  );
}
