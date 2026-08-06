import { PageHeader } from "@/components/page-header";
import { Panel } from "@/components/panel";
import { StatCard } from "@/components/stat-card";
import { FuturesCurveChart } from "@/components/charts/futures-curve";
import { OpenInterestChart } from "@/components/charts/open-interest";
import { getFuturesCurve, getOpenInterest } from "@/lib/data";
import { fmtCompact, fmtNum, fmtSigned } from "@/lib/format";

export const dynamic = "force-dynamic";

const CURVE_ORDER = ["DI1", "DAP", "DDI"];
const OI_ORDER = ["DI1", "DOL", "WDO", "IND", "WIN", "BGI", "CCM", "ETH", "ICF", "DAP"];

export default async function DerivativesPage() {
  const [curve, oi] = await Promise.all([getFuturesCurve(), getOpenInterest()]);
  const curveAssets = CURVE_ORDER.filter((a) => curve.some((r) => r.asset === a));
  const oiAssets = OI_ORDER.filter((a) => oi.some((r) => r.asset === a));

  if (curveAssets.length === 0 && oiAssets.length === 0) {
    return (
      <div className="mx-auto max-w-[1200px] px-5 py-8 md:px-8 lg:py-10">
        <PageHeader
          kicker="Markets · Derivatives"
          title="Derivatives."
          lede="B3 futures term structure and open interest. No derivatives snapshot is loaded yet — the refresh job will populate this within the hour."
        />
      </div>
    );
  }

  const asOf = curve.reduce((m, r) => (r.date > m ? r.date : m), oi[0]?.date ?? "");

  // DI1 term-structure KPIs (latest snapshot).
  const di = curve.filter((r) => r.asset === "DI1");
  const diLast = di.reduce((m, r) => (r.date > m ? r.date : m), "");
  const diCurve = di.filter((r) => r.date === diLast).sort((a, b) => a.days_to_maturity - b.days_to_maturity);
  const front = diCurve[0];
  const long = diCurve[diCurve.length - 1];
  const slope = front && long ? long.settlement_rate - front.settlement_rate : null;

  const diOi = oi.filter((r) => r.asset === "DI1").sort((a, b) => a.date.localeCompare(b.date));
  const diOiLast = diOi[diOi.length - 1];
  const diOiSpark = diOi.slice(-60).map((r) => r.total_open_interest);

  return (
    <div className="mx-auto max-w-[1200px] px-5 py-8 md:px-8 lg:py-10">
      <PageHeader
        kicker="Markets · Derivatives"
        title="The DI curve & the crowd."
        lede="B3 futures: the DI interest-rate term structure (where the market prices the path of Selic) and open interest — how many contracts are live, i.e. where positioning is building."
        asOf={asOf || undefined}
      />

      {front && long && (
        <div className="mb-6 grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-4">
          <StatCard
            featured
            style={{ animationDelay: "60ms" }}
            label="DI front rate"
            value={fmtNum(front.settlement_rate, 2)}
            unit="%"
            accent="var(--chart-1)"
          />
          <StatCard
            style={{ animationDelay: "120ms" }}
            label="DI long rate"
            value={fmtNum(long.settlement_rate, 2)}
            unit="%"
            accent="var(--chart-1)"
          />
          <StatCard
            style={{ animationDelay: "180ms" }}
            label="Curve slope"
            value={slope != null ? fmtSigned(slope, 2) : "—"}
            unit="pp"
            accent="var(--chart-3)"
          />
          {diOiLast && (
            <StatCard
              style={{ animationDelay: "240ms" }}
              label="DI open interest"
              value={fmtCompact(diOiLast.total_open_interest)}
              series={diOiSpark}
              delta={diOiLast.total_open_interest_var ?? undefined}
              deltaLabel={diOiLast.total_open_interest_var != null ? `${fmtSigned(diOiLast.total_open_interest_var, 0)} d/d` : undefined}
              accent="var(--chart-2)"
            />
          )}
        </div>
      )}

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-5">
        {curveAssets.length > 0 && (
          <Panel
            style={{ animationDelay: "300ms" }}
            className="lg:col-span-3"
            title="Futures term structure"
            subtitle="Settlement rate by maturity. The dashed line is a prior-month snapshot — watch the whole curve shift, steepen or invert."
          >
            <FuturesCurveChart rows={curve} assets={curveAssets} />
          </Panel>
        )}
        {oiAssets.length > 0 && (
          <Panel
            style={{ animationDelay: "360ms" }}
            className="lg:col-span-2"
            title="Open interest"
            subtitle="Live contracts per underlying, summed across maturities — a read on positioning."
          >
            <OpenInterestChart rows={oi} assets={oiAssets} />
          </Panel>
        )}
      </div>

      <p className="rise mt-6 text-xs text-muted-foreground" style={{ animationDelay: "440ms" }}>
        Source <span className="font-mono text-foreground/70">gold/mart_futures_curve</span> (DI/DAP/DDI term
        structure) and <span className="font-mono text-foreground/70">gold/mart_open_interest</span> — B3 public
        market-data portal, futures segments. Curve snapshots are month-end.
      </p>
    </div>
  );
}
