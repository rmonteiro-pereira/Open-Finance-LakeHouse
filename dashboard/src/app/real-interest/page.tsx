import { PageHeader } from "@/components/page-header";
import { Panel } from "@/components/panel";
import { StatCard } from "@/components/stat-card";
import { LinePanelChart } from "@/components/charts/line-panel";
import { getRealInterest } from "@/lib/data";
import { fmtNum, fmtPct, fmtSigned } from "@/lib/format";

export const dynamic = "force-dynamic";

export default async function RealInterestPage() {
  const real = await getRealInterest();
  const last = real[real.length - 1];
  const prev = real[real.length - 2];
  const y1 = real[real.length - 13] ?? real[0];
  const tail = real.slice(-24);

  const minReal = real.reduce((m, r) => (r.real_interest_rate < m.real_interest_rate ? r : m));

  return (
    <div className="mx-auto max-w-[1200px] px-5 py-8 md:px-8 lg:py-10">
      <PageHeader
        kicker="Brazil · Rates"
        title="What you actually earn."
        lede="Ex-post real interest: the Selic target deflated by accumulated 12-month IPCA. When the gold line dips below zero, cash is losing to inflation."
        asOf={last.month}
      />

      <div className="mb-6 grid grid-cols-1 gap-3 sm:grid-cols-3">
        <StatCard
          featured
          style={{ animationDelay: "60ms" }}
          label="Real interest"
          value={fmtNum(last.real_interest_rate, 2)}
          unit="% a.a."
          delta={last.real_interest_rate - prev.real_interest_rate}
          deltaLabel={`${fmtSigned(last.real_interest_rate - prev.real_interest_rate, 2)} pp m/m`}
          series={tail.map((r) => r.real_interest_rate)}
          accent="var(--chart-1)"
        />
        <StatCard
          style={{ animationDelay: "120ms" }}
          label="Nominal Selic"
          value={fmtNum(last.selic_target, 2)}
          unit="% a.a."
          delta={last.selic_target - y1.selic_target}
          deltaLabel={`${fmtSigned(last.selic_target - y1.selic_target, 2)} pp · 12m`}
          series={tail.map((r) => r.selic_target)}
          accent="var(--chart-2)"
        />
        <StatCard
          style={{ animationDelay: "180ms" }}
          label="IPCA · 12m"
          value={fmtNum(last.ipca_accum_12m, 2)}
          unit="%"
          delta={last.ipca_accum_12m - prev.ipca_accum_12m}
          deltaLabel={`${fmtSigned(last.ipca_accum_12m - prev.ipca_accum_12m, 2)} pp m/m`}
          series={tail.map((r) => r.ipca_accum_12m)}
          accent="var(--chart-4)"
          invertDelta
        />
      </div>

      <Panel
        style={{ animationDelay: "260ms" }}
        title="Nominal vs. real"
        subtitle="Selic target and accumulated IPCA define the real rate. The dashed line marks zero — below it, real returns are negative."
      >
        <LinePanelChart
          data={real}
          xKey="month"
          xType="month"
          height={380}
          leftUnit="% a.a."
          zeroLine
          series={[
            { key: "ipca_accum_12m", label: "IPCA 12m", color: "var(--chart-4)", kind: "area", digits: 2, unit: "%" },
            { key: "selic_target", label: "Selic target", color: "var(--chart-2)", digits: 2, unit: "%" },
            { key: "real_interest_rate", label: "Real interest", color: "var(--chart-1)", width: 2.5, digits: 2, unit: "%" },
          ]}
        />
      </Panel>

      <div className="rise mt-6 grid grid-cols-1 gap-3 text-sm md:grid-cols-2" style={{ animationDelay: "340ms" }}>
        <div className="rounded-lg border border-border/70 bg-card/50 p-4">
          <p className="text-xs uppercase tracking-wider text-muted-foreground">Method</p>
          <p className="mt-1 leading-relaxed text-muted-foreground">
            Real rate = <span className="font-mono text-foreground/80">(1 + Selic) / (1 + IPCA₁₂ₘ) − 1</span>.
            Ex-post (realised) — the ex-ante Focus read would substitute 12-month-ahead survey expectations.
          </p>
        </div>
        <div className="rounded-lg border border-border/70 bg-card/50 p-4">
          <p className="text-xs uppercase tracking-wider text-muted-foreground">Trough</p>
          <p className="mt-1 leading-relaxed text-muted-foreground">
            Lowest real rate in the window: <span className="text-negative font-medium">{fmtPct(minReal.real_interest_rate, 2)}</span>{" "}
            — the deeply negative real-rate episode during the easing cycle.
          </p>
        </div>
      </div>
    </div>
  );
}
