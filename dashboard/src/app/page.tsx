import { PageHeader } from "@/components/page-header";
import { Panel } from "@/components/panel";
import { StatCard } from "@/components/stat-card";
import { LinePanelChart } from "@/components/charts/line-panel";
import { getMacro, getRealInterest } from "@/lib/data";
import { fmtNum, fmtPct, fmtSigned, fmtSignedPct } from "@/lib/format";

export const dynamic = "force-dynamic";

export default async function MacroPage() {
  const [macro, real] = await Promise.all([getMacro(), getRealInterest()]);
  const last = macro[macro.length - 1];
  const prev = macro[macro.length - 2];
  const y1 = macro[macro.length - 13] ?? macro[0];
  const tail = macro.slice(-24);
  const lastReal = real[real.length - 1];
  const realPrev = real[real.length - 2];

  const spark = (k: keyof typeof last) => tail.map((r) => Number(r[k]));

  return (
    <div className="mx-auto max-w-[1200px] px-5 py-8 md:px-8 lg:py-10">
      <PageHeader
        kicker="Brazil · Macro"
        title="The macro panel, at a glance."
        lede="Policy rate, inflation, the exchange rate and public debt — the four dials that move Brazilian markets, drawn from the lakehouse's monthly gold mart."
        asOf={last.month}
      />

      <div className="mb-6 grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <StatCard
          featured
          style={{ animationDelay: "60ms" }}
          label="Selic target"
          value={fmtNum(last.selic_target, 2)}
          unit="% a.a."
          delta={last.selic_target - y1.selic_target}
          deltaLabel={`${fmtSigned(last.selic_target - y1.selic_target, 2)} pp · 12m`}
          series={spark("selic_target")}
          accent="var(--chart-1)"
        />
        <StatCard
          style={{ animationDelay: "120ms" }}
          label="IPCA · 12m"
          value={fmtNum(lastReal.ipca_accum_12m, 2)}
          unit="%"
          delta={lastReal.ipca_accum_12m - realPrev.ipca_accum_12m}
          deltaLabel={`${fmtSignedPct(lastReal.ipca_accum_12m - realPrev.ipca_accum_12m, 2)} m/m`}
          series={real.slice(-24).map((r) => r.ipca_accum_12m)}
          accent="var(--chart-4)"
          invertDelta
        />
        <StatCard
          style={{ animationDelay: "180ms" }}
          label="USD / BRL"
          value={fmtNum(last.usd_brl, 4)}
          delta={last.usd_brl - prev.usd_brl}
          deltaLabel={`${fmtSignedPct(((last.usd_brl - prev.usd_brl) / prev.usd_brl) * 100, 2)} m/m`}
          series={spark("usd_brl")}
          accent="var(--chart-2)"
          invertDelta
        />
        <StatCard
          style={{ animationDelay: "240ms" }}
          label="Gross debt / GDP"
          value={fmtNum(last.debt_to_gdp_pct, 1)}
          unit="% GDP"
          delta={last.debt_to_gdp_pct - y1.debt_to_gdp_pct}
          deltaLabel={`${fmtSigned(last.debt_to_gdp_pct - y1.debt_to_gdp_pct, 1)} pp · 12m`}
          series={spark("debt_to_gdp_pct")}
          accent="var(--chart-3)"
          invertDelta
        />
      </div>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
        <Panel
          style={{ animationDelay: "300ms" }}
          className="lg:col-span-3"
          title="Policy rate vs. the exchange rate"
          subtitle="Selic target (left) against USD/BRL (right) — the rate-cycle and currency story since 2000."
        >
          <LinePanelChart
            data={macro}
            xKey="month"
            xType="month"
            height={340}
            leftUnit="% a.a."
            rightUnit="USD/BRL"
            series={[
              { key: "selic_target", label: "Selic target", color: "var(--chart-1)", kind: "area", axis: "left", unit: "%", digits: 2 },
              { key: "usd_brl", label: "USD/BRL", color: "var(--chart-2)", axis: "right", digits: 4 },
            ]}
          />
        </Panel>

        <Panel
          style={{ animationDelay: "360ms" }}
          className="lg:col-span-2"
          title="Inflation pulse"
          subtitle="Monthly IPCA (% m/m). Seasonality and the 2002, 2015 and 2021–22 spikes are visible."
        >
          <LinePanelChart
            data={macro}
            xKey="month"
            xType="month"
            height={260}
            leftUnit="% m/m"
            series={[
              { key: "ipca_mom", label: "IPCA m/m", color: "var(--chart-4)", kind: "area", digits: 3, unit: "%" },
            ]}
          />
        </Panel>

        <Panel
          style={{ animationDelay: "420ms" }}
          title="Public debt"
          subtitle="Gross general government debt, % of GDP."
        >
          <LinePanelChart
            data={macro}
            xKey="month"
            xType="month"
            height={260}
            leftUnit="% GDP"
            series={[
              { key: "debt_to_gdp_pct", label: "Debt/GDP", color: "var(--chart-3)", kind: "area", digits: 1, unit: "%" },
            ]}
          />
        </Panel>
      </div>

      <p className="rise mt-6 text-xs text-muted-foreground" style={{ animationDelay: "480ms" }}>
        Latest reading {fmtPct(last.selic_target, 2)} Selic · IPCA 12m {fmtPct(lastReal.ipca_accum_12m, 2)} ·
        USD/BRL {fmtNum(last.usd_brl, 4)} · debt {fmtPct(last.debt_to_gdp_pct, 1)} of GDP. Source:{" "}
        <span className="font-mono text-foreground/70">gold/mart_macro_dashboard</span>.
      </p>
    </div>
  );
}
