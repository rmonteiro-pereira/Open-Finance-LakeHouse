import { PageHeader } from "@/components/page-header";
import { Panel } from "@/components/panel";
import { StatCard } from "@/components/stat-card";
import { LinePanelChart } from "@/components/charts/line-panel";
import { getFx } from "@/lib/data";
import { fmtNum, fmtSignedPct } from "@/lib/format";

export const dynamic = "force-dynamic";

export default async function FxPage() {
  const fx = await getFx();

  // pivot long → wide by date
  const byDate = new Map<string, Record<string, number | string>>();
  for (const r of fx) {
    const row = byDate.get(r.date) ?? { date: r.date };
    row[`${r.series_id}_rate`] = r.rate;
    row[`${r.series_id}_vol`] = r.vol_21d;
    byDate.set(r.date, row);
  }
  const wide = [...byDate.values()].sort((a, b) => String(a.date).localeCompare(String(b.date)));

  const usd = fx.filter((r) => r.series_id === "usd_brl");
  const eur = fx.filter((r) => r.series_id === "eur_brl");
  const lastUsd = usd[usd.length - 1];
  const lastEur = eur[eur.length - 1];
  const asOf = lastUsd.date;

  return (
    <div className="mx-auto max-w-[1200px] px-5 py-8 md:px-8 lg:py-10">
      <PageHeader
        kicker="Brazil · FX"
        title="The real, under pressure."
        lede="USD/BRL and EUR/BRL — daily levels, the latest moves, and 21-day realised volatility. Lower is a stronger real."
        asOf={asOf}
      />

      <div className="mb-6 grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <StatCard
          featured
          style={{ animationDelay: "60ms" }}
          label="USD / BRL"
          value={fmtNum(lastUsd.rate, 4)}
          delta={lastUsd.daily_return_pct}
          deltaLabel={`${fmtSignedPct(lastUsd.daily_return_pct, 2)} d/d`}
          series={usd.slice(-60).map((r) => r.rate)}
          accent="var(--chart-1)"
          invertDelta
        />
        <StatCard
          style={{ animationDelay: "120ms" }}
          label="EUR / BRL"
          value={fmtNum(lastEur.rate, 4)}
          delta={lastEur.daily_return_pct}
          deltaLabel={`${fmtSignedPct(lastEur.daily_return_pct, 2)} d/d`}
          series={eur.slice(-60).map((r) => r.rate)}
          accent="var(--chart-2)"
          invertDelta
        />
        <StatCard
          style={{ animationDelay: "180ms" }}
          label="USD/BRL · 21d vol"
          value={fmtNum(lastUsd.vol_21d, 2)}
          unit="% ann."
          series={usd.slice(-60).map((r) => r.vol_21d)}
          accent="var(--chart-4)"
        />
        <StatCard
          style={{ animationDelay: "240ms" }}
          label="USD/BRL · MTD"
          value={fmtSignedPct(lastUsd.mtd_return_pct, 2)}
          series={usd.slice(-60).map((r) => r.mtd_return_pct)}
          accent="var(--chart-3)"
          invertDelta
        />
      </div>

      <div className="grid grid-cols-1 gap-4">
        <Panel
          style={{ animationDelay: "300ms" }}
          title="Exchange-rate levels"
          subtitle="Daily close, BRL per unit of foreign currency."
        >
          <LinePanelChart
            data={wide}
            xKey="date"
            xType="date"
            height={320}
            leftUnit="BRL"
            series={[
              { key: "usd_brl_rate", label: "USD/BRL", color: "var(--chart-1)", digits: 4 },
              { key: "eur_brl_rate", label: "EUR/BRL", color: "var(--chart-2)", digits: 4 },
            ]}
          />
        </Panel>

        <Panel
          style={{ animationDelay: "360ms" }}
          title="Realised volatility (21-day, annualised)"
          subtitle="Rolling standard deviation of daily returns — risk regime, not direction."
        >
          <LinePanelChart
            data={wide}
            xKey="date"
            xType="date"
            height={240}
            leftUnit="% ann."
            series={[
              { key: "usd_brl_vol", label: "USD/BRL vol", color: "var(--chart-4)", kind: "area", digits: 2, unit: "%", width: 2 },
              { key: "eur_brl_vol", label: "EUR/BRL vol", color: "var(--chart-3)", digits: 2, unit: "%", width: 1.25, opacity: 0.5 },
            ]}
          />
        </Panel>
      </div>
    </div>
  );
}
