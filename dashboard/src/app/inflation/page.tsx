import { PageHeader } from "@/components/page-header";
import { Panel } from "@/components/panel";
import { StatCard } from "@/components/stat-card";
import { LinePanelChart } from "@/components/charts/line-panel";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { getInflation } from "@/lib/data";
import { fmtNum, fmtSigned } from "@/lib/format";

export const dynamic = "force-dynamic";

export default async function InflationPage() {
  const infl = await getInflation();
  const last = infl[infl.length - 1];
  const prev = infl[infl.length - 2];
  const tail = infl.slice(-24);

  return (
    <div className="mx-auto max-w-[1200px] px-5 py-8 md:px-8 lg:py-10">
      <PageHeader
        kicker="Brazil · Inflation"
        title="Every index, side by side."
        lede="Consumer gauges (IPCA, IPCA-15, INPC) and the wholesale-heavy IGP family. The IGP indices swing harder — they carry FX pass-through and commodity prices."
        asOf={last.month}
      />

      <div className="mb-6 grid grid-cols-1 gap-3 sm:grid-cols-3">
        <StatCard
          featured
          style={{ animationDelay: "60ms" }}
          label="IPCA · m/m"
          value={fmtNum(last.ipca_mom, 3)}
          unit="%"
          delta={last.ipca_mom - prev.ipca_mom}
          deltaLabel={`${fmtSigned(last.ipca_mom - prev.ipca_mom, 3)} m/m`}
          series={tail.map((r) => r.ipca_mom)}
          accent="var(--chart-4)"
          invertDelta
        />
        <StatCard
          style={{ animationDelay: "120ms" }}
          label="IGP-M · m/m"
          value={fmtNum(last.igpm_mom, 3)}
          unit="%"
          delta={last.igpm_mom - prev.igpm_mom}
          deltaLabel={`${fmtSigned(last.igpm_mom - prev.igpm_mom, 3)} m/m`}
          series={tail.map((r) => r.igpm_mom)}
          accent="var(--chart-3)"
          invertDelta
        />
        <StatCard
          style={{ animationDelay: "180ms" }}
          label="IGP-M · 12m"
          value={fmtNum(last.igpm_12m, 2)}
          unit="%"
          delta={last.igpm_12m - prev.igpm_12m}
          deltaLabel={`${fmtSigned(last.igpm_12m - prev.igpm_12m, 2)} m/m`}
          series={tail.map((r) => r.igpm_12m)}
          accent="var(--chart-7)"
          invertDelta
        />
      </div>

      <Panel style={{ animationDelay: "260ms" }} bodyClassName="px-3 pb-3 pt-3 sm:px-4">
        <Tabs defaultValue="consumer">
          <div className="flex items-center justify-between gap-4 px-1 pb-3">
            <div>
              <h2 className="text-sm font-semibold tracking-tight">Inflation indices (% m/m)</h2>
              <p className="mt-0.5 text-xs text-muted-foreground">
                Monthly prints. Switch between the consumer basket and the IGP family.
              </p>
            </div>
            <TabsList>
              <TabsTrigger value="consumer">Consumer</TabsTrigger>
              <TabsTrigger value="igp">IGP family</TabsTrigger>
            </TabsList>
          </div>

          <TabsContent value="consumer">
            <LinePanelChart
              data={infl}
              xKey="month"
              xType="month"
              height={360}
              leftUnit="% m/m"
              series={[
                { key: "ipca15_mom", label: "IPCA-15", color: "var(--chart-1)", digits: 3, unit: "%", width: 1.25, opacity: 0.5 },
                { key: "inpc_mom", label: "INPC", color: "var(--chart-2)", digits: 3, unit: "%", width: 1.25, opacity: 0.5 },
                { key: "ipca_mom", label: "IPCA", color: "var(--chart-4)", digits: 3, unit: "%", width: 2.5 },
              ]}
            />
          </TabsContent>

          <TabsContent value="igp">
            <LinePanelChart
              data={infl}
              xKey="month"
              xType="month"
              height={360}
              leftUnit="% m/m"
              rightUnit="% 12m"
              series={[
                { key: "igpm_12m", label: "IGP-M 12m", color: "var(--chart-7)", kind: "area", axis: "right", digits: 2, unit: "%", opacity: 0.85 },
                { key: "igpdi_mom", label: "IGP-DI m/m", color: "var(--chart-8)", digits: 3, unit: "%", width: 1.25, opacity: 0.45 },
                { key: "igpm_mom", label: "IGP-M m/m", color: "var(--chart-3)", digits: 3, unit: "%", width: 2 },
              ]}
            />
          </TabsContent>
        </Tabs>
      </Panel>
    </div>
  );
}
