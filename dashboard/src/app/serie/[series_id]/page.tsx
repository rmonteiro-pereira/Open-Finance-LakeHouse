import { DistributionStrip, SeriesLine } from "@/components/chart";
import { Stat, unitLabel } from "@/components/slot";
import { getMeta, getObservations, getPercentiles, latest } from "@/lib/release";

export const dynamic = "force-static";

/**
 * The atomic unit of the product: one series, one permalink, one place to send someone.
 *
 * A series had no address before this page existed — the site was organised by mart, so
 * the smallest citable thing was a chart on a dashboard. Provenance, unit and licence all
 * live here, next to the number rather than in a README nobody opens.
 */
export async function generateStaticParams() {
  const meta = await getMeta();
  return meta.series.map((s) => ({ series_id: s.series_id }));
}

export default async function Page({ params }: { params: Promise<{ series_id: string }> }) {
  const { series_id } = await params;
  const meta = await getMeta();
  const series = meta.series.find((s) => s.series_id === series_id);
  if (!series) {
    return (
      <div className="space-y-4">
        <h1 className="font-[family-name:var(--font-display)] text-4xl tracking-tight">Série desconhecida</h1>
        <p className="text-sm text-[var(--ink-muted)]">
          <code>{series_id}</code> não está no catálogo deste release.
        </p>
      </div>
    );
  }

  const now = await latest(series_id);
  const history = (await getObservations())
    .filter((o) => o.series_id === series_id)
    .sort((a, b) => (a.date < b.date ? 1 : -1));
  const rank = (await getPercentiles())
    .filter((p) => p.series_id === series_id && p.window_label === "10y" && p.percentile_allowed)
    .sort((a, b) => (a.date < b.date ? 1 : -1))[0];

  const asc = [...history].reverse();
  // The window BEHIND the percentile. Publishing the rank without it asks for trust the
  // product has not earned, on a site whose whole rule is that a number carries its proof.
  const windowValues = rank
    ? asc.filter((o) => o.date > rank.window_start && o.date <= rank.date).map((o) => o.value)
    : [];

  return (
    <div className="space-y-12">
      <header className="space-y-1">
        <h1 className="font-[family-name:var(--font-display)] text-4xl tracking-tight">{series.name}</h1>
        <p className="text-sm text-[var(--ink-muted)]">
          <code>{series.series_id}</code> · {series.domain} · {series.frequency}
        </p>
      </header>

      <Stat label="Última observação" value={now?.value} meta={series} />

      {asc.length > 1 ? (
        <section className="space-y-2">
          <h2 className="rule-label">Histórico</h2>
          <SeriesLine points={asc} meta={series} unitSuffix={unitLabel(series)} />
        </section>
      ) : null}

      <section className="space-y-2">
        <h2 className="rule-label">O que este número é</h2>
        <dl className="grid gap-x-8 gap-y-1 text-sm sm:grid-cols-2">
          <div>
            <dt className="inline text-[var(--ink-muted)]">Unidade: </dt>
            <dd className="inline tnum">{unitLabel(series) || series.unit}</dd>
          </div>
          <div>
            <dt className="inline text-[var(--ink-muted)]">Base: </dt>
            <dd className="inline tnum">{series.basis ?? "—"}</dd>
          </div>
          <div>
            <dt className="inline text-[var(--ink-muted)]">Escala: </dt>
            <dd className="inline tnum">{series.scale}</dd>
          </div>
          <div>
            <dt className="inline text-[var(--ink-muted)]">Contagem de dias: </dt>
            <dd className="inline tnum">{series.day_count ?? "—"}</dd>
          </div>
          <div>
            <dt className="inline text-[var(--ink-muted)]">Horizonte: </dt>
            <dd className="inline tnum">{series.horizon ?? "—"}</dd>
          </div>
          <div>
            <dt className="inline text-[var(--ink-muted)]">Fonte: </dt>
            <dd className="inline tnum">{series.provider}</dd>
          </div>
        </dl>
        <p className="text-xs text-[var(--ink-muted)]">
          A unidade é o par completo, não só o símbolo: <code>percent</code> sozinho cobria a Selic
          diária, a variação mensal do IPCA, o desemprego e a dívida/PIB.
        </p>
      </section>

      {rank ? (
        <section data-slot="percentile" className="space-y-3">
          <h2 className="rule-label">Onde este número cai</h2>
          <p className="max-w-[68ch] text-sm">
            Percentil <strong>{(rank.pct_rank * 100).toFixed(0)}</strong> da janela de 10 anos, por
            mid-rank sobre {rank.n_obs} observações desde{" "}
            <span className="tnum">{rank.window_start}</span>.
          </p>
          <DistributionStrip
            values={windowValues}
            current={now?.value ?? 0}
            windowLabel="de 10 anos"
          />
        </section>
      ) : null}

      <section className="space-y-2">
        <h2 className="rule-label">
          Observações ({history.length})
        </h2>
        <table className="ledger max-w-md">
          <tbody>
            {history.slice(0, 24).map((o) => (
              <tr key={o.date}>
                <td className="tnum text-[var(--ink-muted)]">{o.date}</td>
                <td className="num tnum">{o.value}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </div>
  );
}
