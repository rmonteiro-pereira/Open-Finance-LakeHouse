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
      <main className="p-8">
        <h1 className="font-serif text-3xl">Série desconhecida</h1>
        <p className="text-sm text-muted-foreground">
          <code>{series_id}</code> não está no catálogo deste release.
        </p>
      </main>
    );
  }

  const now = await latest(series_id);
  const history = (await getObservations())
    .filter((o) => o.series_id === series_id)
    .sort((a, b) => (a.date < b.date ? 1 : -1));
  const rank = (await getPercentiles())
    .filter((p) => p.series_id === series_id && p.window_label === "10y" && p.percentile_allowed)
    .sort((a, b) => (a.date < b.date ? 1 : -1))[0];

  return (
    <main className="space-y-8 p-8">
      <header className="space-y-1">
        <h1 className="font-serif text-3xl">{series.name}</h1>
        <p className="text-sm text-muted-foreground">
          <code>{series.series_id}</code> · {series.domain} · {series.frequency}
        </p>
      </header>

      <Stat label="Última observação" value={now?.value} meta={series} />

      <section className="space-y-2">
        <h2 className="text-sm uppercase tracking-wide text-muted-foreground">O que este número é</h2>
        <dl className="grid gap-x-8 gap-y-1 text-sm sm:grid-cols-2">
          <div>
            <dt className="inline text-muted-foreground">Unidade: </dt>
            <dd className="inline font-mono">{unitLabel(series) || series.unit}</dd>
          </div>
          <div>
            <dt className="inline text-muted-foreground">Base: </dt>
            <dd className="inline font-mono">{series.basis ?? "—"}</dd>
          </div>
          <div>
            <dt className="inline text-muted-foreground">Escala: </dt>
            <dd className="inline font-mono">{series.scale}</dd>
          </div>
          <div>
            <dt className="inline text-muted-foreground">Contagem de dias: </dt>
            <dd className="inline font-mono">{series.day_count ?? "—"}</dd>
          </div>
          <div>
            <dt className="inline text-muted-foreground">Horizonte: </dt>
            <dd className="inline font-mono">{series.horizon ?? "—"}</dd>
          </div>
          <div>
            <dt className="inline text-muted-foreground">Fonte: </dt>
            <dd className="inline font-mono">{series.provider}</dd>
          </div>
        </dl>
        <p className="text-xs text-muted-foreground">
          A unidade é o par completo, não só o símbolo: <code>percent</code> sozinho cobria a Selic
          diária, a variação mensal do IPCA, o desemprego e a dívida/PIB.
        </p>
      </section>

      {rank ? (
        <section data-slot="percentile" className="text-sm">
          Percentil <strong>{(rank.pct_rank * 100).toFixed(0)}</strong> da janela de 10 anos, por
          mid-rank sobre {rank.n_obs} observações.
        </section>
      ) : null}

      <section className="space-y-2">
        <h2 className="text-sm uppercase tracking-wide text-muted-foreground">
          Observações ({history.length})
        </h2>
        <table className="w-full max-w-md text-sm">
          <tbody className="font-mono tabular-nums">
            {history.slice(0, 24).map((o) => (
              <tr key={o.date}>
                <td className="py-0.5">{o.date}</td>
                <td className="text-right">{o.value}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </main>
  );
}
