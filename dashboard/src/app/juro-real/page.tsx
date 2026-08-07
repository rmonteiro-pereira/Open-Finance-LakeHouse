import { Stat } from "@/components/slot";
import { getMeta, getPercentiles, latest, seriesMeta } from "@/lib/release";

export const dynamic = "force-static";

/**
 * "O juro real está alto?"
 *
 * The opening is assembled from MARKED SLOTS, never written as free prose. A slot is
 * assertable; a generated sentence is a fluent way to be wrong, and an earlier draft of
 * the RFC admitted that fragility before this rule replaced it.
 */
export default async function Page() {
  const meta = await getMeta();
  const selic = await seriesMeta("selic_meta");
  const focus = await seriesMeta("focus_ipca_12m");
  const selicNow = await latest("selic_meta");
  const focusNow = await latest("focus_ipca_12m");
  const ranked = (await getPercentiles())
    .filter((p) => p.series_id === "selic_meta" && p.window_label === "10y" && p.percentile_allowed)
    .sort((a, b) => (a.date < b.date ? 1 : -1));
  const rank = ranked[0];

  // Fisher exact — the same convention the mart publishes. The subtraction shorthand
  // differs by tens of basis points at Brazilian levels, so "which one" is not a detail.
  const real =
    selicNow && focusNow ? ((1 + selicNow.value / 100) / (1 + focusNow.value / 100) - 1) * 100 : null;

  return (
    <div className="space-y-12">
      <header className="space-y-3">
        <h1 className="font-[family-name:var(--font-display)] text-4xl tracking-tight">O juro real está alto?</h1>
        <p className="max-w-[68ch] text-[var(--ink-muted)]">
          Selic vigente deflacionada pela expectativa de IPCA 12m da Focus — ex-ante, Fisher exato.
          Release <code>{meta.release_id}</code>.
        </p>
      </header>

      <section className="grid gap-x-10 gap-y-8 sm:grid-cols-3">
        <Stat label="Juro real ex-ante" value={real} meta={selic} suffix="% a.a." />
        <Stat label="Selic (meta)" value={selicNow?.value} meta={selic} />
        <Stat label="IPCA esperado 12m" value={focusNow?.value} meta={focus} />
      </section>

      <section data-slot="percentile" className="text-sm">
        {rank ? (
          <p>
            Percentil <strong data-slot="pct-rank">{(rank.pct_rank * 100).toFixed(0)}</strong> da janela
            de 10 anos, sobre {rank.n_obs} observações, por mid-rank.
          </p>
        ) : (
          <p className="text-[var(--ink-muted)]">
            História insuficiente para um percentil de 10 anos — o número não é publicado, em vez de
            ser estimado.
          </p>
        )}
      </section>
    </div>
  );
}
