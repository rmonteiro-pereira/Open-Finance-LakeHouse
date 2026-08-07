import { Columns, SeriesLine } from "@/components/chart";
import { Stat } from "@/components/slot";
import { getMeta, getObservations, seriesMeta } from "@/lib/release";

export const dynamic = "force-static";

/** "A inflação está cedendo?" — the month, and the twelve months, with the basis declared. */
export default async function Page() {
  const meta = await getMeta();
  const ipca = await seriesMeta("ipca");
  const obs = (await getObservations())
    .filter((o) => o.series_id === "ipca")
    .sort((a, b) => (a.date < b.date ? 1 : -1));

  const last12 = obs.slice(0, 12);
  // Compounded, not summed. Twelve monthly variations do not add up, and the difference
  // is visible at Brazilian levels.
  const accum =
    last12.length === 12 ? (last12.reduce((acc, o) => acc * (1 + o.value / 100), 1) - 1) * 100 : null;

  // The rolling 12-month accumulation, compounded at every point. Twelve monthly
  // variations do not add up, and at Brazilian levels the difference is visible.
  const asc = [...obs].reverse();
  const accum12 = asc
    .map((o, i) =>
      i < 11
        ? null
        : {
            date: o.date,
            value: (asc.slice(i - 11, i + 1).reduce((a, r) => a * (1 + r.value / 100), 1) - 1) * 100,
          },
    )
    .filter((x): x is { date: string; value: number } => x !== null);

  return (
    <div className="space-y-12">
      <header className="space-y-3">
        <h1 className="font-[family-name:var(--font-display)] text-4xl tracking-tight">A inflação está cedendo?</h1>
        <p className="max-w-[68ch] text-[var(--ink-muted)]">
          IPCA mensal e o acumulado de 12 meses. Release <code>{meta.release_id}</code>.
        </p>
      </header>

      <section className="grid gap-x-10 gap-y-8 sm:grid-cols-2">
        <Stat label="IPCA no mês" value={obs[0]?.value} meta={ipca} />
        <Stat
          label="IPCA acumulado 12m"
          value={accum}
          meta={ipca}
          suffix="%"
          note={
            accum === null
              ? "menos de 12 observações na janela — não acumulado"
              : "composto, não somado"
          }
        />
      </section>

      {/* SMALL MULTIPLES, not a second y-axis. Monthly variation and a 12-month
          accumulation live on scales an order of magnitude apart, and putting them on one
          plot invents a correlation the data has not got. */}
      <section className="space-y-8">
        <div className="space-y-2">
          <h2 className="rule-label">Variação mensal</h2>
          <Columns points={[...obs].reverse().slice(-36)} unitSuffix="%" />
        </div>
        <div className="space-y-2">
          <h2 className="rule-label">Acumulado em 12 meses</h2>
          {accum12 .length > 1 ? (
            <SeriesLine points={accum12} meta={ipca} unitSuffix="%" />
          ) : (
            <p className="text-sm text-[var(--ink-muted)]">
              Menos de 13 observações: não há janela de 12 meses fechada para acumular.
            </p>
          )}
        </div>
      </section>
    </div>
  );
}
