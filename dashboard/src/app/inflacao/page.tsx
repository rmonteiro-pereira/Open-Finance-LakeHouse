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

  return (
    <main className="space-y-8 p-8">
      <header className="space-y-1">
        <h1 className="font-serif text-3xl">A inflação está cedendo?</h1>
        <p className="text-sm text-muted-foreground">
          IPCA mensal e o acumulado de 12 meses. Release <code>{meta.release_id}</code>.
        </p>
      </header>

      <section className="grid gap-8 sm:grid-cols-2">
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
    </main>
  );
}
