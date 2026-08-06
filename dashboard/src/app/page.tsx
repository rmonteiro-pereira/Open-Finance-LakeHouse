import Link from "next/link";

import { Stat } from "@/components/slot";
import { getMeta, latest, seriesMeta } from "@/lib/release";

export const dynamic = "force-static";

/** The masthead: five numbers and a date. Every one links to the series behind it. */
const HEADLINE = ["selic_meta", "ipca", "usd_brl", "divida_pib", "focus_ipca_12m"];

export default async function Page() {
  const meta = await getMeta();
  const cards = await Promise.all(
    HEADLINE.map(async (id) => ({
      id,
      meta: await seriesMeta(id),
      obs: await latest(id),
    })),
  );

  return (
    <main className="space-y-8 p-8">
      <header className="space-y-1">
        <h1 className="font-serif text-4xl">Como está o Brasil hoje?</h1>
        <p className="text-sm text-muted-foreground">
          Do release <code>{meta.release_id}</code>. Cada número leva à série que o produz — e cada
          um traz a data do dado, não a data da página.
        </p>
      </header>

      <section className="grid gap-8 sm:grid-cols-2 lg:grid-cols-3">
        {cards
          .filter((c) => c.meta)
          .map((c) => (
            <Link key={c.id} href={`/serie/${c.id}`} className="block hover:opacity-80">
              <Stat label={c.meta!.name} value={c.obs?.value} meta={c.meta} />
            </Link>
          ))}
      </section>

      <p className="text-xs text-muted-foreground">
        Séries de fontes que não permitem redistribuição aparecem no catálogo com o motivo, e não
        aqui — ver <Link href="/confianca" className="underline">Confiança</Link>.
      </p>
    </main>
  );
}
