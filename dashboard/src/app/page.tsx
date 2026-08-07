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
    <div className="space-y-12">
      <header className="space-y-3">
        <h1 className="font-[family-name:var(--font-display)] text-5xl tracking-tight">Como está o Brasil hoje?</h1>
        <p className="max-w-[68ch] text-[var(--ink-muted)]">
          Do release <code>{meta.release_id}</code>. Cada número leva à série que o produz — e cada
          um traz a data do dado, não a data da página.
        </p>
      </header>

      <section className="grid gap-x-10 gap-y-10 sm:grid-cols-2 lg:grid-cols-3">
        {cards
          .filter((c) => c.meta)
          .map((c) => (
            <Link key={c.id} href={`/serie/${c.id}`} className="block no-underline">
              <Stat label={c.meta!.name} value={c.obs?.value} meta={c.meta} />
            </Link>
          ))}
      </section>

      <p className="text-xs text-[var(--ink-muted)]">
        Séries de fontes que não permitem redistribuição aparecem no catálogo com o motivo, e não
        aqui — ver <Link href="/confianca" className="underline">Confiança</Link>.
      </p>
    </div>
  );
}
