import { FreshnessChip } from "@/components/slot";
import { getMeta } from "@/lib/release";

export const dynamic = "force-static";

/**
 * "Dá pra confiar hoje, e como eu pego isso?"
 *
 * Generated ENTIRELY from the manifest — not one hand-written figure on the page. That is
 * what makes it incapable of lying: no code path exists that could fix the colour by
 * editing the page. A pipeline outside its budget shows red in public, and that is the
 * feature, not the embarrassment.
 */
export default async function Page() {
  const meta = await getMeta();
  const failed = meta.gates.filter((g) => g.status !== "pass");

  return (
    <main className="space-y-8 p-8">
      <header className="space-y-1">
        <h1 className="font-serif text-3xl">Dá pra confiar hoje?</h1>
        <p className="text-sm text-muted-foreground">
          Release <code>{meta.release_id}</code> ({meta.release_class}), gerado em {meta.generated_at}.
          Esta página é gerada inteiramente do manifesto.
        </p>
      </header>

      <section className="space-y-2">
        <h2 className="text-sm uppercase tracking-wide text-muted-foreground">Portões</h2>
        <p data-slot="gates" data-failed={failed.length}>
          {failed.length === 0
            ? `${meta.gates.length} portões passaram.`
            : `${failed.length} de ${meta.gates.length} portões reprovaram: ${failed
                .map((g) => g.name)
                .join(", ")}.`}
        </p>
      </section>

      <section className="space-y-2">
        <h2 className="text-sm uppercase tracking-wide text-muted-foreground">Tabelas</h2>
        <ul className="space-y-1 text-sm">
          {meta.tables.map((t) => (
            <li key={t.name} data-slot="table-status" data-status={t.status}>
              <code>{t.name}</code> — {t.status}
              {t.rows !== null ? `, ${t.rows.toLocaleString("pt-BR")} linhas` : ""}, chave{" "}
              <code>{t.primary_key.join(" + ")}</code>
            </li>
          ))}
        </ul>
      </section>

      <section className="space-y-2">
        <h2 className="text-sm uppercase tracking-wide text-muted-foreground">Frescor por série</h2>
        <ul className="grid gap-1 text-sm sm:grid-cols-2">
          {meta.series.map((s) => (
            <li key={s.series_id} className="flex items-center justify-between gap-4">
              <code className="text-xs">{s.series_id}</code>
              <FreshnessChip meta={s} />
            </li>
          ))}
        </ul>
        <p className="text-xs text-muted-foreground">
          O veredito é calculado aqui, a partir de <code>last_observation_date</code> e do orçamento
          declarado. O manifesto publica os insumos e nunca o veredito: um manifesto de três dias
          atrás continua respondendo vermelho corretamente, sem ser republicado.
        </p>
      </section>

      <section className="space-y-2">
        <h2 className="text-sm uppercase tracking-wide text-muted-foreground">Como eu pego isso</h2>
        <pre className="overflow-x-auto rounded bg-muted p-3 text-xs">
          {`SELECT * FROM 'parquet/fact_observation.parquet' LIMIT 5;`}
        </pre>
        <p className="text-xs text-muted-foreground">
          Pinar um <code>release_id</code> é o caminho suportado; <code>latest</code> existe para
          exploração e não é reprodutível.
        </p>
      </section>
    </main>
  );
}
