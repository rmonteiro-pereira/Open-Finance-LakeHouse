import Link from "next/link";

import { getMeta, freshness, STATE_LABEL, type SeriesMeta } from "@/lib/release";

export const dynamic = "force-static";

/**
 * "Dá pra confiar hoje, e como eu pego isso?"
 *
 * Generated ENTIRELY from the manifest: not one hand-written figure. That is what makes
 * the page incapable of lying, because no code path exists that could improve the verdict
 * by editing the page.
 *
 * The design decision that matters here is what gets colour. The first version listed all
 * 51 registered series flat, with a red dot on every one that carried no observation —
 * 44 of 51, including every series barred by a written licence verdict. A page that is
 * 86% red has taught its reader to ignore red, which costs exactly the signal it exists
 * to carry. Now `late` is the only alarm, and the other three states are grouped by the
 * REASON they are absent, because withheld, unverified and not-ingested are three
 * different facts and none of them is a failure.
 */
const GROUPS = [
  {
    key: "published",
    title: "Publicadas",
    blurb: "No release, com observação. Estas são as únicas que podem estar atrasadas.",
  },
  {
    key: "not_ingested",
    title: "Não ingeridas neste release",
    blurb: "Registradas e liberadas por licença, sem observação neste corpus.",
  },
  {
    key: "withheld_licence",
    title: "Vedadas por veredito de licença",
    blurb: "O titular não permite redistribuição. Ficam no catálogo com o motivo.",
  },
  {
    key: "withheld_unverified",
    title: "Sem veredito de licença",
    blurb: "Termo de uso não auditado, e o silêncio conta como vermelho.",
  },
] as const;

export default async function Page() {
  const meta = await getMeta();
  const failed = meta.gates.filter((g) => g.status !== "pass");
  const by = (k: string) => meta.series.filter((s) => s.presence === k);

  const published = by("published");
  const late = published.filter((s) => freshness(s).state === "late");
  const rows = published.length;

  return (
    <div className="space-y-14">
      <header className="space-y-3">
        <h1 className="font-[family-name:var(--font-display)] text-4xl tracking-tight">
          Dá pra confiar hoje?
        </h1>
        <p className="max-w-[68ch] text-[var(--ink-muted)]">
          Release <span className="tnum">{meta.release_id}</span>, classe {meta.release_class},
          gerado em <span className="tnum">{meta.generated_at.slice(0, 16).replace("T", " ")}</span>.
          Tudo nesta página vem do manifesto.
        </p>
      </header>

      {/* The verdict, before the detail. A reader who stops here has the answer. */}
      <section aria-labelledby="veredito" className="space-y-4">
        <h2 id="veredito" className="rule-label">
          Veredito
        </h2>
        <dl className="grid grid-cols-2 gap-x-8 gap-y-6 sm:grid-cols-4">
          <Figure
            label="Portões"
            value={failed.length === 0 ? `${meta.gates.length}/${meta.gates.length}` : `${failed.length}`}
            note={failed.length === 0 ? "todos passaram" : `reprovaram: ${failed.map((g) => g.name).join(", ")}`}
            tone={failed.length === 0 ? "ok" : "late"}
            slot="gates"
            extra={{ "data-failed": failed.length }}
          />
          <Figure
            label="Séries publicadas"
            value={`${published.length}`}
            note={`de ${meta.series.length} registradas`}
          />
          <Figure
            label="Atrasadas"
            value={`${late.length}`}
            note={late.length === 0 ? "nenhuma fora do orçamento" : late.map((s) => s.series_id).join(", ")}
            tone={late.length === 0 ? "ok" : "late"}
            slot="late-count"
          />
          <Figure
            label="Tabelas"
            value={`${meta.tables.length}`}
            note={`${meta.tables.reduce((a, t) => a + (t.rows ?? 0), 0).toLocaleString("pt-BR")} linhas`}
          />
        </dl>
        {rows === 0 ? (
          <p className="text-sm text-[var(--late)]">
            Nenhuma série publicada. Um release sem observação não é um release fresco: é um
            release vazio.
          </p>
        ) : null}
      </section>

      <section aria-labelledby="tabelas" className="space-y-4">
        <h2 id="tabelas" className="rule-label">
          Tabelas
        </h2>
        <table className="ledger">
          <thead>
            <tr>
              <th>Tabela</th>
              <th>Chave primária</th>
              <th>Estado</th>
              <th className="num">Linhas</th>
            </tr>
          </thead>
          <tbody>
            {meta.tables.map((t) => (
              <tr key={t.name} data-slot="table-status" data-status={t.status}>
                <td className="tnum">{t.name}</td>
                <td className="text-[var(--ink-muted)]">
                  <span className="tnum text-xs">{t.primary_key.join(" · ")}</span>
                </td>
                <td className="text-[var(--ink-muted)]">{t.status}</td>
                <td className="num tnum">{t.rows?.toLocaleString("pt-BR") ?? "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section aria-labelledby="series" className="space-y-10">
        <div className="space-y-2">
          <h2 id="series" className="rule-label">
            Séries
          </h2>
          <p className="max-w-[68ch] text-sm text-[var(--ink-muted)]">
            Agrupadas pelo motivo de estarem, ou não, neste release. Só séries publicadas podem
            estar atrasadas, e atraso é a única coisa aqui que é uma falha.
          </p>
        </div>

        {GROUPS.map((group) => {
          const items = by(group.key);
          if (items.length === 0) return null;
          return (
            <div key={group.key} className="space-y-3">
              <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
                <h3 className="text-sm font-medium">{group.title}</h3>
                <span className="tnum text-xs text-[var(--ink-muted)]">{items.length}</span>
                <p className="text-xs text-[var(--ink-muted)]">{group.blurb}</p>
              </div>
              <SeriesTable items={items} showDate={group.key === "published"} />
            </div>
          );
        })}
      </section>

      <section aria-labelledby="pegar" className="space-y-4">
        <h2 id="pegar" className="rule-label">
          Como eu pego isso
        </h2>
        <pre className="overflow-x-auto rounded-sm bg-[var(--paper-sunk)] p-4 text-xs">
          <code className="tnum">
            {`SELECT * FROM 'parquet/fact_observation.parquet' LIMIT 5;`}
          </code>
        </pre>
        <p className="max-w-[68ch] text-sm text-[var(--ink-muted)]">
          Pinar um <span className="tnum">release_id</span> é o caminho suportado.{" "}
          <span className="tnum">latest</span> existe para exploração e não é reprodutível.
        </p>
      </section>
    </div>
  );
}

function Figure({
  label,
  value,
  note,
  tone,
  slot,
  extra,
}: {
  label: string;
  value: string;
  note?: string;
  tone?: "ok" | "late";
  slot?: string;
  extra?: Record<string, unknown>;
}) {
  return (
    <div data-slot={slot} {...extra} className="space-y-1">
      <dt className="text-xs text-[var(--ink-muted)]">{label}</dt>
      <dd
        className="tnum text-3xl tracking-tight"
        style={tone ? { color: `var(--${tone})` } : undefined}
      >
        {value}
      </dd>
      {note ? <p className="text-xs text-[var(--ink-muted)]">{note}</p> : null}
    </div>
  );
}

function SeriesTable({ items, showDate }: { items: SeriesMeta[]; showDate: boolean }) {
  return (
    <table className="ledger">
      <thead>
        <tr>
          <th>Série</th>
          <th className="hidden sm:table-cell">Domínio</th>
          <th className="hidden md:table-cell">Fonte</th>
          <th className="num">{showDate ? "Último dado" : "Motivo"}</th>
        </tr>
      </thead>
      <tbody>
        {items.map((s) => {
          const f = freshness(s);
          return (
            <tr key={s.series_id}>
              <td>
                <Link href={`/serie/${s.series_id}`} className="tnum text-xs">
                  {s.series_id}
                </Link>
              </td>
              <td className="hidden text-[var(--ink-muted)] sm:table-cell">{s.domain}</td>
              <td className="hidden text-[var(--ink-muted)] md:table-cell">
                {s.rights_holder ?? s.provider}
              </td>
              <td className="num">
                {showDate ? (
                  <span
                    className="tnum text-xs"
                    style={{ color: f.state === "late" ? "var(--late)" : undefined }}
                    data-state={f.state}
                  >
                    {f.asOf ?? "—"}
                  </span>
                ) : (
                  <span className="text-xs text-[var(--ink-muted)]" data-state={f.state}>
                    {STATE_LABEL[f.state]}
                  </span>
                )}
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}
