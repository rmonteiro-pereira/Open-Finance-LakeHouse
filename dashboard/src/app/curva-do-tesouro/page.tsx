import { getMeta, getTreasury } from "@/lib/release";

export const dynamic = "force-static";

/**
 * "Vale a pena travar IPCA+ hoje?"
 *
 * Keyed by `instrument_id`, never by a bond-type bucket. The bucket merges the zero-coupon
 * and the semiannual-coupon bond of one index at the same maturity — the defect the grain
 * correction exists to remove, and the one a first attempt at that correction reintroduced.
 *
 * This is the TESOURO curve, not the DI curve. For a retail question it is also the right
 * one — it is the price the investor actually pays — and it is the only one publishable at
 * all: B3 barred redistribution of derived values absent written authorisation.
 */
export default async function Page() {
  const meta = await getMeta();
  const rows = await getTreasury();
  const refDate = rows.map((r) => r.date).sort().at(-1);
  const today = rows
    .filter((r) => r.date === refDate)
    .sort((a, b) => (a.maturity < b.maturity ? -1 : 1));

  return (
    <main className="space-y-8 p-8">
      <header className="space-y-1">
        <h1 className="font-serif text-3xl">Vale a pena travar IPCA+ hoje?</h1>
        <p className="text-sm text-muted-foreground">
          Curva do Tesouro Direto em <span data-slot="ref-date">{refDate}</span>. Release{" "}
          <code>{meta.release_id}</code>.
        </p>
      </header>

      <table className="w-full text-sm">
        <thead className="text-left text-xs uppercase text-muted-foreground">
          <tr>
            <th className="py-2">Título</th>
            <th>Vencimento</th>
            <th className="text-right">Taxa de venda</th>
          </tr>
        </thead>
        <tbody className="font-mono tabular-nums">
          {today.map((r) => (
            <tr key={r.instrument_id} data-slot="curve-point" data-instrument-id={r.instrument_id}>
              <td className="py-1 font-sans">{r.bond}</td>
              <td>{r.maturity}</td>
              <td className="text-right">{r.sell_rate.toFixed(2)}%</td>
            </tr>
          ))}
        </tbody>
      </table>

      <p className="text-xs text-muted-foreground">Nada aqui é recomendação de investimento.</p>
    </main>
  );
}
