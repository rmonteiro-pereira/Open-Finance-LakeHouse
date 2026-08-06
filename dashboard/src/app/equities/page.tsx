import { PageHeader } from "@/components/page-header";
import { Panel } from "@/components/panel";
import { EquitiesChart } from "@/components/charts/equities";
import { EquityUniverseTable } from "@/components/equity-universe";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { getEquities, getEquityUniverse } from "@/lib/data";
import { fmtCompact, fmtNum, fmtSignedPct, signClass } from "@/lib/format";
import { cn } from "@/lib/utils";

export const dynamic = "force-dynamic";

// B3 round-lot tickers are plain (PETR4, not PETR4.SA); indices carry ^.
const ORDER = [
  "^BVSP", "^GSPC", "^IXIC", "PETR4", "VALE3", "ITUB4", "BBDC4",
  "BBAS3", "B3SA3", "ABEV3", "WEGE3", "MGLU3", "PRIO3", "RENT3",
];
const NAMES: Record<string, string> = {
  "^BVSP": "Ibovespa",
  "^GSPC": "S&P 500",
  "^IXIC": "Nasdaq Composite",
  PETR4: "Petrobras",
  VALE3: "Vale",
  ITUB4: "Itaú Unibanco",
  BBDC4: "Bradesco",
  BBAS3: "Banco do Brasil",
  B3SA3: "B3",
  ABEV3: "Ambev",
  WEGE3: "WEG",
  MGLU3: "Magazine Luiza",
  PRIO3: "PetroRio",
  RENT3: "Localiza",
};

export default async function EquitiesPage() {
  const [allRows, universe] = await Promise.all([getEquities(), getEquityUniverse()]);
  const symbols = ORDER.filter((s) => allRows.some((r) => r.symbol === s));
  // Only the curated symbols reach the client chart (the full mart is large).
  const rows = allRows.filter((r) => symbols.includes(r.symbol));

  const latest = symbols.map((s) => {
    const ser = rows.filter((r) => r.symbol === s).sort((a, b) => a.date.localeCompare(b.date));
    const last = ser[ser.length - 1];
    const fromHigh = ((last.close - last.high_52w) / last.high_52w) * 100;
    const rangePos =
      last.high_52w === last.low_52w
        ? 100
        : ((last.close - last.low_52w) / (last.high_52w - last.low_52w)) * 100;
    return { ...last, name: NAMES[s] ?? s, fromHigh, rangePos };
  });

  const asOf = rows.reduce((m, r) => (r.date > m ? r.date : m), rows[0].date);

  return (
    <div className="mx-auto max-w-[1200px] px-5 py-8 md:px-8 lg:py-10">
      <PageHeader
        kicker="Markets · Equities"
        title="The watchlist — and the whole exchange."
        lede={`Brazilian blue-chips and the global anchors that lead them, in detail; below, every one of the ${universe.length} cash-market names listed on B3 — searchable by sector, with each name's close, realised vol and 52-week range.`}
        asOf={asOf}
      />

      <Panel
        style={{ animationDelay: "120ms" }}
        title="Price & 52-week range"
        subtitle="Pick a name to inspect its trend, moving average and trading range."
      >
        <EquitiesChart rows={rows} symbols={symbols} />
      </Panel>

      <Panel
        style={{ animationDelay: "240ms" }}
        className="mt-4"
        title="Watchlist snapshot"
        subtitle="Latest close and analytics across the curated watchlist."
        bodyClassName="px-0 pb-0 pt-2"
      >
        <div className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow className="border-border/70 hover:bg-transparent">
                <TableHead className="pl-5">Instrument</TableHead>
                <TableHead className="text-right">Close</TableHead>
                <TableHead className="text-right">Day</TableHead>
                <TableHead className="text-right">21d vol</TableHead>
                <TableHead className="text-right">From 52w high</TableHead>
                <TableHead className="w-[160px] pr-5 text-right">52w range</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {latest.map((r) => (
                <TableRow key={r.symbol} className="border-border/50">
                  <TableCell className="pl-5">
                    <div className="font-medium text-foreground">{r.name}</div>
                    <div className="font-mono text-[11px] text-muted-foreground">{r.symbol}</div>
                  </TableCell>
                  <TableCell className="text-right font-mono tabular-nums">{fmtNum(r.close, 2)}</TableCell>
                  <TableCell className={cn("text-right font-mono tabular-nums", signClass(r.daily_return_pct))}>
                    {fmtSignedPct(r.daily_return_pct, 2)}
                  </TableCell>
                  <TableCell className="text-right font-mono tabular-nums text-muted-foreground">
                    {fmtNum(r.vol_21d, 1)}%
                  </TableCell>
                  <TableCell className={cn("text-right font-mono tabular-nums", signClass(r.fromHigh))}>
                    {fmtSignedPct(r.fromHigh, 1)}
                  </TableCell>
                  <TableCell className="pr-5">
                    <div className="flex items-center justify-end gap-2">
                      <span className="font-mono text-[10px] tabular-nums text-muted-foreground">
                        {fmtCompact(r.low_52w)}
                      </span>
                      <div className="relative h-1.5 w-20 overflow-hidden rounded-full bg-border">
                        <div
                          className="absolute top-1/2 size-2.5 -translate-y-1/2 rounded-full bg-primary ring-2 ring-card"
                          style={{ left: `calc(${Math.max(0, Math.min(100, r.rangePos))}% - 5px)` }}
                        />
                      </div>
                      <span className="font-mono text-[10px] tabular-nums text-muted-foreground">
                        {fmtCompact(r.high_52w)}
                      </span>
                    </div>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      </Panel>

      <Panel
        style={{ animationDelay: "320ms" }}
        className="mt-4"
        title="The whole exchange · B3 cash market"
        subtitle="Every listed round-lot name, grouped by sector. Search, sort, and scan the 52-week range and recent trend."
      >
        <EquityUniverseTable rows={universe} />
      </Panel>
    </div>
  );
}
