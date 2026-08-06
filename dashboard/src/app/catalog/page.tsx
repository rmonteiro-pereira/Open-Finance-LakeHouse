import { PageHeader } from "@/components/page-header";
import { CatalogExplorer, type CatalogCard } from "@/components/catalog-explorer";
import { getCatalog } from "@/lib/data";

export const dynamic = "force-dynamic";

/** evenly thin a series down to ~n points for a lightweight client sparkline. */
function downsample(xs: number[], n = 64): number[] {
  if (xs.length <= n) return xs;
  const step = (xs.length - 1) / (n - 1);
  const out: number[] = [];
  for (let i = 0; i < n; i++) out.push(xs[Math.round(i * step)]);
  return out;
}

export default async function CatalogPage() {
  const catalog = await getCatalog();
  const cards: CatalogCard[] = catalog.map((e) => ({
    series_id: e.series_id,
    name: e.name,
    domain: e.domain,
    source: e.source,
    category: e.category,
    unit: e.unit,
    frequency: e.frequency,
    fact: e.fact,
    spark: e.hasSeries ? downsample(e.values) : [],
    latest: e.latest,
    changePct: e.changePct,
    href: e.href,
  }));

  const withSeries = cards.filter((c) => c.spark.length > 1).length;
  const domainCount = new Set(cards.map((c) => c.domain)).size;

  return (
    <div className="mx-auto max-w-[1200px] px-5 py-8 md:px-8 lg:py-10">
      <PageHeader
        kicker="Registry · dim_series"
        title="The full series catalog."
        lede={`Every series the lakehouse tracks — ${cards.length} across ${domainCount} domains, conformed from the source registry into dim_series. ${withSeries} carry a value history in fact_observation; the rest are multi-symbol facts you can open on their dedicated page.`}
      />
      <CatalogExplorer cards={cards} />
    </div>
  );
}
