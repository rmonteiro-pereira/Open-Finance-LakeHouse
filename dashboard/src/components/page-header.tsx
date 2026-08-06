import { Database, FlaskConical } from "lucide-react";
import { getMeta } from "@/lib/data";
import { fmtDate } from "@/lib/format";

export async function PageHeader({
  kicker,
  title,
  lede,
  asOf,
}: {
  kicker: string;
  title: string;
  lede: string;
  asOf?: string;
}) {
  const meta = await getMeta();
  const live = meta.generated_from === "live";
  return (
    <header className="rise mb-8 flex flex-col gap-5 border-b border-border/70 pb-7 lg:flex-row lg:items-end lg:justify-between">
      <div className="max-w-2xl">
        <p className="mb-2 text-[11px] font-medium uppercase tracking-[0.28em] text-primary/90">
          {kicker}
        </p>
        <h1 className="font-display text-[1.85rem] font-semibold leading-[1.05] tracking-tight text-balance text-foreground/90 md:text-[2.2rem]">
          {title}
        </h1>
        <p className="mt-3 text-sm leading-relaxed text-muted-foreground text-pretty">{lede}</p>
      </div>
      <div className="flex shrink-0 items-center gap-2">
        {asOf && (
          <span className="rounded-full border border-border bg-card/60 px-3 py-1.5 text-xs text-muted-foreground tnum">
            as of <span className="text-foreground/80">{fmtDate(asOf)}</span>
          </span>
        )}
        <span
          className={`inline-flex items-center gap-1.5 rounded-full border px-3 py-1.5 text-xs ${
            live
              ? "border-positive/30 bg-positive/10 text-positive"
              : "border-primary/30 bg-primary/10 text-primary"
          }`}
          title={
            live
              ? `Live Delta snapshot · ${meta.bucket ?? "lakehouse"}`
              : "Synthetic, real-shaped data — swap in snapshot/export.py for live Delta"
          }
        >
          {live ? <Database className="size-3.5" /> : <FlaskConical className="size-3.5" />}
          {live ? "live data" : "demo data"}
        </span>
      </div>
    </header>
  );
}
