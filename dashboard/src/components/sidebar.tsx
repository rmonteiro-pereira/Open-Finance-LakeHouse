"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useState } from "react";
import { Menu, X } from "lucide-react";
import { NAV } from "@/lib/nav";
import { cn } from "@/lib/utils";

function Brand() {
  return (
    <Link href="/" className="group flex items-center gap-3 px-1">
      <span className="relative grid size-9 place-items-center rounded-md bg-primary/15 ring-1 ring-primary/30">
        <span className="font-display text-lg font-semibold text-primary">L</span>
        <span className="absolute -inset-px rounded-md bg-primary/10 blur-sm transition-opacity group-hover:opacity-100 opacity-0" />
      </span>
      <span className="leading-tight">
        <span className="block font-display text-[17px] font-semibold tracking-tight">Ledger</span>
        <span className="block text-[10px] uppercase tracking-[0.22em] text-muted-foreground">
          OFL Lakehouse
        </span>
      </span>
    </Link>
  );
}

function NavLinks({ onNavigate }: { onNavigate?: () => void }) {
  const pathname = usePathname();
  return (
    <nav className="flex flex-col gap-1">
      {NAV.map((item) => {
        const active = item.href === "/" ? pathname === "/" : pathname.startsWith(item.href);
        const Icon = item.icon;
        return (
          <Link
            key={item.href}
            href={item.href}
            onClick={onNavigate}
            aria-current={active ? "page" : undefined}
            className={cn(
              "group relative flex items-center gap-3 rounded-md px-3 py-2.5 text-sm transition-colors",
              active
                ? "bg-accent text-foreground"
                : "text-muted-foreground hover:bg-accent/50 hover:text-foreground",
            )}
          >
            {active && (
              <span className="absolute left-0 top-1/2 h-5 w-[3px] -translate-y-1/2 rounded-r-full bg-primary" />
            )}
            <Icon className={cn("size-4 shrink-0", active ? "text-primary" : "")} strokeWidth={1.75} />
            <span className="flex-1">
              <span className="block font-medium leading-tight">{item.label}</span>
              <span className="block text-[11px] text-muted-foreground/80 leading-tight">
                {item.desc}
              </span>
            </span>
          </Link>
        );
      })}
    </nav>
  );
}

export function Sidebar() {
  const [open, setOpen] = useState(false);
  return (
    <>
      {/* mobile top bar */}
      <div className="sticky top-0 z-40 flex items-center justify-between border-b border-border bg-background/80 px-4 py-3 backdrop-blur md:hidden">
        <Brand />
        <button
          onClick={() => setOpen((v) => !v)}
          className="grid size-9 place-items-center rounded-md border border-border text-muted-foreground"
          aria-label="Toggle navigation"
        >
          {open ? <X className="size-4" /> : <Menu className="size-4" />}
        </button>
      </div>

      {/* mobile drawer */}
      {open && (
        <div className="fixed inset-0 z-40 md:hidden">
          <div className="absolute inset-0 bg-background/70 backdrop-blur-sm" onClick={() => setOpen(false)} />
          <div className="absolute left-0 top-0 h-full w-72 border-r border-border bg-sidebar p-4">
            <div className="mb-6 mt-1">
              <Brand />
            </div>
            <NavLinks onNavigate={() => setOpen(false)} />
          </div>
        </div>
      )}

      {/* desktop rail */}
      <aside className="sticky top-0 hidden h-screen w-[272px] shrink-0 flex-col border-r border-sidebar-border bg-sidebar md:flex">
        <div className="px-5 pb-6 pt-6">
          <Brand />
        </div>
        <div className="px-3">
          <NavLinks />
        </div>
        <div className="mt-auto px-5 pb-6 pt-4">
          <div className="rounded-lg border border-border/70 bg-card/50 p-3">
            <p className="text-[11px] leading-relaxed text-muted-foreground">
              Medallion lakehouse · Delta on MinIO
              <br />
              <span className="text-foreground/70">Polars → Spark → DuckDB</span>
            </p>
          </div>
        </div>
      </aside>
    </>
  );
}
