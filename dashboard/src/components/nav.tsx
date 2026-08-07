"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

import { NAV } from "@/lib/nav";

/**
 * A masthead, not a sidebar.
 *
 * The routes are five questions, and questions read horizontally. A left rail would spend
 * a fifth of the page on five links and push the figures, which are the point, into a
 * narrower column. Below `sm` the row scrolls rather than collapsing into a menu: five
 * items do not earn a hamburger.
 */
export function Nav() {
  const pathname = usePathname();

  return (
    <header className="border-b">
      <div className="shell flex flex-col gap-y-3 pt-6 sm:flex-row sm:items-baseline sm:justify-between">
        <Link href="/" className="no-underline">
          <span className="font-[family-name:var(--font-display)] text-lg tracking-tight">
            Ledger
          </span>
          <span className="ml-2 text-xs text-[var(--ink-muted)]">Open-Finance LakeHouse</span>
        </Link>
      </div>

      <nav className="shell -mb-px flex gap-x-6 overflow-x-auto pt-4 text-sm">
        {NAV.map((item) => {
          const current = item.href === "/" ? pathname === "/" : pathname.startsWith(item.href);
          return (
            <Link
              key={item.href}
              href={item.href}
              aria-current={current ? "page" : undefined}
              title={item.desc}
              className={`whitespace-nowrap border-b-2 pb-2.5 no-underline transition-colors duration-150 ${
                current
                  ? "border-[var(--accent)] text-[var(--ink)]"
                  : "border-transparent text-[var(--ink-muted)] hover:text-[var(--ink)]"
              }`}
            >
              {item.short}
            </Link>
          );
        })}
      </nav>
    </header>
  );
}
