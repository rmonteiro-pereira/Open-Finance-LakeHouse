import {
  Gauge,
  Percent,
  Flame,
  ArrowLeftRight,
  Spline,
  CandlestickChart,
  Activity,
  Library,
  type LucideIcon,
} from "lucide-react";

export type NavItem = {
  href: string;
  label: string;
  short: string;
  desc: string;
  icon: LucideIcon;
};

export const NAV: NavItem[] = [
  { href: "/", label: "Macro Overview", short: "Macro", desc: "Policy rate, inflation, FX & debt", icon: Gauge },
  { href: "/real-interest", label: "Real Interest", short: "Real rate", desc: "Selic deflated by 12m IPCA", icon: Percent },
  { href: "/inflation", label: "Inflation", short: "Inflation", desc: "Headline, cores & the IGP family", icon: Flame },
  { href: "/fx", label: "Foreign Exchange", short: "FX", desc: "USD/BRL & EUR/BRL, returns & vol", icon: ArrowLeftRight },
  { href: "/yield-curve", label: "Yield Curve", short: "Curve", desc: "Treasury curve over time", icon: Spline },
  { href: "/equities", label: "Equities", short: "Equities", desc: "Blue-chip & global watchlist", icon: CandlestickChart },
  { href: "/derivatives", label: "Derivatives", short: "Derivs", desc: "DI futures curve & open interest", icon: Activity },
  { href: "/catalog", label: "Series Catalog", short: "Catalog", desc: "Every series in the lakehouse registry", icon: Library },
];
