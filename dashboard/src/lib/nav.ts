import {
  Gauge,
  Percent,
  Flame,
  Spline,
  ShieldCheck,
  type LucideIcon,
} from "lucide-react";

export type NavItem = {
  href: string;
  label: string;
  short: string;
  desc: string;
  icon: LucideIcon;
};

/**
 * Six routes, each named after a question somebody asks out loud.
 *
 * The previous eight were named after marts — /fx, /yield-curve, /derivatives — which is
 * the factory's floor plan projected onto the shop window. A reader does not arrive
 * wanting `mart_real_interest`; they arrive asking whether the real rate is high.
 *
 * /derivatives, /equities and the DI curve are also gone for a second, independent
 * reason: B3 barred redistribution of derived values absent written authorisation, so
 * those numbers cannot appear on a public page at all.
 */
export const NAV: NavItem[] = [
  { href: "/", label: "Hoje", short: "Hoje", desc: "Como está o Brasil hoje", icon: Gauge },
  { href: "/juro-real", label: "Juro real", short: "Juro real", desc: "O juro real está alto?", icon: Percent },
  { href: "/inflacao", label: "Inflação", short: "Inflação", desc: "A inflação está cedendo?", icon: Flame },
  { href: "/curva-do-tesouro", label: "Curva do Tesouro", short: "Curva", desc: "Vale a pena travar IPCA+ hoje?", icon: Spline },
  { href: "/confianca", label: "Confiança", short: "Confiança", desc: "Dá pra confiar hoje, e como eu pego isso", icon: ShieldCheck },
];
