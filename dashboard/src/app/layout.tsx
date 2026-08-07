import type { Metadata } from "next";
import { Fraunces, Hanken_Grotesk, JetBrains_Mono } from "next/font/google";

import { Nav } from "@/components/nav";
import "./globals.css";

/*
  One family for the interface, one for figures. Fraunces appears exactly once per page,
  on the title: the almanac signature. It never reaches a label, a button or a datum.
*/
const sans = Hanken_Grotesk({ variable: "--font-sans", subsets: ["latin"] });
const display = Fraunces({ variable: "--font-display", subsets: ["latin"], axes: ["SOFT", "WONK"] });
const mono = JetBrains_Mono({ variable: "--font-mono", subsets: ["latin"] });

export const metadata: Metadata = {
  title: "Ledger · Open-Finance LakeHouse",
  description:
    "Macro e renda fixa brasileiros, publicados de um release versionado. Cada número com sua unidade, sua data e sua procedência.",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html
      lang="pt-BR"
      className={`${display.variable} ${sans.variable} ${mono.variable} h-full`}
      suppressHydrationWarning
    >
      <body className="min-h-full">
        <Nav />
        <main className="shell pb-24 pt-10">{children}</main>
        <footer className="mt-16 border-t py-8">
          <div className="shell flex flex-wrap items-baseline justify-between gap-x-8 gap-y-2 text-xs text-[var(--ink-muted)]">
            <span>
              Dados de um release publicado. Sem servidor, sem credencial, sem leitura do cluster.
            </span>
            <span>Nada aqui é recomendação de investimento.</span>
          </div>
        </footer>
      </body>
    </html>
  );
}
