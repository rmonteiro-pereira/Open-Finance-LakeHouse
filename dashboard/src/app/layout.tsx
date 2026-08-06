import type { Metadata } from "next";
import { Inter, JetBrains_Mono } from "next/font/google";
import "./globals.css";
import { Sidebar } from "@/components/sidebar";

// One clean sans for both body and headings — the shadcn default register.
const sans = Inter({
  variable: "--font-sans",
  subsets: ["latin"],
});

const display = Inter({
  variable: "--font-display",
  subsets: ["latin"],
});

const mono = JetBrains_Mono({
  variable: "--font-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Ledger · OFL Lakehouse",
  description:
    "Brazilian macro, rates, inflation, FX, the treasury yield curve and equities — read from the Open-Finance-LakeHouse gold marts.",
};

export default function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  return (
    <html
      lang="en"
      className={`dark ${display.variable} ${sans.variable} ${mono.variable} h-full antialiased`}
      suppressHydrationWarning
    >
      <body className="min-h-full">
        <div className="app-bg flex min-h-screen flex-col md:flex-row">
          <Sidebar />
          <main className="relative z-10 min-w-0 flex-1">{children}</main>
        </div>
      </body>
    </html>
  );
}
