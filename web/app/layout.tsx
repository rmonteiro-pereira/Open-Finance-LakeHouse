import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Open Finance Lakehouse",
  description:
    "Brazilian macro and market data, rebuilt every night by a Polars, Spark and DuckDB pipeline and published as a versioned snapshot.",
};

export default function RootLayout({ children }: LayoutProps<"/">) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
