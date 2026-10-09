"use client";

import { useState } from "react";

import type { Dashboard } from "@/server/snapshot";

import { LineChart } from "./LineChart";

const RANGES = [
  { label: "1Y", years: 1 },
  { label: "5Y", years: 5 },
  { label: "10Y", years: 10 },
  { label: "All", years: null },
] as const;

const percent = (value: number) => `${value.toFixed(1)}%`;
const brl = (value: number) => value.toFixed(2);

export function Charts({ macro, usdBrl }: Pick<Dashboard, "macro" | "usdBrl">) {
  const [range, setRange] = useState<(typeof RANGES)[number]["label"]>("10Y");
  const years = RANGES.find((r) => r.label === range)?.years ?? null;
  const end = usdBrl.t[usdBrl.t.length - 1];
  const from = years == null ? 0 : end - years * 365.25 * 86_400_000;

  return (
    <>
      <div className="filters" role="group" aria-label="Time range">
        {RANGES.map((r) => (
          <button key={r.label} type="button" aria-pressed={r.label === range} onClick={() => setRange(r.label)}>
            {r.label}
          </button>
        ))}
      </div>
      <div className="charts">
        <LineChart
          title="Policy rate, inflation and the real rate"
          subtitle="Monthly average of the Selic target, IPCA over 12 months, and the rate net of inflation. Percent."
          from={from}
          format={percent}
          zeroLine
          series={[
            { key: "selic", label: "Selic target", color: "var(--series-1)", data: macro.selic_target },
            { key: "ipca", label: "IPCA, 12 months", color: "var(--series-2)", data: macro.ipca_12m },
            { key: "real", label: "Real rate", color: "var(--series-3)", data: macro.real_rate },
          ]}
        />
        <LineChart
          title="US dollar in reais"
          subtitle="PTAX selling rate published by the central bank. BRL per USD, daily."
          from={from}
          format={brl}
          series={[{ key: "usd", label: "USD/BRL", color: "var(--series-1)", data: usdBrl }]}
        />
      </div>
    </>
  );
}
