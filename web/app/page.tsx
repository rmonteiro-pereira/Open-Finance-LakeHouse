import { bisectCenter } from "d3-array";

import { Charts } from "@/components/Charts";
import { getDashboard, status, type Line, type Mart, type Series, type Status } from "@/server/snapshot";

const REPO = "https://github.com/rmonteiro-pereira/Open-Finance-LakeHouse";
const YEAR_MS = 365.25 * 86_400_000;

const month = new Intl.DateTimeFormat("en", { year: "numeric", month: "short", timeZone: "UTC" });
const day = new Intl.DateTimeFormat("en", { year: "numeric", month: "short", day: "numeric", timeZone: "UTC" });
const stamp = new Intl.DateTimeFormat("en", { dateStyle: "medium", timeStyle: "short", timeZone: "UTC" });
const count = new Intl.NumberFormat("en");

function size(bytes: number): string {
  return bytes < 1024 * 1024 ? `${Math.max(1, Math.round(bytes / 1024))} KB` : `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}

type Tile = { label: string; value: string; asOf: string; delta: string; spark: string };

/** Latest value, its change against a year earlier, and a sparkline of the last `points`. */
function tile(label: string, line: Line, opts: { digits: number; suffix: string; points: number; daily?: boolean }): Tile {
  let last = line.v.length - 1;
  while (last > 0 && line.v[last] == null) last--;
  const value = line.v[last] as number;
  const before = line.v[bisectCenter(line.t, line.t[last] - YEAR_MS)];
  const change = before == null ? null : value - before;
  const unit = opts.suffix === "%" ? " pp" : "";
  const delta =
    change == null ? "no value a year earlier" : `${change >= 0 ? "+" : "−"}${Math.abs(change).toFixed(opts.digits)}${unit} in a year`;

  const start = Math.max(0, last - opts.points + 1);
  const values = line.v.slice(start, last + 1).filter((v): v is number => v != null);
  const low = Math.min(...values);
  const span = Math.max(...values) - low || 1;
  const spark = values
    .map((v, i) => `${i ? "L" : "M"}${((i / (values.length - 1)) * 120).toFixed(1)} ${(30 - ((v - low) / span) * 28).toFixed(1)}`)
    .join("");

  return {
    label,
    value: `${value.toFixed(opts.digits)}${opts.suffix}`,
    asOf: (opts.daily ? day : month).format(line.t[last]),
    delta,
    spark,
  };
}

const TIER_NOTE: Record<Mart["redistribution"], string> = {
  open: "Open government data",
  derived: "Derived from B3 files",
  private: "Private",
};

const SKIP_NOTE = {
  private: "an input source forbids redistribution",
  missing: "not built in this run",
  empty: "built with no rows",
};

const STATUS: Record<Status, { icon: string; label: string }> = {
  fresh: { icon: "●", label: "Fresh" },
  late: { icon: "▲", label: "Late" },
  "marts only": { icon: "◇", label: "In marts only" },
  withheld: { icon: "–", label: "Withheld" },
  "no data": { icon: "–", label: "No observations" },
};

function StatusLabel({ value }: { value: Status }) {
  return (
    <span className={`status ${value.replace(" ", "-")}`}>
      <span aria-hidden="true">{STATUS[value].icon}</span> {STATUS[value].label}
    </span>
  );
}

export default async function Page() {
  const { manifest, series, baseUrl, macro, usdBrl } = await getDashboard();

  const tiles = [
    tile("Selic target, month average", macro.selic_target, { digits: 2, suffix: "%", points: 36 }),
    tile("IPCA, 12 months", macro.ipca_12m, { digits: 2, suffix: "%", points: 36 }),
    tile("Real interest rate", macro.real_rate, { digits: 2, suffix: "%", points: 36 }),
    tile("US dollar, BRL", usdBrl, { digits: 2, suffix: "", points: 750, daily: true }),
    tile("Gross debt to GDP", macro.debt_to_gdp, { digits: 1, suffix: "%", points: 36 }),
  ];

  const published = series.filter((s) => s.rows);
  const states = new Map<Series, Status>(series.map((s) => [s, status(s, manifest.generated_at)]));
  const fresh = published.filter((s) => states.get(s) === "fresh").length;
  const rows = manifest.marts.reduce((sum, m) => sum + m.rows, 0) + (manifest.observations?.rows ?? 0);
  const ordered = [...series].sort((a, b) => a.domain.localeCompare(b.domain) || a.key.localeCompare(b.key));

  return (
    <main>
      <header className="hero">
        <p className="eyebrow">Open Finance Lakehouse</p>
        <h1>Brazil&rsquo;s macro and market data, rebuilt every night.</h1>
        <p className="lede">
          Fifty-one public series flow through a Polars, Spark and DuckDB pipeline on a single-node Kubernetes cluster.
          The cluster accepts no inbound traffic: after each run it publishes a versioned snapshot, and this page reads
          only that.
        </p>
        <p className="asof">
          <span className="pulse" aria-hidden="true" />
          Snapshot <code>{manifest.run_id}</code> published {stamp.format(new Date(manifest.generated_at))} UTC
        </p>
      </header>

      <section aria-label="Headline figures" className="tiles">
        {tiles.map((t) => (
          <article className="card tile" key={t.label}>
            <h2>{t.label}</h2>
            <p className="value">{t.value}</p>
            <svg className="spark" viewBox="0 0 120 32" preserveAspectRatio="none" aria-hidden="true">
              <path d={t.spark} />
            </svg>
            <p className="meta">
              {t.delta}
              <br />
              as of {t.asOf}
            </p>
          </article>
        ))}
      </section>

      <section aria-labelledby="charts-title">
        <h2 id="charts-title" className="section">
          Rates, inflation and the currency
        </h2>
        <Charts macro={macro} usdBrl={usdBrl} />
      </section>

      <section aria-labelledby="pipeline-title">
        <h2 id="pipeline-title" className="section">
          What this snapshot holds
        </h2>
        <ul className="facts">
          <li>
            <strong>{manifest.marts.length}</strong> gold marts published
          </li>
          <li>
            <strong>{count.format(rows)}</strong> rows
          </li>
          <li>
            <strong>
              {fresh} of {published.length}
            </strong>{" "}
            open series fresh
          </li>
          <li>
            <strong>{manifest.skipped.length}</strong> marts withheld
          </li>
        </ul>
        <ol className="flow">
          <li>
            <strong>Sources</strong>BACEN, IBGE, IPEA, Tesouro, B3
          </li>
          <li>
            <strong>Bronze</strong>Polars, one Delta table per series
          </li>
          <li>
            <strong>Silver</strong>Spark merge into a star schema
          </li>
          <li>
            <strong>Gold</strong>DuckDB marts with post-build checks
          </li>
          <li>
            <strong>Snapshot</strong>Parquet filtered by licence tier
          </li>
        </ol>

        <div className="card scroll">
          <table>
            <caption>Gold marts in this snapshot</caption>
            <thead>
              <tr>
                <th>Mart</th>
                <th className="num">Rows</th>
                <th>Covers</th>
                <th>Licence tier</th>
                <th>Checksum</th>
                <th>File</th>
              </tr>
            </thead>
            <tbody>
              {manifest.marts.map((m) => (
                <tr key={m.name}>
                  <td>
                    <code>{m.name}</code>
                  </td>
                  <td className="num">{count.format(m.rows)}</td>
                  <td className="nowrap">
                    {m.min_date} to {m.max_date}
                  </td>
                  <td className="nowrap">{TIER_NOTE[m.redistribution]}</td>
                  <td>
                    <code title={m.sha256}>{m.sha256.slice(0, 10)}</code>
                  </td>
                  <td className="nowrap">
                    {m.download ? <a href={`${baseUrl}/${m.path}`}>Parquet, {size(m.bytes)}</a> : "Charts only"}
                  </td>
                </tr>
              ))}
              {manifest.skipped.map((s) => (
                <tr key={s.name} className="withheld">
                  <td>
                    <code>{s.name}</code>
                  </td>
                  <td colSpan={5} className="nowrap">Withheld: {SKIP_NOTE[s.reason]}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section aria-labelledby="catalog-title">
        <h2 id="catalog-title" className="section">
          Series catalogue
        </h2>
        <p className="note">
          Freshness compares each series&rsquo; newest observation with how often it is released. A series can be late
          because its source stopped updating, not only because a run failed. Series from B3 reach this site only
          inside derived marts; Yahoo and the ANBIMA sandbox are withheld.
        </p>
        <div className="card scroll tall">
          <table>
            <thead>
              <tr>
                <th>Series</th>
                <th>Domain</th>
                <th>Source</th>
                <th>Frequency</th>
                <th className="num">Rows</th>
                <th>Newest</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {ordered.map((s) => (
                <tr key={s.key}>
                  <td>
                    {s.name}
                    <br />
                    <code>{s.key}</code>
                  </td>
                  <td>{s.domain}</td>
                  <td>{s.source}</td>
                  <td>{s.frequency}</td>
                  <td className="num">{s.rows ? count.format(s.rows) : ""}</td>
                  <td className="nowrap">{s.last_date ?? ""}</td>
                  <td className="nowrap">
                    <StatusLabel value={states.get(s) as Status} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <footer>
        <a href={REPO}>Source on GitHub</a>
        <span>Data: Banco Central do Brasil, IBGE, IPEA, Tesouro Nacional, B3.</span>
      </footer>
    </main>
  );
}
