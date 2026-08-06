// Formatting helpers — pt-BR aware, tuned for financial figures.

const ptNum = (digits: number) =>
  new Intl.NumberFormat("pt-BR", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });

export const fmtNum = (v: number | null | undefined, digits = 2): string =>
  v == null || Number.isNaN(v) ? "—" : ptNum(digits).format(v);

export const fmtPct = (v: number | null | undefined, digits = 2): string =>
  v == null || Number.isNaN(v) ? "—" : `${ptNum(digits).format(v)}%`;

export const fmtSigned = (v: number | null | undefined, digits = 2): string => {
  if (v == null || Number.isNaN(v)) return "—";
  const s = ptNum(digits).format(v);
  return v > 0 ? `+${s}` : s;
};

export const fmtSignedPct = (v: number | null | undefined, digits = 2): string => {
  if (v == null || Number.isNaN(v)) return "—";
  return `${fmtSigned(v, digits)}%`;
};

export const fmtBRL = (v: number | null | undefined, digits = 2): string =>
  v == null || Number.isNaN(v)
    ? "—"
    : new Intl.NumberFormat("pt-BR", {
        style: "currency",
        currency: "BRL",
        minimumFractionDigits: digits,
        maximumFractionDigits: digits,
      }).format(v);

export const fmtCompact = (v: number | null | undefined): string =>
  v == null || Number.isNaN(v)
    ? "—"
    : new Intl.NumberFormat("pt-BR", { notation: "compact", maximumFractionDigits: 1 }).format(v);

const MONTHS_PT = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"];

/** "2024-06-01" → "jun 2024" (UTC-safe, no timezone drift) */
export const fmtMonth = (iso: string): string => {
  const [y, m] = iso.split("-");
  return `${MONTHS_PT[Number(m) - 1]} ${y}`;
};

/** "2024-06-01" → "jun '24" (compact axis) */
export const fmtMonthShort = (iso: string): string => {
  const [y, m] = iso.split("-");
  return `${MONTHS_PT[Number(m) - 1]} '${y.slice(2)}`;
};

/** "2024-06-15" → "15 jun 24" */
export const fmtDate = (iso: string): string => {
  const [y, m, d] = iso.split("-");
  return `${d} ${MONTHS_PT[Number(m) - 1]} ${y.slice(2)}`;
};

export const fmtYear = (iso: string): string => iso.slice(0, 4);

export const signClass = (v: number | null | undefined): string =>
  v == null || v === 0 ? "text-muted-foreground" : v > 0 ? "text-positive" : "text-negative";

/** delta between last two points of a numeric series */
export const lastDelta = (xs: number[]): number =>
  xs.length < 2 ? 0 : xs[xs.length - 1] - xs[xs.length - 2];
