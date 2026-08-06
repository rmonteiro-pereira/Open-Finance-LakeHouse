#!/usr/bin/env python3
"""Generate realistic *synthetic* snapshots for the OFL dashboard.

Output JSON files match the EXACT shape of `snapshot/export.py` (the live
DuckDB exporter), so the dashboard cannot tell the difference. This lets the
UI be fully populated without MinIO access. Swap to real data by running
`export.py` instead — the dashboard reads the same files.

Pure standard library, deterministic (seeded). No pandas / numpy required.

    python snapshot/gen_synthetic.py
"""
from __future__ import annotations

import json
import math
import os
import random
from datetime import date, timedelta

SEED = 20260623
random.seed(SEED)

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.normpath(os.path.join(HERE, "..", "public", "data"))
os.makedirs(OUT, exist_ok=True)

# Plano Real floor — no BRL macro history before this.
START = date(2000, 1, 1)
END = date(2026, 6, 1)


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def months(a: date, b: date):
    y, m = a.year, a.month
    while (y, m) <= (b.year, b.month):
        yield date(y, m, 1)
        m += 1
        if m > 12:
            m = 1
            y += 1


def bdays(a: date, b: date):
    """Business days (Mon-Fri) inclusive."""
    d = a
    while d <= b:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


def interp_yearly(anchors: dict[int, float], dts: list[date], jitter: float):
    """Linear-interpolate yearly anchors to a monthly path with light noise."""
    ys = sorted(anchors)
    out = []
    for d in dts:
        t = d.year + (d.month - 1) / 12.0
        if t <= ys[0]:
            v = anchors[ys[0]]
        elif t >= ys[-1]:
            v = anchors[ys[-1]]
        else:
            lo = max(y for y in ys if y <= t)
            hi = min(y for y in ys if y >= t)
            v = anchors[lo] if lo == hi else anchors[lo] + (anchors[hi] - anchors[lo]) * (t - lo) / (hi - lo)
        out.append(v * (1 + random.uniform(-jitter, jitter)))
    return out


def r(x: float, n: int = 4) -> float:
    return round(x, n)


def write(name: str, rows: list[dict]):
    path = os.path.join(OUT, f"{name}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, separators=(",", ":"))
    print(f"  {name:24s} {len(rows):>6d} rows  ->  {os.path.relpath(path, HERE)}")


MONTHS = list(months(START, END))
ISO_M = [d.isoformat() for d in MONTHS]


# --------------------------------------------------------------------------- #
# macro paths (yearly anchors -> monthly)
# --------------------------------------------------------------------------- #
SELIC = interp_yearly({
    2000: 18.0, 2001: 17.5, 2002: 22.0, 2003: 18.0, 2004: 17.0, 2005: 18.5,
    2006: 14.0, 2007: 11.5, 2008: 13.0, 2009: 9.5, 2010: 10.0, 2011: 11.5,
    2012: 8.0, 2013: 9.0, 2014: 11.5, 2015: 14.0, 2016: 14.0, 2017: 9.0,
    2018: 6.5, 2019: 5.5, 2020: 2.5, 2021: 6.0, 2022: 13.0, 2023: 13.0,
    2024: 11.0, 2025: 11.0, 2026: 9.75,
}, MONTHS, 0.015)

IPCA12 = interp_yearly({
    2000: 6.0, 2001: 7.0, 2002: 11.5, 2003: 9.0, 2004: 7.0, 2005: 5.7,
    2006: 4.5, 2007: 4.0, 2008: 6.0, 2009: 4.3, 2010: 5.9, 2011: 6.5,
    2012: 5.8, 2013: 5.9, 2014: 6.4, 2015: 9.5, 2016: 7.0, 2017: 3.5,
    2018: 3.7, 2019: 4.3, 2020: 4.5, 2021: 9.5, 2022: 7.5, 2023: 4.6,
    2024: 4.4, 2025: 4.0, 2026: 3.8,
}, MONTHS, 0.03)

USDBRL_M = interp_yearly({
    2000: 1.95, 2001: 2.35, 2002: 3.20, 2003: 2.95, 2004: 2.70, 2005: 2.35,
    2006: 2.15, 2007: 1.85, 2008: 2.00, 2009: 1.80, 2010: 1.70, 2011: 1.80,
    2012: 2.05, 2013: 2.30, 2014: 2.55, 2015: 3.65, 2016: 3.35, 2017: 3.25,
    2018: 3.75, 2019: 4.00, 2020: 5.05, 2021: 5.45, 2022: 5.20, 2023: 4.95,
    2024: 5.40, 2025: 5.55, 2026: 5.40,
}, MONTHS, 0.02)

DEBT = interp_yearly({
    2000: 65.0, 2003: 70.0, 2006: 66.0, 2010: 62.0, 2013: 57.0, 2015: 65.0,
    2016: 70.0, 2018: 77.0, 2020: 87.0, 2021: 80.0, 2022: 72.0, 2023: 74.0,
    2024: 76.5, 2025: 78.0, 2026: 77.0,
}, MONTHS, 0.01)


def seasonal_ipca_mom(d: date, accum12: float) -> float:
    base = accum12 / 12.0
    seas = 0.18 * math.sin((d.month - 1) / 12.0 * 2 * math.pi + 0.6)
    return max(-0.4, base + seas + random.uniform(-0.12, 0.12))


# --------------------------------------------------------------------------- #
# 1. mart_macro_dashboard  (month)
# --------------------------------------------------------------------------- #
def macro():
    rows = []
    for d, s, i12, fx, db in zip(MONTHS, SELIC, IPCA12, USDBRL_M, DEBT):
        rows.append({
            "month": d.isoformat(),
            "selic_target": r(s, 2),
            "ipca_mom": r(seasonal_ipca_mom(d, i12), 3),
            "usd_brl": r(fx, 4),
            "debt_to_gdp_pct": r(db, 2),
        })
    return rows


# --------------------------------------------------------------------------- #
# 2. mart_real_interest  (month)
# --------------------------------------------------------------------------- #
def real_interest():
    rows = []
    for d, s, i12 in zip(MONTHS, SELIC, IPCA12):
        real = ((1 + s / 100) / (1 + i12 / 100) - 1) * 100
        rows.append({
            "month": d.isoformat(),
            "selic_target": r(s, 2),
            "ipca_accum_12m": r(i12, 2),
            "real_interest_rate": r(real, 2),
        })
    return rows


# --------------------------------------------------------------------------- #
# 3. mart_inflation_panel  (month)
# --------------------------------------------------------------------------- #
def inflation_panel():
    rows = []
    igpm_run = []
    for d, i12 in zip(MONTHS, IPCA12):
        ipca = seasonal_ipca_mom(d, i12)
        ipca15 = ipca + random.uniform(-0.08, 0.08)
        inpc = ipca + random.uniform(-0.10, 0.12)
        # IGP family is far more volatile (wholesale + FX pass-through)
        igpm = ipca * 1.4 + random.uniform(-0.6, 0.9)
        igpdi = igpm + random.uniform(-0.25, 0.25)
        igpm_run.append(igpm)
        igpm_12m = sum(igpm_run[-12:]) if len(igpm_run) >= 1 else igpm
        rows.append({
            "month": d.isoformat(),
            "ipca_mom": r(ipca, 3),
            "ipca15_mom": r(ipca15, 3),
            "inpc_mom": r(inpc, 3),
            "igpm_mom": r(igpm, 3),
            "igpm_12m": r(igpm_12m, 2),
            "igpdi_mom": r(igpdi, 3),
        })
    return rows


# --------------------------------------------------------------------------- #
# 4. mart_fx  (symbol x date, daily, last 2y)
# --------------------------------------------------------------------------- #
def monthly_at(path: list[float], d: date) -> float:
    idx = (d.year - START.year) * 12 + (d.month - START.month)
    idx = max(0, min(len(path) - 1, idx))
    return path[idx]


def fx():
    fx_start = date(2024, 6, 1)
    days = list(bdays(fx_start, date(2026, 6, 26)))
    rows = []
    for series_id, scale in (("usd_brl", 1.0), ("eur_brl", 1.08)):
        level = monthly_at(USDBRL_M, fx_start) * scale
        rets: list[float] = []
        mtd_anchor = level
        prev_month = days[0].month
        for d in days:
            target = monthly_at(USDBRL_M, d) * scale
            drift = (target - level) * 0.05
            shock = random.gauss(0, 0.006) * level
            new = max(0.5, level + drift + shock)
            ret = (new / level - 1) * 100
            level = new
            rets.append(ret)
            vol = 0.0
            if len(rets) >= 21:
                window = rets[-21:]
                mean = sum(window) / 21
                vol = math.sqrt(sum((x - mean) ** 2 for x in window) / 21) * math.sqrt(252)
            if d.month != prev_month:
                prev_month = d.month
                mtd_anchor = level
            mtd = (level / mtd_anchor - 1) * 100
            rows.append({
                "series_id": series_id,
                "date": d.isoformat(),
                "rate": r(level, 4),
                "daily_return_pct": r(ret, 3),
                "vol_21d": r(vol, 2),
                "mtd_return_pct": r(mtd, 2),
            })
    return rows


# --------------------------------------------------------------------------- #
# 5. mart_yield_curve  (bond x date, monthly snapshots last 3y)
# --------------------------------------------------------------------------- #
def yield_curve():
    snaps = [d for d in MONTHS if d >= date(2023, 6, 1)]
    bonds = [
        # (bond, maturity, bond_type)
        ("NTN-B 2027", date(2027, 5, 15), "ipca_plus"),
        ("NTN-B 2030", date(2030, 8, 15), "ipca_plus"),
        ("NTN-B 2035", date(2035, 5, 15), "ipca_plus"),
        ("NTN-B 2045", date(2045, 5, 15), "ipca_plus"),
        ("LTN 2027", date(2027, 1, 1), "prefixado"),
        ("LTN 2029", date(2029, 1, 1), "prefixado"),
        ("NTN-F 2033", date(2033, 1, 1), "prefixado"),
        ("LFT 2028", date(2028, 3, 1), "selic"),
        ("LFT 2031", date(2031, 3, 1), "selic"),
    ]
    rows = []
    for d in snaps:
        selic_now = monthly_at(SELIC, d)
        for bond, mat, btype in bonds:
            ytm = max(0.1, (mat - d).days / 365.25)
            if btype == "ipca_plus":
                base = 5.8 + 0.12 * math.log(ytm + 1)
                y = base + (selic_now - 10) * 0.06 + random.uniform(-0.15, 0.15)
                price = 1000 / ((1 + y / 100) ** min(ytm, 30)) * 3.2
            elif btype == "prefixado":
                base = selic_now + 0.4 + 0.18 * math.log(ytm + 1)
                y = base + random.uniform(-0.2, 0.2)
                price = 1000 / ((1 + y / 100) ** min(ytm, 30))
            else:  # selic / floater
                y = selic_now + random.uniform(-0.05, 0.1)
                price = 15000 + random.uniform(-50, 50)
            rows.append({
                "date": d.isoformat(),
                "bond": bond,
                "maturity": mat.isoformat(),
                "years_to_maturity": r(ytm, 2),
                "yield": r(y, 3),
                "buy_rate": r(y + 0.05, 3),
                "sell_price": r(price, 2),
                "bond_type": btype,
            })
    return rows


# --------------------------------------------------------------------------- #
# 6. mart_equity_daily  (symbol x date, daily last 2y)
#    A curated set with FULL daily history — feeds the interactive detail chart.
# --------------------------------------------------------------------------- #
EQUITIES = [
    # symbol, start_price, annual_drift, daily_vol
    ("^BVSP", 120000, 0.12, 0.011),
    ("^GSPC", 5200.0, 0.14, 0.009),
    ("^IXIC", 16500.0, 0.18, 0.012),
    ("PETR4.SA", 36.0, 0.10, 0.018),
    ("VALE3.SA", 62.0, -0.04, 0.017),
    ("ITUB4.SA", 30.0, 0.16, 0.013),
    ("BBDC4.SA", 13.5, 0.05, 0.016),
    ("BBAS3.SA", 26.0, 0.14, 0.015),
    ("B3SA3.SA", 11.5, 0.06, 0.016),
    ("ABEV3.SA", 12.5, 0.02, 0.012),
    ("WEGE3.SA", 38.0, 0.18, 0.015),
    ("MGLU3.SA", 11.0, -0.10, 0.035),
    ("PRIO3.SA", 44.0, 0.22, 0.024),
    ("RENT3.SA", 52.0, 0.08, 0.020),
]


def equity_daily():
    eq_start = date(2024, 6, 1)
    days = list(bdays(eq_start, date(2026, 6, 26)))
    rows = []
    for symbol, p0, drift, dvol in EQUITIES:
        price = p0
        closes: list[float] = []
        rets: list[float] = []
        for d in days:
            mu = drift / 252
            ret = mu + random.gauss(0, dvol)
            close = max(0.01, price * (1 + ret))
            o = price * (1 + random.gauss(0, dvol * 0.4))
            hi = max(o, close) * (1 + abs(random.gauss(0, dvol * 0.5)))
            lo = min(o, close) * (1 - abs(random.gauss(0, dvol * 0.5)))
            vol_shares = abs(random.gauss(1, 0.3)) * (1e7 if symbol.endswith(".SA") else 1e9)
            price = close
            closes.append(close)
            rets.append(ret * 100)
            sma21 = sum(closes[-21:]) / min(len(closes), 21)
            vol21 = 0.0
            if len(rets) >= 21:
                w = rets[-21:]
                m = sum(w) / 21
                vol21 = math.sqrt(sum((x - m) ** 2 for x in w) / 21) * math.sqrt(252)
            w52 = closes[-252:]
            rows.append({
                "symbol": symbol,
                "date": d.isoformat(),
                "open": r(o, 2),
                "high": r(hi, 2),
                "low": r(lo, 2),
                "close": r(close, 2),
                "volume": r(vol_shares, 0),
                "daily_return_pct": r(ret * 100, 3),
                "sma_21": r(sma21, 2),
                "vol_21d": r(vol21, 2),
                "high_52w": r(max(w52), 2),
                "low_52w": r(min(w52), 2),
            })
    return rows


# --------------------------------------------------------------------------- #
# 6b. mart_equity_universe  (one row per B3-listed name — the whole exchange)
#     Compact: latest snapshot + a downsampled close sparkline per ticker.
#     Mirrors a GROUP BY over fact_security_price (b3_cotahist round-lot universe).
# --------------------------------------------------------------------------- #
# (symbol, name, sector, start_price, annual_drift, daily_vol)
UNIVERSE = [
    # financials
    ("ITUB4", "Itaú Unibanco", "Financials", 30.0, 0.16, 0.013),
    ("BBDC4", "Bradesco", "Financials", 13.5, 0.05, 0.016),
    ("BBAS3", "Banco do Brasil", "Financials", 26.0, 0.14, 0.015),
    ("SANB11", "Santander Brasil", "Financials", 28.0, 0.06, 0.015),
    ("B3SA3", "B3", "Financials", 11.5, 0.06, 0.016),
    ("BPAC11", "BTG Pactual", "Financials", 34.0, 0.20, 0.018),
    ("ITSA4", "Itaúsa", "Financials", 9.8, 0.12, 0.013),
    ("BBSE3", "BB Seguridade", "Financials", 34.0, 0.10, 0.013),
    ("PSSA3", "Porto Seguro", "Financials", 30.0, 0.12, 0.015),
    ("CXSE3", "Caixa Seguridade", "Financials", 14.0, 0.14, 0.014),
    ("IRBR3", "IRB Brasil RE", "Financials", 38.0, -0.05, 0.030),
    # oil, gas & petro
    ("PETR4", "Petrobras PN", "Oil & Gas", 36.0, 0.10, 0.018),
    ("PETR3", "Petrobras ON", "Oil & Gas", 40.0, 0.10, 0.018),
    ("PRIO3", "PetroRio", "Oil & Gas", 44.0, 0.22, 0.024),
    ("RECV3", "PetroReconcavo", "Oil & Gas", 18.0, 0.05, 0.026),
    ("VBBR3", "Vibra Energia", "Oil & Gas", 22.0, 0.10, 0.020),
    ("UGPA3", "Ultrapar", "Oil & Gas", 24.0, 0.14, 0.019),
    ("CSAN3", "Cosan", "Oil & Gas", 12.0, -0.06, 0.024),
    ("RAIZ4", "Raízen", "Oil & Gas", 3.2, -0.12, 0.030),
    # mining, steel & materials
    ("VALE3", "Vale", "Materials", 62.0, -0.04, 0.017),
    ("CSNA3", "CSN", "Materials", 14.0, -0.02, 0.026),
    ("GGBR4", "Gerdau", "Materials", 18.0, 0.04, 0.020),
    ("GOAU4", "Metalúrgica Gerdau", "Materials", 10.0, 0.04, 0.022),
    ("USIM5", "Usiminas", "Materials", 7.0, -0.05, 0.030),
    ("BRAP4", "Bradespar", "Materials", 18.0, -0.03, 0.020),
    ("SUZB3", "Suzano", "Materials", 55.0, 0.08, 0.019),
    ("KLBN11", "Klabin", "Materials", 21.0, 0.06, 0.016),
    ("DXCO3", "Dexco", "Materials", 7.5, 0.05, 0.022),
    ("CBAV3", "CBA", "Materials", 6.5, -0.04, 0.030),
    # utilities & power
    ("ELET3", "Eletrobras ON", "Utilities", 42.0, 0.16, 0.018),
    ("ELET6", "Eletrobras PNB", "Utilities", 45.0, 0.16, 0.018),
    ("EQTL3", "Equatorial", "Utilities", 32.0, 0.12, 0.015),
    ("ENGI11", "Energisa", "Utilities", 45.0, 0.10, 0.014),
    ("ENEV3", "Eneva", "Utilities", 13.0, 0.08, 0.020),
    ("CMIG4", "Cemig", "Utilities", 11.0, 0.14, 0.016),
    ("CPLE6", "Copel", "Utilities", 10.0, 0.16, 0.016),
    ("CPFE3", "CPFL Energia", "Utilities", 34.0, 0.12, 0.013),
    ("TAEE11", "Taesa", "Utilities", 35.0, 0.08, 0.012),
    ("EGIE3", "Engie Brasil", "Utilities", 40.0, 0.10, 0.013),
    ("SBSP3", "Sabesp", "Utilities", 80.0, 0.22, 0.018),
    ("AURE3", "Auren Energia", "Utilities", 11.0, 0.04, 0.018),
    # consumer staples & food
    ("ABEV3", "Ambev", "Consumer Staples", 12.5, 0.02, 0.012),
    ("JBSS3", "JBS", "Consumer Staples", 32.0, 0.14, 0.018),
    ("MRFG3", "Marfrig", "Consumer Staples", 16.0, 0.06, 0.026),
    ("BEEF3", "Minerva", "Consumer Staples", 7.5, -0.04, 0.026),
    ("BRFS3", "BRF", "Consumer Staples", 22.0, 0.18, 0.022),
    ("SMTO3", "São Martinho", "Consumer Staples", 28.0, 0.06, 0.016),
    ("NTCO3", "Natura & Co", "Consumer Staples", 15.0, 0.05, 0.026),
    ("ASAI3", "Assaí", "Consumer Staples", 9.0, -0.08, 0.024),
    ("CRFB3", "Carrefour Brasil", "Consumer Staples", 9.5, -0.10, 0.022),
    ("PCAR3", "Pão de Açúcar", "Consumer Staples", 4.0, -0.18, 0.034),
    # consumer discretionary & retail
    ("LREN3", "Lojas Renner", "Consumer Disc.", 16.0, -0.02, 0.022),
    ("MGLU3", "Magazine Luiza", "Consumer Disc.", 11.0, -0.10, 0.035),
    ("AMER3", "Americanas", "Consumer Disc.", 0.8, -0.40, 0.060),
    ("PETZ3", "Petz", "Consumer Disc.", 5.0, -0.06, 0.026),
    ("CVCB3", "CVC Brasil", "Consumer Disc.", 2.4, -0.15, 0.038),
    ("SOMA3", "Grupo Soma", "Consumer Disc.", 7.0, 0.04, 0.026),
    ("COGN3", "Cogna", "Consumer Disc.", 2.8, -0.05, 0.030),
    ("YDUQ3", "Yduqs", "Consumer Disc.", 13.0, 0.02, 0.026),
    # industrials, transport & capital goods
    ("WEGE3", "WEG", "Industrials", 38.0, 0.18, 0.015),
    ("EMBR3", "Embraer", "Industrials", 38.0, 0.30, 0.022),
    ("RENT3", "Localiza", "Industrials", 52.0, 0.08, 0.020),
    ("RAIL3", "Rumo", "Industrials", 20.0, 0.10, 0.018),
    ("CCRO3", "CCR", "Industrials", 12.0, 0.06, 0.016),
    ("ECOR3", "EcoRodovias", "Industrials", 7.0, 0.10, 0.020),
    ("AZUL4", "Azul", "Industrials", 9.0, -0.25, 0.050),
    ("GOLL4", "Gol", "Industrials", 8.0, -0.30, 0.055),
    ("POMO4", "Marcopolo", "Industrials", 7.5, 0.18, 0.020),
    # real estate & construction
    ("MRVE3", "MRV", "Real Estate", 9.0, -0.04, 0.026),
    ("CYRE3", "Cyrela", "Real Estate", 21.0, 0.10, 0.022),
    ("EZTC3", "EZTec", "Real Estate", 15.0, -0.02, 0.024),
    ("MULT3", "Multiplan", "Real Estate", 24.0, 0.08, 0.016),
    ("IGTI11", "Iguatemi", "Real Estate", 22.0, 0.06, 0.018),
    ("JHSF3", "JHSF", "Real Estate", 4.5, 0.04, 0.026),
    # health care
    ("RDOR3", "Rede D'Or", "Health Care", 30.0, 0.14, 0.018),
    ("HAPV3", "Hapvida", "Health Care", 4.2, 0.10, 0.034),
    ("FLRY3", "Fleury", "Health Care", 16.0, 0.06, 0.016),
    ("RADL3", "Raia Drogasil", "Health Care", 26.0, 0.10, 0.015),
    ("HYPE3", "Hypera", "Health Care", 30.0, 0.04, 0.018),
    ("QUAL3", "Qualicorp", "Health Care", 2.0, -0.20, 0.040),
    # tech & telecom
    ("TOTS3", "Totvs", "Technology", 30.0, 0.16, 0.018),
    ("VIVT3", "Telefônica Vivo", "Telecom", 50.0, 0.10, 0.012),
    ("TIMS3", "TIM Brasil", "Telecom", 18.0, 0.12, 0.014),
    ("INTB3", "Intelbras", "Technology", 22.0, 0.10, 0.022),
    ("LWSA3", "Locaweb", "Technology", 6.0, -0.06, 0.030),
    # agribusiness
    ("SLCE3", "SLC Agrícola", "Agribusiness", 18.0, 0.04, 0.020),
    ("AGRO3", "BrasilAgro", "Agribusiness", 24.0, 0.06, 0.018),
]


def _spark(closes: list[float], n: int = 40) -> list[float]:
    if len(closes) <= n:
        return [r(c, 2) for c in closes]
    step = (len(closes) - 1) / (n - 1)
    return [r(closes[round(i * step)], 2) for i in range(n)]


def equity_universe():
    """Latest snapshot + sparkline for every B3-listed name (the whole exchange)."""
    start = date(2024, 6, 1)
    days = list(bdays(start, date(2026, 6, 26)))
    rows = []
    for symbol, name, sector, p0, drift, dvol in UNIVERSE:
        price = p0
        closes: list[float] = []
        rets: list[float] = []
        for _ in days:
            ret = drift / 252 + random.gauss(0, dvol)
            price = max(0.05, price * (1 + ret))
            closes.append(price)
            rets.append(ret * 100)
        last = closes[-1]
        prev = closes[-2] if len(closes) > 1 else last
        w52 = closes[-252:]
        vol21 = 0.0
        if len(rets) >= 21:
            w = rets[-21:]
            m = sum(w) / 21
            vol21 = math.sqrt(sum((x - m) ** 2 for x in w) / 21) * math.sqrt(252)
        rows.append({
            "symbol": f"{symbol}.SA",
            "name": name,
            "sector": sector,
            "close": r(last, 2),
            "daily_return_pct": r((last / prev - 1) * 100, 3),
            "vol_21d": r(vol21, 2),
            "high_52w": r(max(w52), 2),
            "low_52w": r(min(w52), 2),
            "spark": _spark(closes),
        })
    rows.sort(key=lambda x: x["name"])
    return rows


# --------------------------------------------------------------------------- #
# dim_series  (the FULL 48-series catalog, mirroring sources/registry.yml)
# --------------------------------------------------------------------------- #
# (series_id, name, domain, source, category, unit, frequency, fact)
CATALOG = [
    # ----- rates -----
    ("selic", "SELIC (Over, effective daily)", "rates", "bacen", "interest_rate", "% a.a.", "daily", "fact_observation"),
    ("cdi", "CDI (interbank deposit rate)", "rates", "bacen", "interest_rate", "% a.a.", "daily", "fact_observation"),
    ("over", "Over/SELIC (annualized)", "rates", "bacen", "interest_rate", "% a.a.", "daily", "fact_observation"),
    ("selic_meta", "SELIC target (Copom)", "rates", "bacen", "interest_rate", "% a.a.", "daily", "fact_observation"),
    ("tlp", "TLP (long-term rate)", "rates", "bacen", "interest_rate", "% a.a.", "daily", "fact_observation"),
    ("cdi_anual", "CDI (annualized, base 252)", "rates", "bacen", "interest_rate", "% a.a.", "daily", "fact_observation"),
    ("tr", "TR (reference rate)", "rates", "bacen", "interest_rate", "% m/m", "monthly", "fact_observation"),
    ("poupanca", "Poupança remuneration", "rates", "bacen", "interest_rate", "% m/m", "monthly", "fact_observation"),
    ("focus_selic_fim_ano", "Focus — Selic, end of year (median)", "rates", "bacen", "expectation", "% a.a.", "weekly", "fact_observation"),
    # ----- inflation -----
    ("ipca", "IPCA (headline CPI)", "inflation", "ibge", "price_index", "% m/m", "monthly", "fact_observation"),
    ("ipca_15", "IPCA-15 (CPI preview)", "inflation", "ibge", "price_index", "% m/m", "monthly", "fact_observation"),
    ("inpc", "INPC (CPI low-income)", "inflation", "ibge", "price_index", "% m/m", "monthly", "fact_observation"),
    ("igp_m", "IGP-M (general prices)", "inflation", "fgv", "price_index", "% m/m", "monthly", "fact_observation"),
    ("igp_di", "IGP-DI (general prices)", "inflation", "fgv", "price_index", "% m/m", "monthly", "fact_observation"),
    ("igp_10", "IGP-10 (general prices)", "inflation", "fgv", "price_index", "% m/m", "monthly", "fact_observation"),
    ("ipc_fipe", "IPC-FIPE (São Paulo CPI)", "inflation", "fgv", "price_index", "% m/m", "monthly", "fact_observation"),
    ("ipca_nucleo_ms", "IPCA core — trimmed, smoothed (MS)", "inflation", "bacen", "core_inflation", "% m/m", "monthly", "fact_observation"),
    ("ipca_nucleo_ma", "IPCA core — trimmed (MA)", "inflation", "bacen", "core_inflation", "% m/m", "monthly", "fact_observation"),
    ("ipca_nucleo_dp", "IPCA core — double weight (DP)", "inflation", "bacen", "core_inflation", "% m/m", "monthly", "fact_observation"),
    ("ipca_nucleo_ex3", "IPCA core — exclusion EX3", "inflation", "bacen", "core_inflation", "% m/m", "monthly", "fact_observation"),
    ("focus_ipca_12m", "Focus — IPCA 12m ahead (median)", "inflation", "bacen", "expectation", "%", "weekly", "fact_observation"),
    # ----- fx -----
    ("usd_brl", "USD/BRL (commercial buy, avg)", "fx", "bacen", "exchange_rate", "BRL", "daily", "fact_observation"),
    ("eur_brl", "EUR/BRL (commercial buy, avg)", "fx", "bacen", "exchange_rate", "BRL", "daily", "fact_observation"),
    ("usd_brl_compra", "USD/BRL (PTAX buy)", "fx", "bacen", "exchange_rate", "BRL", "daily", "fact_observation"),
    ("focus_cambio_fim_ano", "Focus — USD/BRL, end of year (median)", "fx", "bacen", "expectation", "BRL", "weekly", "fact_observation"),
    # ----- fiscal -----
    ("divida_pib", "Gross govt debt (DBGG)", "fiscal", "bacen", "fiscal", "% GDP", "monthly", "fact_observation"),
    ("dlsp_pib", "Net public-sector debt (DLSP)", "fiscal", "bacen", "fiscal", "% GDP", "monthly", "fact_observation"),
    ("ibc_br", "IBC-Br activity index (s.a.)", "fiscal", "bacen", "activity", "index", "monthly", "fact_observation"),
    ("resultado_primario", "Primary balance — public sector", "fiscal", "bacen", "fiscal", "R$ mn", "monthly", "fact_observation"),
    ("reservas_internacionais", "International reserves", "fiscal", "bacen", "fiscal", "US$ mn", "daily", "fact_observation"),
    ("ipea_nfsp_primario", "NFSP — primary result (IPEA)", "fiscal", "ipea", "fiscal", "R$ mn", "monthly", "fact_observation"),
    ("ipea_divida_liquida", "Net general govt debt (IPEA)", "fiscal", "ipea", "fiscal", "% GDP", "annual", "fact_observation"),
    ("ipea_pib", "GDP — current prices (IPEA)", "fiscal", "ipea", "macro", "R$ mn", "monthly", "fact_observation"),
    # ----- credit -----
    ("credito_total", "Total outstanding credit (SFN)", "credit", "bacen", "credit", "R$ mn", "monthly", "fact_observation"),
    ("inadimplencia_pf", "Credit default — households (PF)", "credit", "bacen", "credit", "%", "monthly", "fact_observation"),
    ("inadimplencia_pj", "Credit default — firms (PJ)", "credit", "bacen", "credit", "%", "monthly", "fact_observation"),
    # ----- market -----
    ("ibge", "Unemployment rate (PNAD Contínua)", "market", "ibge", "labor", "%", "monthly", "fact_observation"),
    ("anbima_ima_b", "ANBIMA IMA-B (NTN-B index)", "market", "anbima", "fixed_income_index", "index", "daily", "fact_observation"),
    ("anbima_ima_b5", "ANBIMA IMA-B 5 (short index)", "market", "anbima", "fixed_income_index", "index", "daily", "fact_observation"),
    ("anbima_irf_m", "ANBIMA IRF-M (fixed-rate index)", "market", "anbima", "fixed_income_index", "index", "daily", "fact_observation"),
    ("tesouro_direto", "Tesouro Direto (prices & yields)", "market", "tesouro", "government_bond", "mixed", "daily", "fact_treasury"),
    ("anbima", "ANBIMA secondary-market TPF", "market", "anbima", "fixed_income", "mixed", "daily", "fact_treasury"),
    ("b3", "B3 indices (Ibovespa)", "market", "yahoo", "equity_index", "points", "daily", "fact_security_price"),
    # ----- equities -----
    ("yahoo_etf", "Yahoo Finance ETFs", "equities", "yahoo", "etf", "price", "daily", "fact_security_price"),
    ("yahoo_commodity", "Yahoo Finance commodities", "equities", "yahoo", "commodity", "price", "daily", "fact_security_price"),
    ("yahoo_currency", "Yahoo Finance currencies", "equities", "yahoo", "currency", "price", "daily", "fact_security_price"),
    ("yahoo_global", "Global benchmarks", "equities", "yahoo", "benchmark", "mixed", "daily", "fact_security_price"),
    ("b3_cotahist", "B3 COTAHIST — cash-market OHLCV", "equities", "b3", "equity", "price", "daily", "fact_security_price"),
]

# series whose representative single value lives in a dedicated mart page rather
# than in fact_observation (multi-symbol / multi-bond facts) — the catalog still
# lists them, linking to the right page.
_NON_OBS = {"tesouro_direto", "anbima", "b3", "yahoo_etf", "yahoo_commodity",
            "yahoo_currency", "yahoo_global", "b3_cotahist"}


def dim_series():
    cols = ["series_id", "name", "domain", "source", "category", "unit", "frequency", "fact"]
    return [dict(zip(cols, x)) for x in CATALOG]


# --------------------------------------------------------------------------- #
# fact_observation  (silver long-format: one row per series_id x month)
#   schema mirrors s3://lakehouse/silver/fact_observation —
#   series_id, date, value, source. The dashboard's catalog reads this.
# --------------------------------------------------------------------------- #
def _interp(anchors, jitter=0.0, floor=None, ceil=None):
    out = interp_yearly(anchors, MONTHS, jitter)
    if floor is not None or ceil is not None:
        out = [max(floor, x) if floor is not None else x for x in out]
        out = [min(ceil, x) if ceil is not None else x for x in out]
    return out


def observation_paths():
    """Return {series_id: [value per month]} for every fact_observation series."""
    ipca_mom = [seasonal_ipca_mom(d, i12) for d, i12 in zip(MONTHS, IPCA12)]
    n = len(MONTHS)

    def noise(path, sd):
        return [v + random.gauss(0, sd) for v in path]

    def scaled(path, mul=1.0, add=0.0):
        return [v * mul + add for v in path]

    p = {
        # rates
        "selic": list(SELIC),
        "cdi": scaled(SELIC, add=-0.1),
        "over": list(SELIC),
        "selic_meta": [round(v * 4) / 4 for v in SELIC],  # quarter-point Copom steps
        "cdi_anual": scaled(SELIC, add=-0.1),
        "tlp": _interp({2000: 8.2, 2010: 7.0, 2018: 7.0, 2020: 4.8, 2022: 6.4, 2024: 6.8, 2026: 7.2}, 0.03),
        "tr": _interp({2000: 0.16, 2008: 0.12, 2013: 0.05, 2017: 0.0, 2020: 0.0, 2022: 0.06, 2026: 0.08}, 0.2, floor=0.0),
        "poupanca": _interp({2000: 0.72, 2010: 0.62, 2018: 0.5, 2020: 0.27, 2022: 0.6, 2026: 0.56}, 0.05, floor=0.1),
        "focus_selic_fim_ano": noise(SELIC, 0.25),
        # inflation
        "ipca": ipca_mom,
        "ipca_15": noise(ipca_mom, 0.08),
        "inpc": noise(ipca_mom, 0.10),
        "igp_m": [v * 1.4 + random.uniform(-0.6, 0.9) for v in ipca_mom],
        "igp_di": [v * 1.4 + random.uniform(-0.6, 0.9) for v in ipca_mom],
        "igp_10": [v * 1.4 + random.uniform(-0.6, 0.9) for v in ipca_mom],
        "ipc_fipe": noise(ipca_mom, 0.10),
        "ipca_nucleo_ms": noise(scaled(ipca_mom, mul=0.85), 0.05),
        "ipca_nucleo_ma": noise(scaled(ipca_mom, mul=0.85), 0.06),
        "ipca_nucleo_dp": noise(scaled(ipca_mom, mul=0.88), 0.05),
        "ipca_nucleo_ex3": noise(scaled(ipca_mom, mul=0.82), 0.06),
        "focus_ipca_12m": [max(2.0, v) for v in noise(scaled(IPCA12, mul=0.96), 0.15)],
        # fx
        "usd_brl": list(USDBRL_M),
        "eur_brl": scaled(USDBRL_M, mul=1.08),
        "usd_brl_compra": scaled(USDBRL_M, mul=0.998),
        "focus_cambio_fim_ano": noise(USDBRL_M, 0.08),
        # fiscal
        "divida_pib": list(DEBT),
        "dlsp_pib": scaled(DEBT, mul=0.8),
        "ibc_br": _interp({2003: 90, 2008: 105, 2014: 122, 2016: 112, 2019: 121, 2020: 110, 2022: 124, 2024: 129, 2026: 133}, 0.01),
        "resultado_primario": _interp({2000: 2500, 2008: 11000, 2014: -4000, 2016: -22000, 2020: -55000, 2022: 6000, 2024: -16000, 2026: -9000}, 0.25),
        "reservas_internacionais": _interp({2000: 33000, 2006: 60000, 2008: 200000, 2012: 372000, 2016: 360000, 2020: 355000, 2024: 350000, 2026: 345000}, 0.01),
        "ipea_nfsp_primario": _interp({2000: 2500, 2008: 11000, 2014: -4000, 2016: -22000, 2020: -55000, 2022: 6000, 2024: -16000, 2026: -9000}, 0.28),
        "ipea_divida_liquida": scaled(DEBT, mul=0.7),
        "ipea_pib": _interp({2000: 120000, 2005: 205000, 2010: 350000, 2015: 480000, 2020: 610000, 2024: 900000, 2026: 1010000}, 0.02),
        # credit
        "credito_total": _interp({2000: 320000, 2005: 620000, 2010: 1700000, 2015: 3100000, 2020: 4050000, 2024: 5500000, 2026: 5950000}, 0.01),
        "inadimplencia_pf": _interp({2000: 7.6, 2009: 8.0, 2012: 7.0, 2016: 6.2, 2019: 5.2, 2021: 4.2, 2023: 5.9, 2026: 5.4}, 0.03, floor=0.5),
        "inadimplencia_pj": _interp({2000: 3.6, 2009: 3.9, 2016: 3.5, 2019: 2.4, 2021: 1.6, 2023: 2.6, 2026: 2.4}, 0.04, floor=0.2),
        # market
        "ibge": _interp({2002: 12.0, 2008: 8.0, 2014: 6.8, 2017: 13.0, 2019: 11.5, 2021: 14.0, 2023: 8.0, 2026: 6.8}, 0.02, floor=3.0),
        "anbima_ima_b": _interp({2003: 1000, 2008: 1800, 2014: 3000, 2020: 6000, 2024: 8500, 2026: 9300}, 0.01),
        "anbima_ima_b5": _interp({2003: 1000, 2008: 1700, 2014: 2700, 2020: 5000, 2024: 6800, 2026: 7300}, 0.01),
        "anbima_irf_m": _interp({2003: 1000, 2008: 1900, 2014: 3100, 2020: 6200, 2024: 8800, 2026: 9600}, 0.01),
    }
    assert len(p) == 40, f"expected 40 observation paths, got {len(p)}"
    assert all(len(v) == n for v in p.values()), "all paths must align to MONTHS"
    return p


def fact_observation():
    paths = observation_paths()
    src = {row[0]: row[3] for row in CATALOG}
    # units that warrant fewer decimals (levels) vs many (rates / indices %).
    big = {"credito_total", "ipea_pib", "reservas_internacionais", "resultado_primario",
           "ipea_nfsp_primario", "anbima_ima_b", "anbima_ima_b5", "anbima_irf_m", "ibc_br"}
    rows = []
    for sid, path in paths.items():
        nd = 0 if sid in big else 4
        for d, v in zip(MONTHS, path):
            rows.append({"series_id": sid, "date": d.isoformat(), "value": r(v, nd), "source": src[sid]})
    return rows


# --------------------------------------------------------------------------- #
def main():
    print(f"generating synthetic snapshot -> {OUT}")
    write("mart_macro_dashboard", macro())
    write("mart_real_interest", real_interest())
    write("mart_inflation_panel", inflation_panel())
    write("mart_fx", fx())
    write("mart_yield_curve", yield_curve())
    write("mart_equity_daily", equity_daily())
    write("mart_equity_universe", equity_universe())
    write("dim_series", dim_series())
    write("fact_observation", fact_observation())
    meta = {
        "generated_from": "synthetic",
        "seed": SEED,
        "start": START.isoformat(),
        "end": END.isoformat(),
        "note": "Synthetic, real-shaped data. Replace with snapshot/export.py for live Delta.",
    }
    with open(os.path.join(OUT, "_meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
    print("done.")


if __name__ == "__main__":
    main()
