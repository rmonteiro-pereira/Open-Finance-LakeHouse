"""Deterministic descriptive finance/data work. No arbitrary SQL or implicit prices."""

import csv
import io
from datetime import date
from decimal import Decimal, InvalidOperation, localcontext
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from .db import digest


def number(value):
    if (
        not isinstance(value, (str, int, Decimal))
        or isinstance(value, bool)
        or len(str(value)) > 80
    ):
        raise ValueError("provide a bounded decimal string")
    try:
        result = Decimal(str(value))
    except InvalidOperation:
        raise ValueError("invalid decimal") from None
    if not result.is_finite() or abs(result) > Decimal("1e24") or result.as_tuple().exponent < -18:
        raise ValueError("decimal exceeds supported bounds")
    return result


class SeriesSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    series_id: str = Field(pattern=r"^[A-Za-z][A-Za-z0-9_-]{0,127}$")
    operation: Literal["level", "difference", "percent_change", "index_100", "rolling_mean"] = (
        "level"
    )
    window: int = Field(3, ge=2, le=365)
    lag: int = Field(1, ge=1, le=365)


class DatasetSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    snapshot_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
    series: list[SeriesSpec] = Field(min_length=1, max_length=8)
    date_from: date | None = None
    date_to: date | None = None
    alignment: Literal["outer", "inner"] = "outer"
    # No filling: observations on mismatched calendars stay null.


def prepare_dataset(db, store, specification):
    from .macro import series_query

    spec = (
        specification
        if isinstance(specification, DatasetSpec)
        else DatasetSpec.model_validate(specification)
    )
    if spec.date_from and spec.date_to and spec.date_from > spec.date_to:
        raise ValueError("date interval reversed")
    names = [f"{s.series_id}__{s.operation}" for s in spec.series]
    if len(names) != len(set(names)):
        raise ValueError("duplicate output columns")
    columns, metadata, dates = {}, [], []
    with localcontext() as context:
        context.prec = 50
        for request, name in zip(spec.series, names, strict=True):
            source = series_query(
                db,
                store,
                request.series_id,
                snapshot_id=spec.snapshot_id,
                date_from=spec.date_from,
                date_to=spec.date_to,
                limit=100000,
            )
            if source["has_more"]:
                raise ValueError("dataset exceeds 100,000 source rows; narrow dates")
            values = [None if r["value"] is None else Decimal(r["value"]) for r in source["rows"]]
            transformed = []
            base = next((v for v in values if v is not None), None)
            for i, value in enumerate(values):
                result = value
                if request.operation in {"difference", "percent_change"}:
                    prev = values[i - request.lag] if i >= request.lag else None
                    result = None if value is None or prev is None else value - prev
                    if request.operation == "percent_change":
                        result = None if result is None or prev == 0 else result / prev * 100
                elif request.operation == "index_100":
                    result = None if value is None or base in (None, 0) else value / base * 100
                elif request.operation == "rolling_mean":
                    window = values[max(0, i - request.window + 1) : i + 1]
                    result = (
                        None
                        if len(window) != request.window or any(v is None for v in window)
                        else sum(window) / request.window
                    )
                transformed.append(None if result is None else str(result))
            columns[name] = {
                str(row["date"]): value
                for row, value in zip(source["rows"], transformed, strict=True)
            }
            dates.append(set(columns[name]))
            metadata.append(
                {
                    "column": name,
                    "source": source["series"],
                    "source_dataset_sha256": source["source_dataset_sha256"],
                    "operation": request.model_dump(),
                    "output_unit": "%"
                    if request.operation == "percent_change"
                    else "index"
                    if request.operation == "index_100"
                    else source["series"].get("unit"),
                }
            )
    aligned = set.intersection(*dates) if spec.alignment == "inner" else set.union(*dates)
    if len(aligned) > 100000:
        raise ValueError("aligned dataset exceeds 100,000 rows")
    rows = [
        {"date": day, **{name: values.get(day) for name, values in columns.items()}}
        for day in sorted(aligned)
    ]
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["date", *names])
    for row in rows:
        writer.writerow([row["date"], *[row[name] for name in names]])
    content = output.getvalue()
    import hashlib

    manifest = {
        "specification": spec.model_dump(mode="json"),
        "columns": metadata,
        "rows": len(rows),
        "csv_sha256": hashlib.sha256(content.encode()).hexdigest(),
        "method": "ofl/descriptive-dataset/1",
        "null_policy": "No imputation. Inner joins intersect observation dates, including dates with null values. Lags/windows count source observations within selected dates; no hidden warm-up.",
        "qualification": "Macro source numeric representation; descriptive transformations, not adjusted asset returns, causal estimates or historical-availability backtests.",
    }
    return {"manifest": manifest, "preview": rows[:200], "csv": content}


def portfolio(body):
    positions = body.get("positions", [])
    if not isinstance(positions, list) or not 1 <= len(positions) <= 200:
        raise ValueError("provide 1–200 valued positions")
    currency = body.get("currency")
    if not isinstance(currency, str) or not 1 <= len(currency) <= 16:
        raise ValueError("base currency required")
    try:
        valuation_date = date.fromisoformat(str(body.get("valuation_date", "")))
    except ValueError:
        raise ValueError("valuation date required") from None
    result, ids = [], set()
    with localcontext() as context:
        context.prec = 50
        for item in positions:
            if not isinstance(item, dict):
                raise ValueError("position must be an object")
            identity = tuple(
                str(item.get(k, "")).strip() for k in ("instrument_id", "market", "currency")
            )
            if not all(identity) or identity in ids or any(len(v) > 128 for v in identity):
                raise ValueError("distinct instrument identity, market and currency required")
            ids.add(identity)
            if identity[2] != currency:
                raise ValueError(
                    "convert positions explicitly to one valuation currency before analysis"
                )
            value = number(item.get("market_value"))
            if value < 0:
                raise ValueError("this descriptive workflow supports long-only valued holdings")
            exposure = item.get("asset_class", "unknown")
            if not isinstance(exposure, str) or not 1 <= len(exposure) <= 80:
                raise ValueError("bounded asset_class required")
            result.append(
                {
                    "instrument_id": identity[0],
                    "market": identity[1],
                    "currency": currency,
                    "asset_class": exposure,
                    "market_value": str(value),
                }
            )
        total = sum(Decimal(p["market_value"]) for p in result)
        if total <= 0:
            raise ValueError("positive portfolio value required")
        groups = {}
        for row in result:
            row["weight_percent"] = str(Decimal(row["market_value"]) / total * 100)
            groups[row["asset_class"]] = groups.get(row["asset_class"], Decimal(0)) + Decimal(
                row["market_value"]
            )
        scenarios = body.get("scenarios", [])
        if not isinstance(scenarios, list) or len(scenarios) > 20:
            raise ValueError("at most 20 scenarios")
        scenario_results = []
        for scenario in scenarios:
            if (
                not isinstance(scenario, dict)
                or not isinstance(scenario.get("name"), str)
                or not 1 <= len(scenario["name"]) <= 120
            ):
                raise ValueError("scenario name required")
            shocks = scenario.get("shocks", {})
            if not isinstance(shocks, dict) or set(shocks) != set(groups):
                raise ValueError(
                    "specify an explicit percentage shock for every asset class, including zero"
                )
            parsed = {key: number(value) for key, value in shocks.items()}
            if any(value < -100 or value > 1000 for value in parsed.values()):
                raise ValueError("scenario shock outside supported range [-100,1000] percent")
            change = sum(value * parsed[key] / 100 for key, value in groups.items())
            scenario_results.append(
                {
                    "name": scenario["name"],
                    "assumptions": {k: str(v) for k, v in parsed.items()},
                    "value_change": str(change),
                    "projected_value": str(total + change),
                    "change_percent": str(change / total * 100),
                }
            )
    return {
        "valuation_date": str(valuation_date),
        "currency": currency,
        "total_value": str(total),
        "positions": result,
        "exposures": [
            {
                "asset_class": key,
                "market_value": str(value),
                "weight_percent": str(value / total * 100),
            }
            for key, value in groups.items()
        ],
        "largest_position_percent": max(result, key=lambda p: Decimal(p["weight_percent"]))[
            "weight_percent"
        ],
        "scenarios": scenario_results,
        "input_sha256": digest(body),
        "method": "ofl/valued-long-only-portfolio/1",
        "qualification": "User-supplied valuations and classifications, not verified market quotes. Linear hypothetical shocks are assumptions, not forecasts, suitability recommendations or historical performance. No automatic FX conversion, look-through, derivatives or corporate-action adjustment.",
    }


def compare_quotes(db, *, symbol, market, date_from, date_to, release):
    from .query import quote_query

    result = quote_query(
        db,
        symbol=symbol,
        market=market,
        date_from=date_from,
        date_to=date_to,
        release=release,
        limit=10001,
    )
    rows = result["rows"]
    if not rows or len(rows) > 10000:
        raise ValueError("no quotes or more than 10,000 rows; narrow interval")
    from .cotahist import IDENTITY

    identities = {tuple(str(r.get(k)) for k in IDENTITY if k != "date") for r in rows}

    if (
        len(identities) != 1
        or not all(r.get("isin") for r in rows)
        or len({r["date"] for r in rows}) != len(rows)
    ):
        raise ValueError(
            "ambiguous instrument identity, duplicate dates or quotation basis changed"
        )
    first, last = rows[0], rows[-1]
    if (
        first["date"] == last["date"]
        or first["close_per_unit"] in (None, 0)
        or last["close_per_unit"] is None
    ):
        raise ValueError("two distinct dates and nonzero valid quotation basis required")
    with localcontext() as ctx:
        ctx.prec = 50
        change = last["close_per_unit"] - first["close_per_unit"]
        percent = change / first["close_per_unit"] * 100
    return {
        "release": result["release"],
        "first": first,
        "last": last,
        "difference_per_unit": str(change),
        "price_change_percent": str(percent),
        "qualification": "Unadjusted observed price change, not total return. Equal identity does not establish absence of corporate actions. No reinvestment, costs or historical strategy claim.",
    }


def release_diff(db, *, left, right, symbol, market, date_from, date_to):
    from .query import quote_query

    datasets = [
        quote_query(
            db,
            release=release,
            symbol=symbol,
            market=market,
            date_from=date_from,
            date_to=date_to,
            limit=10001,
        )
        for release in (left, right)
    ]
    if any(d["release"] is None or len(d["rows"]) > 10000 for d in datasets):
        raise ValueError("release unavailable or diff exceeds 10,000 rows")

    def keyed(rows):
        result = {}
        for row in rows:
            key = tuple(
                str(row.get(k))
                for k in (
                    "date",
                    "isin",
                    "distribution",
                    "tpmerc",
                    "symbol",
                    "currency",
                    "quotation_factor",
                    "specification",
                    "expiry_raw",
                    "exercise_price",
                    "forward_term",
                )
            )
            if key in result:
                raise ValueError("ambiguous records within a release")
            result[key] = row
        return result

    a, b = [keyed(d["rows"]) for d in datasets]
    changes = []
    for key in sorted(a.keys() | b.keys()):
        before, after = a.get(key), b.get(key)
        # record IDs/import provenance differ across releases without an economic change.
        fields = sorted(
            k
            for k in (set(before or {}) | set(after or {}))
            - {"record_id", "import_id", "source_hash", "source_line"}
            if (before or {}).get(k) != (after or {}).get(k)
        )
        if before is None or after is None or fields:
            changes.append(
                {
                    "identity": list(key),
                    "state": "added"
                    if before is None
                    else "removed"
                    if after is None
                    else "changed",
                    "fields": fields,
                    "before": before,
                    "after": after,
                }
            )
    return {
        "left_release": left,
        "right_release": right,
        "scope": {"symbol": symbol, "market": market, "date_from": date_from, "date_to": date_to},
        "changes": changes,
        "qualification": "Record-level observed release differences within explicit scope; not a corporate-action continuity proof.",
    }
