"""Only approved, explicitly mapped table cells become financial observations."""

import calendar
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from psycopg.types.json import Jsonb

from .cotahist import FUTURES_MARKETS, MARKETS
from .db import Database, digest


def observation_period(text, format):
    if format not in {"%Y-%m-%d", "%d/%m/%Y", "%Y-%m", "%m/%Y", "%Y"}:
        raise ValueError("unsupported date format")
    start = datetime.strptime(text, format).date()
    precision = "year" if format == "%Y" else "month" if format in {"%Y-%m", "%m/%Y"} else "day"
    end = (
        date(start.year, 12, 31)
        if precision == "year"
        else date(start.year, start.month, calendar.monthrange(start.year, start.month)[1])
        if precision == "month"
        else start
    )
    return start, end, precision


def normalize(cells, mapping):
    if not isinstance(mapping.get("header_rows", 1), int) or mapping.get("header_rows", 1) < 0:
        raise ValueError("header_rows must be a nonnegative integer")
    required = {"date", "subject", "metric", "value", "unit", "market"}
    if not required <= mapping.keys():
        raise ValueError("mapping requires date, subject, metric, value, unit, market")

    def field(row, key):
        config = mapping.get(key)
        if not isinstance(config, dict) or not ({"column", "constant"} & config.keys()):
            raise ValueError(f"{key}: specify column or constant")
        if "column" in config:
            index = config["column"]
            if not isinstance(index, int) or index < 0 or index >= len(row):
                raise ValueError(f"{key}: invalid column")
            return str(row[index] or "").strip()
        return str(config["constant"]).strip()

    output = []
    for row_index, row in enumerate(
        cells[mapping.get("header_rows", 1) :], mapping.get("header_rows", 1)
    ):
        if not any(str(v or "").strip() for v in row):
            continue
        market = field(row, "market")
        if market not in MARKETS:
            raise ValueError(f"row {row_index}: unknown market")
        if market in FUTURES_MARKETS:
            continue
        date_text = field(row, "date")
        day, date_end, date_precision = observation_period(
            date_text, mapping.get("date_format", "%Y-%m-%d")
        )
        raw_value = field(row, "value")
        decimal_separator = mapping.get("decimal_separator", ".")
        thousands = mapping.get("thousands_separator", "")
        if decimal_separator not in {".", ","} or thousands == decimal_separator:
            raise ValueError("invalid decimal/thousands separators")
        if thousands:
            raw_value = raw_value.replace(thousands, "")
        if decimal_separator == ",":
            raw_value = raw_value.replace(",", ".")
        if raw_value.startswith("(") and raw_value.endswith(")"):
            raw_value = "-" + raw_value[1:-1]
        try:
            value = Decimal(raw_value)
        except InvalidOperation as exc:
            raise ValueError(f"row {row_index}: invalid numeric value") from exc
        if (
            not value.is_finite()
            or value.as_tuple().exponent < -12
            or abs(value) >= Decimal("1e26")
        ):
            raise ValueError("value outside exact observation precision")
        values = {k: field(row, k) for k in ("subject", "metric", "unit")}
        if any(not v or v.lower() in {"unknown", "?"} for v in values.values()):
            raise ValueError("subject, metric and unit must be resolved")
        output.append(
            {
                "date": day,
                "date_end": date_end,
                "date_precision": date_precision,
                "date_text": date_text,
                "market": market,
                "value": value,
                **values,
                "currency": field(row, "currency") if "currency" in mapping else None,
                "row": row_index,
                "value_column": mapping["value"].get("column"),
            }
        )
    return output


def submit_review(db, candidate_id, *, actor, expected_revision, cells, mapping, state):
    if state not in {"approved", "rejected", "needs_review"}:
        raise ValueError("invalid review state")
    if len(cells) > 5000 or any(len(row) > 200 for row in cells):
        raise ValueError("table too large")
    rows = normalize(cells, mapping) if state == "approved" else []
    if state == "approved" and not rows:
        raise ValueError("approval must contain non-futures observations")
    with db.connect() as con:
        candidate = con.execute(
            "SELECT * FROM candidates WHERE id=%s FOR UPDATE", (candidate_id,)
        ).fetchone()
        if candidate is None:
            raise LookupError("candidate not found")
        if candidate["current_revision"] != expected_revision:
            raise FileExistsError("review changed; reload before saving")
        revision = expected_revision + 1
        con.execute(
            "INSERT INTO reviews(candidate_id,revision,actor,state,cells,mapping) VALUES (%s,%s,%s,%s,%s,%s)",
            (candidate_id, revision, actor, state, Jsonb(cells), Jsonb(mapping)),
        )
        con.execute("UPDATE observations SET active=false WHERE candidate_id=%s", (candidate_id,))
        for row in rows:
            evidence = {
                "asset_id": candidate["asset_id"],
                "page": candidate["page"],
                "bbox": candidate["bbox"],
                "row": row["row"],
                "column": row["value_column"],
                "coordinate_space": candidate["evidence"].get("coordinate_space", "source_table"),
            }
            boxes = candidate["evidence"].get("cell_boxes", [])
            if (
                row["value_column"] is not None
                and row["row"] < len(boxes)
                and row["value_column"] < len(boxes[row["row"]])
            ):
                evidence["cell_bbox"] = boxes[row["row"]][row["value_column"]]
            con.execute(
                """INSERT INTO observations(id,candidate_id,revision,date,subject,market,metric,value,unit,currency,evidence,date_end,date_precision,date_text)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    digest([candidate_id, revision, row["row"]]),
                    candidate_id,
                    revision,
                    row["date"],
                    row["subject"],
                    row["market"],
                    row["metric"],
                    row["value"],
                    row["unit"],
                    row["currency"],
                    Jsonb(evidence),
                    row["date_end"],
                    row["date_precision"],
                    row["date_text"],
                ),
            )
        con.execute(
            "UPDATE candidates SET state=%s,current_revision=%s WHERE id=%s",
            (state, revision, candidate_id),
        )
        con.execute(
            "INSERT INTO audit(actor,action,target,detail) VALUES (%s,'review',%s,%s)",
            (
                actor,
                candidate_id,
                Jsonb({"revision": revision, "state": state, "observations": len(rows)}),
            ),
        )
        job = Database.enqueue_on(con, "build_observations", {}, actor)
    return {"revision": revision, "observations": len(rows), "job_id": job}
