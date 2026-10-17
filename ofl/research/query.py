"""Read one pinned Delta snapshot with bounded filters and SQL parameters."""

from decimal import Decimal, localcontext


def per_unit(close, factor):
    if not factor or factor < 0:
        return None
    with localcontext() as context:
        context.prec = 38
        return (close / Decimal(factor)).quantize(Decimal("0.000000000001"))


def quote_query(
    db,
    *,
    symbol=None,
    market=None,
    date_from=None,
    date_to=None,
    currency=None,
    limit=100,
    offset=0,
    release=None,
    arrow=False,
):
    import duckdb
    from deltalake import DeltaTable

    if release:
        releases = db.rows("SELECT * FROM releases WHERE id=%s AND kind='quotes'", (release,))
    else:
        releases = db.rows(
            "SELECT r.* FROM pointers p JOIN releases r ON r.id=p.release_id WHERE p.kind='quotes'"
        )
    if not releases:
        return {"release": None, "rows": [], "qualification": "No quote release published"}
    current = releases[0]
    table = DeltaTable(current["delta_uri"], version=current["delta_version"])
    con = duckdb.connect()
    try:
        con.execute("SET memory_limit='512MB'")
        con.register("quotes", table.to_pyarrow_dataset())
        filters, params = [], []
        for column, value in (("symbol", symbol), ("tpmerc", market), ("currency", currency)):
            if value:
                filters.append(f"{column}=?")
                params.append(value)
        if date_from:
            filters.append("date>=?")
            params.append(date_from)
        if date_to:
            filters.append("date<=?")
            params.append(date_to)
        if date_from and date_to and str(date_from) > str(date_to):
            raise ValueError("date interval reversed")
        where = " AND ".join(filters) or "true"
        sql = f"""SELECT *
            FROM quotes WHERE {where} ORDER BY date,record_id LIMIT ? OFFSET ?"""
        cursor = con.execute(sql, [*params, limit, offset])
        if arrow:
            import pyarrow as pa

            result = cursor.to_arrow_table()
            derived = [
                per_unit(close, factor)
                for close, factor in zip(
                    result["close"].to_pylist(), result["quotation_factor"].to_pylist(), strict=True
                )
            ]
            return current["id"], result.append_column(
                "close_per_unit", pa.array(derived, type=pa.decimal128(38, 12))
            )
        names = [col[0] for col in cursor.description]
        rows = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
        for row in rows:
            row["close_per_unit"] = per_unit(row["close"], row["quotation_factor"])
        return {
            "release": current["id"],
            "rows": rows,
            "qualification": "Original currency and quotation basis; unadjusted prices",
            "limit": limit,
            "offset": offset,
        }
    finally:
        con.close()


def export_quotes(db, store, job_id, payload):
    # Bound export size and disclose truncation instead of unbounded web workloads.
    filters = payload.get("filters", {})
    if set(filters) - {"symbol", "market", "currency", "date_from", "date_to"}:
        raise ValueError("unsupported export filter")
    result = quote_query(db, **filters, release=payload["release"], limit=1_000_001, arrow=True)
    if not isinstance(result, tuple):
        raise ValueError("quote release unavailable")
    release, table = result
    if table.num_rows > 1_000_000:
        raise ValueError("export exceeds one million rows; narrow the date range")
    import pyarrow.parquet as pq

    key = f"exports/{job_id}.parquet"
    path = store.path(key)
    path.parent.mkdir(exist_ok=True)
    store.check_space(table.nbytes)
    pq.write_table(table, path)
    return {"release": release, "rows": table.num_rows, "key": key}
