"""``ofl`` command-line entrypoint.

Examples:
    ofl ingest --series selic
    ofl ingest --domain rates
    ofl ingest                 # all active series
    ofl registry               # list the registry
"""

from __future__ import annotations

import argparse

from ofl.platform.logging import configure_logging, get_logger
from ofl.registry import load_registry

log = get_logger("ofl.cli")


def _ingest(args: argparse.Namespace) -> int:
    from ofl.ingestion import run_ingestion

    reg = load_registry()
    if args.series:
        targets = [reg.series[args.series]]
    elif args.domain:
        targets = [s for s in reg.by_domain(args.domain) if s.is_active]
    else:
        targets = reg.active()

    results = [run_ingestion(s) for s in targets]
    landed = sum(1 for r in results if not r.get("skipped"))
    log.info("ingest_done", requested=len(targets), landed=landed)
    return 0


def _silver(_args: argparse.Namespace) -> int:
    from ofl.platform.spark import build_spark_session
    from ofl.transform.spark.silver import run_silver

    spark = build_spark_session("ofl-silver")
    try:
        result = run_silver(spark)
        log.info("silver_done", merged=result.get("merged"))
    finally:
        spark.stop()
    return 0


def _gold(args: argparse.Namespace) -> int:
    from ofl.transform.gold.runner import run_gold

    result = run_gold(write=not args.dry_run)
    log.info("gold_done", marts=result)
    return 0


def _stream_produce(args: argparse.Namespace) -> int:
    from ofl.streaming.producer import run_producer

    symbols = [s for s in (args.symbols or "").split(",") if s.strip()] or None
    run_producer(symbols, max_seconds=args.max_seconds, max_events=args.max_events)
    return 0


def _stream_bronze(args: argparse.Namespace) -> int:
    from ofl.streaming.bronze import build_streaming_session, run_bronze_stream

    spark = build_streaming_session()
    try:
        result = run_bronze_stream(
            spark, seconds=args.seconds, trigger_interval=args.trigger
        )
        log.info(
            "stream_bronze_done",
            batches=result["batches"],
            bronze_rows=result["bronze_rows"],
            dead_rows=result["dead_rows"],
        )
    finally:
        spark.stop()
    return 0


def _registry(_args: argparse.Namespace) -> int:
    reg = load_registry()
    for domain in reg.domains():
        members = ", ".join(s.key for s in reg.by_domain(domain))
        log.info("registry_domain", domain=domain, series=members)
    return 0


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    parser = argparse.ArgumentParser(prog="ofl", description="Open-Finance LakeHouse")
    sub = parser.add_subparsers(dest="cmd", required=True)

    ing = sub.add_parser("ingest", help="extract a source to the bronze layer")
    ing.add_argument("--series", help="single series key (e.g. selic)")
    ing.add_argument("--domain", help="all active series in a domain (e.g. rates)")
    ing.set_defaults(func=_ingest)

    sil = sub.add_parser("silver", help="conform bronze -> silver star schema (Spark)")
    sil.set_defaults(func=_silver)

    gold = sub.add_parser("gold", help="build DuckDB gold marts from silver")
    gold.add_argument("--dry-run", action="store_true", help="compute marts without writing")
    gold.set_defaults(func=_gold)

    prod = sub.add_parser("stream-produce", help="capture the live trade feed to _landing")
    prod.add_argument("--symbols", help="comma-separated, e.g. btcusdt,ethusdt")
    prod.add_argument("--max-seconds", type=float, default=120.0, help="wall-clock cap")
    prod.add_argument("--max-events", type=int, default=20_000, help="landed-event cap")
    prod.set_defaults(func=_stream_produce)

    sbr = sub.add_parser("stream-bronze", help="_landing -> bronze Delta (Spark streaming)")
    sbr.add_argument("--seconds", type=float, default=120.0, help="wall-clock cap")
    sbr.add_argument("--trigger", default="10 seconds", help="micro-batch interval")
    sbr.set_defaults(func=_stream_bronze)

    reg = sub.add_parser("registry", help="list the source registry")
    reg.set_defaults(func=_registry)

    args = parser.parse_args(argv)
    from ofl.platform.lineage import emit_run

    with emit_run(f"ofl_{args.cmd}"):
        return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
