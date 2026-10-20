"""Private-lane CLI; defaults never contact B3/MUB3 websites."""

import json
from pathlib import Path

from .config import ResearchSettings
from .storage import Store


def configure(subparsers):
    for name in ("b3", "archive", "research"):
        parser = subparsers.add_parser(name, help="private file-first research lane")
        parser.add_argument(
            "action",
            choices=("import", "coverage", "reprocess")
            if name != "research"
            else (
                "migrate",
                "worker",
                "serve",
                "coverage",
                "embed",
                "pilot",
                "evaluate",
                "evaluate-fixture",
                "index",
                "build",
                "mcp",
                "sync",
                "tag-jurisdiction",
                "rechunk",
            ),
        )
        parser.add_argument("path", nargs="?", type=Path)
        parser.add_argument("--source", choices=("b3_cotahist", "b3_bulletin", "mub3"))
        parser.add_argument("--metadata", type=Path, help="JSON provenance/catalog column mapping")
        parser.add_argument(
            "--supersedes", help="published import replaced by an equal-priority correction"
        )
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--once", action="store_true")
        parser.add_argument(
            "--lane", choices=("all", "default", "embedding", "checks"), default="all"
        )
        parser.add_argument(
            "--if-idle", action="store_true", help="embedding enqueue only: reuse an in-flight job"
        )
        parser.add_argument("--drain", action="store_true")
        parser.add_argument("--host", default="127.0.0.1")
        parser.add_argument("--port", type=int, default=8081)
        parser.add_argument("--limit", type=int, default=1000)
        parser.add_argument("--kind", choices=("news", "letters", "macro"))
        parser.add_argument("--collection")
        parser.add_argument("--sync-limit", type=int, default=20)
        parser.set_defaults(func=dispatch)


def dispatch(args):
    from .db import Database

    settings = ResearchSettings()
    if args.action == "mcp":
        from .mcp import create_mcp

        create_mcp(settings).run(transport="stdio")
        return 0
    db = Database(settings)
    store = (
        None
        if args.action in {"evaluate", "evaluate-fixture", "tag-jurisdiction", "rechunk"}
        else Store.from_settings(settings)
    )
    if args.action == "migrate":
        db.migrate()
        result = {"schema": 1}
    elif args.action == "import":
        if args.path is None:
            raise ValueError("input path required")
        source = args.source or ("b3_cotahist" if args.cmd == "b3" else "mub3")
        metadata = json.loads(args.metadata.read_text()) if args.metadata else {}
        if args.dry_run:
            if source == "b3_cotahist":
                import hashlib
                import tempfile

                from .cotahist import stage

                with args.path.open("rb") as stream:
                    sha = hashlib.file_digest(stream, "sha256").hexdigest()
                with tempfile.TemporaryDirectory() as tmp:
                    result = stage(
                        args.path,
                        Path(tmp),
                        sha,
                        input_name=args.path.name,
                        batch_rows=settings.batch_rows,
                        max_uncompressed_bytes=settings.max_uncompressed_bytes,
                    )
            else:
                result = {
                    "path": args.path.name,
                    "source": source,
                    "bytes": args.path.stat().st_size,
                    "qualification": "Registration preview; catalog/asset validation happens in the worker",
                }
        else:
            from .imports import register

            result = register(db, store, args.path, source, metadata)
    elif args.action == "reprocess":
        if args.path is None:
            raise ValueError("import identifier required")
        from .imports import reprocess

        result = reprocess(db, str(args.path), supersedes=args.supersedes)
    elif args.action == "worker":
        from .worker import run

        run(db, store, once=args.once, drain=args.drain, lane=args.lane)
        result = {"worker": "stopped"}
    elif args.action == "serve":
        import uvicorn

        from .api import create_app

        uvicorn.run(create_app(settings), host=args.host, port=args.port)
        return 0
    elif args.action == "sync":
        if not args.kind or not 1 <= args.sync_limit <= 200:
            raise ValueError("sync requires --kind and --sync-limit between 1 and 200")
        from .library import COLLECTION

        if args.collection and not COLLECTION.fullmatch(args.collection):
            raise ValueError("invalid collection")
        result = {
            "job_id": db.enqueue(
                "library_sync",
                {"kind": args.kind, "limit": args.sync_limit, "collection": args.collection},
            )
        }
    elif args.action == "coverage":
        from .imports import coverage

        result = coverage(db)
    elif args.action == "embed":
        result = {
            "job_id": (db.enqueue_if_idle if args.if_idle else db.enqueue)(
                "embed", {"limit": args.limit}
            )
        }
    elif args.action == "tag-jurisdiction":
        import sys

        from .jurisdiction import backfill

        if not 1 <= args.limit <= 10000:
            raise ValueError("tag-jurisdiction batch (--limit) must be between 1 and 10000")
        result = backfill(
            db,
            batch=args.limit,
            dry_run=args.dry_run,
            progress=lambda value: print(json.dumps(value), file=sys.stderr, flush=True),
        )
    elif args.action == "rechunk":
        import sys

        from .archive import rechunk

        if not 1 <= args.limit <= 10000:
            raise ValueError("rechunk batch (--limit) must be between 1 and 10000")
        result = rechunk(
            db,
            batch=args.limit,
            dry_run=args.dry_run,
            progress=lambda value: print(json.dumps(value), file=sys.stderr, flush=True),
        )
    elif args.action == "evaluate-fixture":
        import sys
        from .retrieval_fixture import evaluate_fixture

        result = evaluate_fixture(
            settings, progress=lambda value: print(json.dumps(value), file=sys.stderr, flush=True)
        )
    elif args.action == "evaluate":
        if args.path is None:
            raise ValueError("judged evaluation manifest required")
        from .retrieval_eval import evaluate

        result = evaluate(db, args.path, maximum=args.limit)
    elif args.action == "pilot":
        if args.path is None:
            raise ValueError("judged-query JSONL required")
        from .pilot import evaluate

        result = evaluate(db, args.path, maximum=args.limit)
    elif args.action == "index":
        from .search import create_vector_index

        result = {"index": create_vector_index(db)}
    else:
        result = {"job_id": db.enqueue("build_observations", {})}
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0
