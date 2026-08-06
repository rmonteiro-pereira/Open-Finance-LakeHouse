"""The MCP protocol shell — deliberately the thinnest thing in this package.

Everything that decides anything lives in ``tools.py``, which imports no SDK and is
therefore exercisable by ``pytest`` with nothing installed. This file only translates.
That split is why the eval suite is a gate in the core CI rather than a demo that needs
a protocol client to observe.

Reads ``ofl_public.duckdb`` (or a directory of tables) from a LOCAL release. It never
reaches the cluster: an agent surface that needs a VPN is one that dies with the homelab,
and `docs/GOLD_EXPORT.md` anticipated exactly this boundary before there was anything to
put on the other side of it.

Run:  ``python -m ofl.mcp.server --release ./out``   (needs ``pip install ofl[mcp]``)
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from ofl.mcp.tools import TOOLS, Refusal, ToolContext
from ofl.platform.logging import get_logger

log = get_logger(__name__)


def build_context(release_dir: str | Path) -> ToolContext:
    """Load a release into memory and derive the allowlist from what is actually there.

    The allowlist is derived, never hand-written: a hand-maintained list drifts away from
    the licence verdicts it is supposed to enforce, and the drift is silent.
    """
    import duckdb
    import polars as pl

    from ofl.registry import load_registry

    d = Path(release_dir)
    con = duckdb.connect()
    tables: list[str] = []
    for path in sorted(list(d.rglob("*.parquet")) + list(d.rglob("*.csv"))):
        df = pl.read_parquet(path) if path.suffix == ".parquet" else pl.read_csv(path, try_parse_dates=True)
        con.register(path.stem, df.to_arrow())
        tables.append(path.stem)

    series = [
        {
            "series_id": s.key,
            "name": s.name,
            "domain": s.domain,
            "unit": s.unit,
            "basis": s.basis,
            "scale": s.scale,
            "day_count": s.day_count,
            "horizon": s.horizon,
            "frequency": s.frequency,
            "provider": s.handler,
        }
        for s in load_registry().active()
    ]
    log.info("mcp_context", tables=len(tables), series=len(series))
    return ToolContext.from_catalog(con, series, tables)


def call(ctx: ToolContext, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    """Dispatch one tool call, turning a refusal into a payload rather than a stack trace.

    A refusal is an ANSWER — "I will not serve this, and here is the verdict that says
    so". Letting it surface as an exception would make it indistinguishable from a bug,
    and an agent cannot relay a bug to a user as a reason.
    """
    tool = TOOLS.get(name)
    if tool is None:
        return {"refused": True, "code": "unknown_tool", "reason": f"no tool named {name!r}"}
    try:
        return tool(ctx, **arguments)
    except Refusal as refusal:
        return refusal.as_dict()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="OFL MCP server (stdio)")
    parser.add_argument("--release", required=True, help="local release directory")
    args = parser.parse_args(argv)

    ctx = build_context(args.release)

    try:
        from mcp.server.fastmcp import FastMCP
    except ImportError:
        raise SystemExit(
            "the MCP SDK is not installed — `pip install ofl[mcp]`. The tools themselves "
            "do not need it: `python -c \"from ofl.mcp.tools import TOOLS\"` works without."
        ) from None

    app = FastMCP("ofl")
    for name, tool in TOOLS.items():
        app.add_tool(_bind(ctx, name, tool), name=name, description=(tool.__doc__ or "").strip())
    app.run()
    return 0


def _bind(ctx: ToolContext, name: str, tool):
    def _handler(**arguments: Any) -> dict[str, Any]:
        return call(ctx, name, arguments)

    _handler.__name__ = name
    _handler.__doc__ = tool.__doc__
    return _handler


if __name__ == "__main__":  # pragma: no cover - entrypoint
    raise SystemExit(main())
