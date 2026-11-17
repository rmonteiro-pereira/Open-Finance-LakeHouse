"""Container dependency sets must retain the portable Polars runtime."""

import re
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_locked_core_dependencies_include_compatible_polars():
    """Check the core runtime dependency without importing native libraries."""
    lock = tomllib.loads((REPO_ROOT / "uv.lock").read_text(encoding="utf-8"))
    packages = {package["name"]: package for package in lock["package"]}
    project = packages["ofl"]
    polars_dependency = next(
        dependency for dependency in project["dependencies"] if dependency["name"] == "polars"
    )
    assert "rtcompat" in polars_dependency.get("extra", [])
    polars = packages["polars"]
    assert {"name": "polars-runtime-compat"} in polars["optional-dependencies"]["rtcompat"]
    runtime = packages["polars-runtime-compat"]
    assert runtime["version"] == polars["version"]
    assert any(
        re.search(r"manylinux[^/]*_x86_64\.whl$", wheel["url"]) for wheel in runtime["wheels"]
    )
