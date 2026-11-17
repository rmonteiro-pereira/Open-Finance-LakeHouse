"""Container dependency sets must retain the portable Polars runtime."""

import tomllib
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    ("dockerfile", "extras"),
    [
        ("Dockerfile", ("yahoo", "lineage")),
        ("Dockerfile.spark", ("spark",)),
        ("Dockerfile.research", ("research", "research-ocr")),
        ("Dockerfile.research", ("research", "research-ocr", "research-search")),
    ],
)
def test_locked_image_dependencies_include_compatible_polars(dockerfile, extras):
    """Check the shared runtime dependency without importing native libraries."""
    lock = tomllib.loads((REPO_ROOT / "uv.lock").read_text(encoding="utf-8"))
    packages = {package["name"]: package for package in lock["package"]}
    project = packages["ofl"]
    polars_dependency = next(
        dependency for dependency in project["dependencies"] if dependency["name"] == "polars"
    )
    assert "rtcompat" in polars_dependency.get("extra", [])
    for extra in extras:
        assert extra in project["optional-dependencies"]
    polars = packages["polars"]
    assert {"name": "polars-runtime-compat"} in polars["optional-dependencies"]["rtcompat"]
    runtime = packages["polars-runtime-compat"]
    assert runtime["version"] == polars["version"]
    assert any("manylinux2014_x86_64" in wheel["url"] for wheel in runtime["wheels"])
    image = (REPO_ROOT / "docker" / dockerfile).read_text(encoding="utf-8")
    assert "uv.lock" in image
    assert "uv sync --locked --no-dev" in image
    assert "uv pip install" not in image
    for extra in extras:
        assert f"--extra {extra}" in image
