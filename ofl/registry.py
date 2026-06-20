"""Typed loader for ``sources/registry.yml`` — the project's single source of truth.

This replaces Kedro's catalog + per-source ``parameters_*.yml`` sprawl. Ingestion,
Airflow DAG generation, and the silver ``dim_series`` dimension all read from here.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

_DEFAULT_REGISTRY = "sources/registry.yml"


class Series(BaseModel):
    """One registered data series (or grouped multi-symbol/​multi-bond source)."""

    key: str
    domain: str
    handler: str
    name: str
    category: str = "uncategorized"
    unit: str = "unknown"
    frequency: str = "unknown"
    fact: str = "observation"  # target silver fact: observation | security_price | treasury

    # handler-specific
    sgs_id: int | None = None
    symbols: list[dict[str, Any]] = Field(default_factory=list)
    bonds: list[dict[str, Any]] = Field(default_factory=list)

    max_value: float | None = None
    status: str = "active"  # active | planned
    extra: dict[str, Any] = Field(default_factory=dict)

    @property
    def is_active(self) -> bool:
        return self.status != "planned"


class Registry(BaseModel):
    version: int
    defaults: dict[str, Any] = Field(default_factory=dict)
    series: dict[str, Series]

    def active(self) -> list[Series]:
        return [s for s in self.series.values() if s.is_active]

    def by_domain(self, domain: str) -> list[Series]:
        return [s for s in self.series.values() if s.domain == domain]

    def by_handler(self, handler: str) -> list[Series]:
        return [s for s in self.series.values() if s.handler == handler]

    def domains(self) -> list[str]:
        return sorted({s.domain for s in self.series.values()})


def _coerce(key: str, body: dict[str, Any]) -> Series:
    known = set(Series.model_fields) - {"key", "extra"}
    fields = {k: v for k, v in body.items() if k in known}
    extra = {k: v for k, v in body.items() if k not in known}
    return Series(key=key, extra=extra, **fields)


def _default_path() -> str:
    # Lazy import so the registry can be loaded without pydantic-settings/env.
    try:
        from ofl.config import get_settings

        return get_settings().registry_path
    except Exception:
        return _DEFAULT_REGISTRY


@lru_cache
def load_registry(path: str | None = None) -> Registry:
    p = Path(path or _default_path())
    raw = yaml.safe_load(p.read_text(encoding="utf-8"))
    series = {key: _coerce(key, body) for key, body in raw.get("series", {}).items()}
    return Registry(version=raw["version"], defaults=raw.get("defaults", {}), series=series)
