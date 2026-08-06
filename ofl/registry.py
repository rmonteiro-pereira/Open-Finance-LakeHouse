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

    # `unit` alone was never a unit. It carried `percent` for the SELIC daily rate
    # (~0.05), monthly IPCA variation (~0.4), the unemployment level (~7) and
    # debt/GDP (~75) — four quantities that share nothing but a symbol. A guard keyed
    # on it lets a %/day series and a %/month series onto the same axis.
    #
    # The quantity is the tuple:
    #   unit       what the number is measured in
    #   basis      over what period, or that it is a level
    #   scale      power of ten between the stored value and `unit` — this is what
    #              stops "R$ million" from becoming a contract that claims reais
    #   day_count  252 / 360 / 365 / calendar, for anything compounded
    #   horizon    spot vs an expectation; a Focus median and a realised rate are
    #              otherwise indistinguishable
    #
    # For a series that is a GROUP of symbols or a multi-column fact (treasury,
    # security_price, ...) the registry row does not describe one quantity, so it
    # declares `unit_scope: per_column` and the units live in the table's contract.
    unit: str = "unknown"
    basis: str | None = None
    scale: int = 1
    day_count: str | None = None
    horizon: str | None = None
    unit_scope: str | None = None

    frequency: str = "unknown"
    fact: str = "observation"  # target silver fact: observation | security_price | treasury

    # handler-specific
    sgs_id: int | None = None
    symbols: list[dict[str, Any]] = Field(default_factory=list)
    bonds: list[dict[str, Any]] = Field(default_factory=list)

    max_value: float | None = None
    start_date: str | None = None  # ISO floor for the backfill walk (resolved from handler default)
    status: str = "active"  # active | planned
    # Optional per-series freshness budget (e.g. "40d", "72h") for the staleness
    # alert. When unset, the alert rule falls back to a threshold keyed off
    # `frequency` (daily/monthly/...). Irregular release-calendar series
    # (divida_pib, reservas_internacionais, anbima, focus_*) should set this.
    freshness_sla: str | None = None
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

    def handlers(self) -> list[str]:
        """Distinct source handlers — the per-source DAG (blast-radius) axis."""
        return sorted({s.handler for s in self.series.values()})


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


#: Repo root (parent of the ``ofl`` package) — used to resolve a relative
#: registry path independently of the process CWD.
_REPO_ROOT = Path(__file__).resolve().parent.parent


@lru_cache
def load_registry(path: str | None = None) -> Registry:
    p = Path(path or _default_path())
    # A relative path (the default) is resolved against the repo root, not the
    # CWD: the Airflow dag-processor parses DAGs from a different CWD and its
    # image lacks pydantic-settings (so the OFL_REGISTRY override is unavailable),
    # which otherwise leaves the bare "sources/registry.yml" unresolvable.
    if not p.is_absolute() and not p.exists():
        p = _REPO_ROOT / p
    raw = yaml.safe_load(p.read_text(encoding="utf-8"))
    defaults = raw.get("defaults", {})
    series = {key: _coerce(key, body) for key, body in raw.get("series", {}).items()}
    # Resolve handler-level defaults onto each series (per-series value wins).
    for s in series.values():
        if s.start_date is None:
            s.start_date = defaults.get(s.handler, {}).get("start_date")
    return Registry(version=raw["version"], defaults=defaults, series=series)
