"""Typed loader for ``sources/providers.yml`` — who owns each source, and what may ship.

Two rules live here rather than in the release builder, because a rule that lives in the
consumer is a rule each new consumer gets to forget:

* **Default deny.** :func:`is_redistributable` answers ``False`` for a handler with no
  entry. An unaudited term of use is not permission.
* **The provenance domain is closed.** :func:`assert_known_providers` rejects any value
  that is not one of the registered handler keys — so the licence gate cannot be silently
  bypassed by a typo, which would otherwise read as "no restricted provider found".
"""

from __future__ import annotations

from collections.abc import Iterable
from functools import lru_cache
from pathlib import Path

import yaml
from pydantic import BaseModel, Field

_DEFAULT_PROVIDERS = "sources/providers.yml"
_REPO_ROOT = Path(__file__).resolve().parent.parent

#: Authenticity of the values themselves, orthogonal to who owns them.
#:
#: ``sandbox`` is the ANBIMA case — format-real, value-fictitious. ``synthetic`` is
#: generated. Neither may be published in a production release, and the check is separate
#: from the licence check so that a source which becomes redistributable does not drag
#: fictitious values along with it.
DATA_CLASSES = ("live", "sandbox", "synthetic")


class Provider(BaseModel):
    """One source handler's ownership and verdict."""

    key: str
    rights_holder: str
    display_name: str = ""
    exhibit: bool = False
    redistribute: bool = False
    license_id: str = "unverified"
    url: str = ""
    verdict_date: str = ""
    verdict_by: str = ""
    notes: str = ""

    @property
    def state(self) -> str:
        """The three states the catalogue renders: ``open``/``restricted``/``unverified``."""
        if self.license_id == "unverified":
            return "unverified"
        return "open" if self.redistribute else "restricted"


class ProviderRegistry(BaseModel):
    version: int
    handlers: dict[str, Provider] = Field(default_factory=dict)

    def get(self, handler: str) -> Provider | None:
        return self.handlers.get(handler)

    def redistributable(self) -> set[str]:
        return {k for k, p in self.handlers.items() if p.redistribute}


@lru_cache
def load_providers(path: str | None = None) -> ProviderRegistry:
    p = Path(path or _DEFAULT_PROVIDERS)
    if not p.is_absolute() and not p.exists():
        p = _REPO_ROOT / p
    raw = yaml.safe_load(p.read_text(encoding="utf-8"))
    handlers = {k: Provider(key=k, **v) for k, v in (raw.get("handlers") or {}).items()}
    return ProviderRegistry(version=raw["version"], handlers=handlers)


def is_redistributable(handler: str) -> bool:
    """Deny by default: an unregistered handler is not publishable."""
    provider = load_providers().get(handler)
    return bool(provider and provider.redistribute)


def assert_known_providers(values: Iterable[str]) -> None:
    """Fail on a provenance value outside the registered handler keys.

    Without this, the licence gate's "no restricted provider present" is satisfied by a
    misspelled provider just as well as by a clean release.
    """
    known = set(load_providers().handlers)
    unknown = sorted({v for v in values if v not in known})
    if unknown:
        raise ValueError(
            f"provider value(s) outside the registered handlers: {unknown}. "
            f"Registered: {sorted(known)}"
        )


def assert_publishable(values: Iterable[str]) -> None:
    """Raise if any provenance value may not be redistributed. Domain is checked first."""
    values = list(values)
    assert_known_providers(values)
    blocked = sorted({v for v in values if not is_redistributable(v)})
    if blocked:
        registry = load_providers()
        detail = {
            v: f"{registry.handlers[v].license_id} ({registry.handlers[v].rights_holder})"
            for v in blocked
        }
        raise ValueError(f"non-redistributable provider(s) present: {detail}")
