"""The list of managers and how each one publishes (``sources/letters.yml``)."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field

DEFAULT_PATH = Path(__file__).resolve().parents[2] / "sources" / "letters.yml"


class Collector(BaseModel):
    """One way of listing a manager's documents.

    ``wp_media``: the WordPress media library over its public REST API. Lists every
    PDF on the site, so candidates must look like a letter to be kept.
    ``page_links``: PDF links on pages that are themselves a letters listing, so every
    link is kept unless it is plainly something else.
    """

    kind: Literal["wp_media", "page_links"]
    base: str | None = None
    urls: list[str] = Field(default_factory=list)


class Manager(BaseModel):
    id: str
    name: str
    collectors: list[Collector]


def load_managers(path: Path | None = None) -> list[Manager]:
    data = yaml.safe_load((path or DEFAULT_PATH).read_text(encoding="utf-8"))
    managers = [Manager(**m) for m in data["managers"]]
    ids = [m.id for m in managers]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate manager id in letters.yml")
    return managers
