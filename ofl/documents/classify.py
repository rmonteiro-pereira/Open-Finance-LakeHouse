"""Decide whether a listed document is a letter or report worth archiving.

Works on the file name and the listing title only: cheap, and good enough to keep
regulations, fact sheets and policies out. A wrong "keep" costs one stored PDF; a wrong
"drop" loses a letter, so the drop list is specific and the keep list is broad.
"""

from __future__ import annotations

import re
import unicodedata
from urllib.parse import unquote, urlparse

_DROP = re.compile(
    r"regulamento|regul_|lamina|politica|formulario|manual|codigo|termo|adesao|prospecto"
    r"|demonstrac|assembleia|\bata\b|edital|fato[-_ ]?relevante|balancete|\bvoto|compliance"
    r"|cadastr|suitability|privacidade|etica|risco|informe[-_ ]?(mensal|diario|trimestral)"
    r"|perfil[-_ ]?mensal|\bdfs?\b|tabela|ficha|kit\b|curriculo|logo|apresentacao[-_ ]?institucional"
)
_KEEP = re.compile(
    r"carta|comentario|coment_|relatorio|letter|report|gestao|gestor|mensal|trimestral"
    r"|semestral|anual|perspectiv|panorama|cenario|visao|call\b|tematic|\brg\b"
)


def _norm(text: str) -> str:
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    return re.sub(r"[_\-.%+]+", " ", text.lower())


def describe(url: str, title: str = "") -> str:
    name = unquote(urlparse(url).path.rsplit("/", 1)[-1])
    return _norm(f"{name} {title}")


def is_letter(url: str, title: str = "", *, listing_is_letters: bool = False) -> tuple[bool, str]:
    """Return (keep, reason). ``listing_is_letters``: the link came from a letters page."""
    text = describe(url, title)
    dropped = _DROP.search(text)
    if dropped:
        return False, f"looks like '{dropped.group(0).strip()}'"
    if listing_is_letters:
        return True, "listed on a letters page"
    kept = _KEEP.search(text)
    if kept:
        return True, f"matches '{kept.group(0).strip()}'"
    return False, "no letter-like word in name or title"
