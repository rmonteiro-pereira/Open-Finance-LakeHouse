"""Parse RSS 2.0, RSS 1.0 (RDF) and Atom into plain dicts. Standard library only."""

from __future__ import annotations

import html
import re
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from xml.etree import ElementTree as ET

_TAG = re.compile(r"<[^>]+>")
_SPACE = re.compile(r"[ \t\r\f\v]+")
_BLANK = re.compile(r"\n\s*\n+")
_BLOCK = re.compile(r"</(p|div|li|h[1-6]|blockquote|tr)>|<br\s*/?>", re.I)
_DROP = re.compile(r"<(script|style|figure|iframe)\b.*?</\1>", re.I | re.S)
# UOL writes dates in Portuguese: "Qua, 07 Out 2026 18:31:37 -0300".
_PT = {"jan": "Jan", "fev": "Feb", "mar": "Mar", "abr": "Apr", "mai": "May", "jun": "Jun",
       "jul": "Jul", "ago": "Aug", "set": "Sep", "out": "Oct", "nov": "Nov", "dez": "Dec"}  # fmt: skip


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower()


def to_text(markup: str) -> str:
    """HTML fragment -> readable plain text, paragraphs kept as blank-line breaks."""
    text = _DROP.sub(" ", markup or "")
    text = _BLOCK.sub("\n\n", text)
    text = html.unescape(_TAG.sub(" ", text))
    text = _SPACE.sub(" ", text.replace("\xa0", " "))
    return _BLANK.sub("\n\n", "\n".join(line.strip() for line in text.split("\n"))).strip()


def parse_date(raw: str) -> str:
    """Any feed date -> ISO 8601 in UTC, or '' when it cannot be read."""
    raw = (raw or "").strip()
    if not raw:
        return ""
    try:
        when = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        parts = raw.split()
        if len(parts) >= 4 and parts[0].endswith(","):  # drop the weekday, translate the month
            parts = parts[1:]
            parts[1] = _PT.get(parts[1].lower()[:3], parts[1])
        try:
            when = parsedate_to_datetime(" ".join(parts))
        except (TypeError, ValueError):
            return ""
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return when.astimezone(UTC).isoformat(timespec="seconds")


def parse_feed(payload: bytes) -> list[dict]:
    """Feed bytes -> items with title, link, published, summary_html, body_html, author, categories."""
    root = ET.fromstring(payload.lstrip(b"\xef\xbb\xbf"))
    items = []
    for node in root.iter():
        if _local(node.tag) not in {"item", "entry"}:
            continue
        item: dict = {"title": "", "link": "", "published": "", "summary_html": "", "body_html": "",
                      "author": "", "categories": []}  # fmt: skip
        for child in node:
            name, text = _local(child.tag), (child.text or "").strip()
            if name == "title":
                item["title"] = to_text(text)
            elif name == "link":
                href = child.attrib.get("href")
                if href and child.attrib.get("rel", "alternate") == "alternate":
                    item["link"] = href.strip()
                elif text and not item["link"]:
                    item["link"] = text
            elif name in {"pubdate", "published", "date"} or (name == "updated" and not item["published"]):
                item["published"] = parse_date(text) or item["published"]
            elif name in {"description", "summary", "subtitle"}:
                item["summary_html"] = item["summary_html"] or text
            elif name in {"encoded", "content"}:
                item["body_html"] = text
            elif name in {"creator", "author"}:
                item["author"] = text or " ".join((sub.text or "").strip() for sub in child if _local(sub.tag) == "name")
            elif name == "category":
                value = text or child.attrib.get("term", "")
                if value:
                    item["categories"].append(value)
        if item["link"] and item["title"]:
            items.append(item)
    return items
