"""Owned versioned research artifacts; source evidence never becomes user-authored fact."""

import json
from psycopg.types.json import Jsonb
from .db import digest, uid

KINDS = {"investigation", "portfolio", "watchlist", "check", "dataset"}


def owned(con, actor, workspace_id):
    item = con.execute(
        "SELECT * FROM workspaces WHERE id=%s AND owner=%s FOR UPDATE", (workspace_id, actor)
    ).fetchone()
    if not item:
        raise LookupError("workspace not found")
    return item


def evidence(con, references):
    if len(references) > 50 or len(set(references)) != len(references):
        raise ValueError("provide at most 50 distinct passage references")
    result = []
    for passage_id in references:
        row = con.execute(
            """SELECT p.id,p.text,p.revision,p.method,p.page,p.asset_id,p.item_id,
          i.title,i.source,i.date_start,i.date_end,i.date_precision,i.metadata,
          a.sha256 FROM passages p JOIN items i ON i.id=p.item_id
          LEFT JOIN assets a ON a.id=p.asset_id WHERE p.id=%s AND p.active
          AND (p.asset_id IS NULL OR a.active)""",
            (passage_id,),
        ).fetchone()
        if not row:
            raise ValueError("evidence reference unavailable")
        result.append(json.loads(json.dumps(row, default=str)))
    return result


def validate(con, kind, body, *, generated=False):
    if kind == "check" and not generated:
        body = {k: v for k, v in body.items() if k not in {"result", "comparison", "executed_at"}}
    if len(json.dumps(body, ensure_ascii=False)) > 500000:
        raise ValueError("artifact exceeds 500KB")
    if kind == "investigation":
        question = body.get("question", "")
        if not isinstance(question, str) or not 1 <= len(question) <= 2000:
            raise ValueError("investigation needs a question")
        refs = body.get("references", [])
        if not isinstance(refs, list) or any(not isinstance(x, str) for x in refs):
            raise ValueError("invalid references")
        captured = evidence(con, refs)
        findings = body.get("findings", [])
        if not isinstance(findings, list) or len(findings) > 50:
            raise ValueError("at most 50 findings")
        source = {r["id"]: r for r in captured}
        for finding in findings:
            if (
                not isinstance(finding, dict)
                or not isinstance(finding.get("kind"), str)
                or finding.get("kind")
                not in {
                    "observation",
                    "interpretation",
                    "counterpoint",
                    "limitation",
                }
            ):
                raise ValueError("finding kind required")
            text = finding.get("text", "")
            if not isinstance(text, str) or not 1 <= len(text) <= 4000:
                raise ValueError("finding text required")
            citations = finding.get("references", [])
            if not isinstance(citations, list) or any(
                not isinstance(ref, str) or ref not in source for ref in citations
            ):
                raise ValueError("finding cites uncaptured evidence")
            if finding["kind"] == "observation" and (
                not citations or not any(text in source[r]["text"] for r in citations)
            ):
                raise ValueError(
                    "observations must quote captured source text exactly; use interpretation otherwise"
                )
            finding["status"] = (
                "source_excerpt" if finding["kind"] == "observation" else "user_authored_unverified"
            )
        # Captures are server-resolved. Never accept caller-supplied provenance.
        return {**body, "evidence": captured, "findings": findings}
    if kind == "portfolio":
        from .analytics import portfolio

        return {**body, "analysis": portfolio(body)}
    if kind == "watchlist":
        entries = body.get("entries", [])
        if not isinstance(entries, list) or not 1 <= len(entries) <= 100:
            raise ValueError("watchlist needs 1–100 instrument identities")
        for entry in entries:
            if not isinstance(entry, dict) or any(
                not isinstance(entry.get(k), str) or not entry[k].strip()
                for k in ("isin", "market", "currency", "distribution")
            ):
                raise ValueError(
                    "ISIN, market, currency and distribution are required; ticker alone is insufficient"
                )
        return body
    if kind == "check":
        if not isinstance(body.get("type"), str) or body.get("type") not in {"search", "coverage"}:
            raise ValueError("check type must be search or coverage")
        if body["type"] == "search" and (
            not isinstance(body.get("q"), str) or not 1 <= len(body["q"]) <= 1000
        ):
            raise ValueError("search check needs a bounded query")
        return body
    if kind == "dataset":
        from .analytics import DatasetSpec

        return DatasetSpec.model_validate(body).model_dump(mode="json")
    raise ValueError("unsupported workspace kind")


def save(db, actor, kind, title, body, *, workspace_id=None, expected_revision=0, generated=False):
    if kind not in KINDS or not 1 <= len(title.strip()) <= 200:
        raise ValueError("invalid workspace kind or title")
    with db.connect() as con:
        if workspace_id:
            item = owned(con, actor, workspace_id)
            if item["kind"] != kind:
                raise ValueError("workspace kind is immutable")
        else:
            if expected_revision != 0:
                raise FileExistsError("new artifact starts at revision zero")
            workspace_id = uid()
            item = {"current_revision": 0}
            con.execute(
                "INSERT INTO workspaces(id,owner,kind,title) VALUES (%s,%s,%s,%s)",
                (workspace_id, actor, kind, title),
            )
        if item["current_revision"] != expected_revision:
            raise FileExistsError("workspace revision changed; reload before saving")
        body = validate(con, kind, body, generated=generated)
        revision = expected_revision + 1
        checksum = digest({"kind": kind, "title": title, "body": body})
        con.execute(
            "INSERT INTO workspace_revisions(workspace_id,revision,body,sha256) VALUES (%s,%s,%s,%s)",
            (workspace_id, revision, Jsonb({"title": title, "content": body}), checksum),
        )
        con.execute(
            "UPDATE workspaces SET title=%s,current_revision=%s WHERE id=%s",
            (title, revision, workspace_id),
        )
        con.execute(
            "INSERT INTO audit(actor,action,target) VALUES (%s,'workspace_revision',%s)",
            (actor, f"{workspace_id}:{revision}"),
        )
    return {"id": workspace_id, "revision": revision, "sha256": checksum}


def load(db, actor, workspace_id, revision=None):
    with db.connect() as con:
        item = owned(con, actor, workspace_id)
        record = con.execute(
            "SELECT * FROM workspace_revisions WHERE workspace_id=%s AND revision=%s",
            (workspace_id, revision or item["current_revision"]),
        ).fetchone()
        if not record:
            raise LookupError("revision not found")
        blocked = set()
        content = record["body"]["content"]
        for ref in content.get("evidence", []):
            current = con.execute(
                "SELECT p.active AS passage_active,a.active AS asset_active,i.source,i.metadata FROM passages p JOIN items i ON i.id=p.item_id LEFT JOIN assets a ON a.id=p.asset_id WHERE p.id=%s",
                (ref["id"],),
            ).fetchone()
            if (
                current
                and current["source"] == "news"
                and current["metadata"].get("content_tier") == "headline_only"
                and (not current["passage_active"] or current["asset_active"] is False)
            ):
                blocked.add(ref["id"])
        if blocked:
            content["evidence"] = [
                {**ref, "text": "[access restricted by current source policy]"}
                if ref["id"] in blocked
                else ref
                for ref in content["evidence"]
            ]
            content["findings"] = [
                {**finding, "text": "[source-linked finding restricted by current source policy]"}
                if blocked.intersection(finding.get("references", []))
                else finding
                for finding in content.get("findings", [])
            ]
    return {
        **item,
        **record,
        "redacted_references": sorted(blocked),
        "checksum_scope": "original stored revision; current-policy redactions are a presentation, not a new immutable revision",
    }


def export_markdown(record):
    body = record["body"]["content"]

    # Source/user Markdown is rendered as literal indented text, not active HTML.
    def block(value):
        return "\n".join("    " + line for line in str(value).splitlines())

    lines = [
        "# OFL research artifact",
        block(record["body"]["title"]),
        f"Revision: {record['revision']} · SHA-256: {record['sha256']}",
        "## Question",
        block(body.get("question", "")),
    ]
    for finding in body.get("findings", []):
        lines += [
            f"## {finding['kind']} · {finding['status']}",
            block(finding["text"]),
            "References: " + ", ".join(finding.get("references", [])),
        ]
    lines += [
        "## Limitations",
        block(body.get("limitations", "No user outcome or suitability validation implied.")),
    ]
    for ref in body.get("evidence", []):
        lines += [
            f"## Evidence {ref['id']}",
            block(ref["title"]),
            block(ref["text"]),
            block(json.dumps({k: v for k, v in ref.items() if k != "text"}, ensure_ascii=False)),
        ]
    if record["kind"] != "investigation":
        lines += ["## Content", block(json.dumps(body, ensure_ascii=False, indent=2))]
    return "\n\n".join(lines) + "\n"


def rerun(db, store, actor, workspace_id):
    record = load(db, actor, workspace_id)
    if record["kind"] != "check":
        raise ValueError("only saved checks can be rerun")
    body = record["body"]["content"]
    if body["type"] == "search":
        from .search import search

        result = search(db, body["q"], source=body.get("source"), mode="keyword", limit=20)
    else:
        from .imports import coverage

        result = coverage(db)
    previous = body.get("result")
    body = {**body, "result": json.loads(json.dumps(result, default=str))}
    # Compare source identifiers/content, not wall-clock invocation metadata.
    body["comparison"] = {
        "previous_sha256": digest(previous) if previous is not None else None,
        "current_sha256": digest(body["result"]),
        "changed": previous != body["result"] if previous is not None else None,
    }
    return save(
        db,
        actor,
        "check",
        record["title"],
        body,
        workspace_id=workspace_id,
        expected_revision=record["revision"],
        generated=True,
    )


def schedule_due(db):
    import json

    roles = json.loads(db.settings.roles or "{}")
    with db.connect() as con:
        rows = con.execute(
            "SELECT s.* FROM check_schedules s WHERE enabled AND next_run_at<=now() ORDER BY next_run_at FOR UPDATE SKIP LOCKED LIMIT 10"
        ).fetchall()
        for row in rows:
            if roles.get(row["owner"]) not in {"reader", "reviewer", "admin"}:
                con.execute(
                    "UPDATE check_schedules SET enabled=false WHERE workspace_id=%s",
                    (row["workspace_id"],),
                )
                continue
            pending = con.execute(
                "SELECT 1 FROM jobs WHERE kind='research_check' AND payload->>'workspace_id'=%s AND state IN ('queued','running') LIMIT 1",
                (row["workspace_id"],),
            ).fetchone()
            if not pending:
                db.enqueue_on(
                    con,
                    "research_check",
                    {"workspace_id": row["workspace_id"], "owner": row["owner"]},
                    row["owner"],
                )
            con.execute(
                "UPDATE check_schedules SET next_run_at=now()+(interval_minutes * interval '1 minute') WHERE workspace_id=%s",
                (row["workspace_id"],),
            )
        return len(rows)
