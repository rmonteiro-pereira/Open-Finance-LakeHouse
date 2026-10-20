"""Offline access-control and source-policy redaction contracts for the research API."""

import json
from contextlib import contextmanager

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("psycopg")

from fastapi import HTTPException
from starlette.datastructures import Headers

from ofl.research.api import authenticate, can_read_feedback, require_admin, require_reviewer
from ofl.research.config import ResearchSettings
from ofl.research.workspaces import has_research_access, load, owned, schedule_due

GATEWAY = "gateway-secret"
SERVICE = "s" * 32
ORIGIN = "https://research.example"


def settings(**overrides):
    values = {
        "gateway_token": GATEWAY,
        "roles": json.dumps(
            {"ana": "admin", "rui": "reviewer", "bia": "reader", "eva": "owner", "svc": "admin"}
        ),
        "service_tokens": json.dumps({"svc": SERVICE}),
        "origin": ORIGIN,
    }
    return ResearchSettings(**{**values, **overrides})


def gateway(identity, **extra):
    return Headers({"x-ofl-gateway-token": GATEWAY, "x-ofl-identity": identity, **extra})


def bearer(token, **extra):
    return Headers({"authorization": f"Bearer {token}", **extra})


def rejected(config, headers, method="GET"):
    with pytest.raises(HTTPException) as caught:
        authenticate(config, headers, method)
    return caught.value.status_code, caught.value.detail


@pytest.mark.parametrize("role", ["reader", "reviewer", "admin"])
def test_gateway_identity_receives_its_configured_role(role):
    config = settings(roles=json.dumps({"ana": role}))
    for method in ("GET", "HEAD"):
        assert authenticate(config, gateway("ana"), method) == ("ana", role)


@pytest.mark.parametrize("secret", [GATEWAY, "XXXX"])
def test_gateway_token_is_required_and_must_match(secret):
    config = settings(gateway_token=secret)
    denied = (401, "verified private gateway required")
    assert rejected(config, Headers({"x-ofl-identity": "ana"})) == denied
    assert rejected(config, Headers({"x-ofl-gateway-token": secret + "x"})) == denied
    assert rejected(config, Headers({"x-ofl-gateway-token": "", "x-ofl-identity": "ana"})) == denied


def test_unconfigured_gateway_rejects_even_an_empty_token():
    config = settings(gateway_token="")
    headers = Headers({"x-ofl-gateway-token": "", "x-ofl-identity": "ana"})
    assert rejected(config, headers) == (401, "verified private gateway required")


@pytest.mark.parametrize("roles", ["", "not json", None])
def test_unreadable_access_configuration_fails_closed(roles):
    config = settings()
    config.roles = roles
    assert rejected(config, gateway("ana")) == (503, "research access is not configured")
    config = settings(service_tokens="{")
    assert rejected(config, bearer(SERVICE)) == (503, "research access is not configured")


@pytest.mark.parametrize("identity", ["", "nobody", "eva", "Ana"])
def test_identity_without_a_research_role_is_refused(identity):
    config = settings(roles=json.dumps({"ana": "admin", "eva": "owner"}))
    assert rejected(config, gateway(identity)) == (403, "identity has no research access")


def test_missing_identity_header_is_not_any_configured_principal():
    names = ("", "XXXX", "None", "null")
    config = settings(roles=json.dumps(dict.fromkeys(names, "admin")))
    headers = Headers({"x-ofl-gateway-token": GATEWAY})
    assert rejected(config, headers) == (403, "identity has no research access")
    assert rejected(config, gateway("")) == (403, "identity has no research access")
    config.service_tokens = json.dumps({"": SERVICE})
    assert rejected(config, bearer(SERVICE)) == (403, "identity has no research access")


def test_roles_must_be_a_mapping():
    config = settings(roles=json.dumps(["ana"]))
    assert rejected(config, gateway("ana")) == (403, "identity has no research access")


@pytest.mark.parametrize("method", ["POST", "PUT", "PATCH", "DELETE"])
def test_gateway_writes_require_the_configured_origin(method):
    config = settings()
    assert authenticate(config, gateway("ana", origin=ORIGIN), method) == ("ana", "admin")
    same_origin = (403, "same-origin request required")
    assert rejected(config, gateway("ana"), method) == same_origin
    assert rejected(config, gateway("ana", origin=ORIGIN + "/"), method) == same_origin
    assert rejected(config, gateway("ana", origin="https://other.example"), method) == same_origin


def test_gateway_reads_do_not_need_an_origin():
    for method in ("GET", "HEAD"):
        assert authenticate(settings(), gateway("bia"), method) == ("bia", "reader")


def test_service_token_resolves_its_principal_and_is_always_a_reader():
    headers = bearer(SERVICE, **{"x-ofl-identity": "bia"})
    for method in ("GET", "HEAD"):
        assert authenticate(settings(), headers, method) == ("svc", "reader")


def test_service_token_does_not_need_the_gateway_secret():
    assert authenticate(settings(gateway_token=""), bearer(SERVICE), "GET") == ("svc", "reader")


@pytest.mark.parametrize("method", ["POST", "PUT", "DELETE"])
def test_service_tokens_are_read_only(method):
    headers = bearer(SERVICE, origin=ORIGIN)
    assert rejected(settings(), headers, method) == (403, "service credentials are read-only")


def test_service_token_must_match_exactly():
    invalid = (401, "invalid service credential")
    assert rejected(settings(), bearer(SERVICE[:-1])) == invalid
    assert rejected(settings(), bearer(SERVICE + "s")) == invalid
    assert rejected(settings(), bearer("")) == invalid
    assert rejected(settings(service_tokens=json.dumps([SERVICE])), bearer(SERVICE)) == invalid


def test_service_secret_minimum_is_32_characters():
    accepted = "a" * 32
    short = "b" * 31
    config = settings(
        service_tokens=json.dumps({"svc": accepted, "bia": short, "rui": 12345678901234567890})
    )
    assert authenticate(config, bearer(accepted), "GET") == ("svc", "reader")
    assert rejected(config, bearer(short)) == (401, "invalid service credential")
    assert rejected(config, bearer("12345678901234567890")) == (401, "invalid service credential")


def test_service_principal_still_needs_a_research_role():
    config = settings(service_tokens=json.dumps({"eva": SERVICE}))
    assert rejected(config, bearer(SERVICE)) == (403, "identity has no research access")


def test_non_bearer_authorization_falls_back_to_the_gateway():
    headers = Headers({"authorization": f"Basic {SERVICE}", "x-ofl-identity": "bia"})
    assert rejected(settings(), headers) == (401, "verified private gateway required")
    both = gateway("bia", authorization=f"Basic {SERVICE}")
    assert authenticate(settings(), both, "GET") == ("bia", "reader")


def test_admin_routes_accept_only_admins():
    assert require_admin(("ana", "admin")) == ("ana", "admin")
    for role in ("reader", "reviewer"):
        with pytest.raises(HTTPException) as caught:
            require_admin((role, role))
        assert (caught.value.status_code, caught.value.detail) == (
            403,
            "administrator role required",
        )


def test_review_routes_accept_reviewers_and_admins():
    for role in ("reviewer", "admin"):
        assert require_reviewer(("ana", role)) == ("ana", role)
    with pytest.raises(HTTPException) as caught:
        require_reviewer(("bia", "reader"))
    assert (caught.value.status_code, caught.value.detail) == (403, "reviewer role required")


# Source-policy redaction on read. The database is replaced by a recorder that serves
# the stored revision and the current state of each captured passage.


class Rows:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row


class PolicyConnection:
    def __init__(self, item, record, current):
        self.item, self.record, self.current = item, record, current
        self.calls = []

    def execute(self, sql, params=()):
        self.calls.append((sql, params))
        if sql.startswith("SELECT * FROM workspaces WHERE id=%s AND owner=%s"):
            return Rows(self.item if params == (self.item["id"], self.item["owner"]) else None)
        if sql.startswith("SELECT * FROM workspace_revisions WHERE workspace_id=%s"):
            wanted = (self.item["id"], self.record["revision"])
            return Rows(self.record if params == wanted else None)
        if sql.startswith("SELECT p.active AS passage_active,a.active AS asset_active,i.source"):
            return Rows(self.current.get(params[0]))
        raise AssertionError(sql)


class PolicyDatabase:
    def __init__(self, connection):
        self.connection = connection

    @contextmanager
    def connect(self):
        yield self.connection


def headline(passage_active=True, asset_active=True, source="news", tier="headline_only"):
    return {
        "passage_active": passage_active,
        "asset_active": asset_active,
        "source": source,
        "metadata": {"content_tier": tier},
    }


def stored(*passages):
    evidence = [{"id": p, "text": f"text {p}", "title": f"title {p}"} for p in passages]
    findings = [
        {"kind": "observation", "text": f"finding {p}", "references": [p]} for p in passages
    ]
    findings.append({"kind": "interpretation", "text": "uncited"})
    reviews = [{"passage_id": p, "verdict": "relevant", "rationale": f"why {p}"} for p in passages]
    content = {"evidence": evidence, "findings": findings, "reference_reviews": reviews}
    return {
        "workspace_id": "w1",
        "revision": 2,
        "sha256": "abc",
        "body": {"title": "t", "content": content},
    }


def open_workspace(current, revision=None, *passages):
    item = {"id": "w1", "owner": "ana", "kind": "investigation", "current_revision": 2}
    con = PolicyConnection(item, stored(*(passages or ("p1", "p2"))), current)
    return load(PolicyDatabase(con), "ana", "w1", revision), con


@pytest.mark.parametrize(
    "state",
    [
        headline(passage_active=False),
        headline(asset_active=False),
        headline(passage_active=False, asset_active=False),
    ],
)
def test_withdrawn_headline_source_redacts_excerpt_finding_and_review(state):
    result, _ = open_workspace({"p1": state, "p2": headline()})
    content = result["body"]["content"]
    assert result["redacted_references"] == ["p1"]
    assert content["evidence"][0] == {
        "id": "p1",
        "text": "[access restricted by current source policy]",
        "title": "title p1",
    }
    assert content["evidence"][1]["text"] == "text p2"
    assert content["findings"][0] == {
        "kind": "observation",
        "text": "[source-linked finding restricted by current source policy]",
        "references": ["p1"],
    }
    assert content["findings"][1]["text"] == "finding p2"
    assert content["findings"][2] == {"kind": "interpretation", "text": "uncited"}
    assert content["reference_reviews"][0] == {
        "passage_id": "p1",
        "verdict": "relevant",
        "rationale": "[source-linked review restricted by current source policy]",
    }
    assert content["reference_reviews"][1]["rationale"] == "why p2"
    assert result["checksum_scope"] == (
        "original stored revision; current-policy redactions are a presentation, "
        "not a new immutable revision"
    )
    assert result["sha256"] == "abc" and result["kind"] == "investigation"


@pytest.mark.parametrize(
    "state",
    [
        None,
        headline(),
        headline(asset_active=None),
        headline(passage_active=False, source="letters"),
        headline(passage_active=False, tier="full_text"),
    ],
)
def test_sources_still_allowed_by_policy_are_not_redacted(state):
    result, _ = open_workspace({"p1": state})
    content = result["body"]["content"]
    assert result["redacted_references"] == []
    assert content == stored("p1", "p2")["body"]["content"]


def test_every_withdrawn_reference_is_listed_in_order():
    result, _ = open_workspace(
        {"p2": headline(passage_active=False), "p1": headline(asset_active=False)}, None, "p2", "p1"
    )
    assert result["redacted_references"] == ["p1", "p2"]
    assert all(
        f["text"].startswith("[source-linked") for f in result["body"]["content"]["findings"][:2]
    )


def test_redaction_reads_the_requested_revision_of_the_owner_only():
    result, con = open_workspace({}, 2)
    assert con.calls[0][1] == ("w1", "ana")
    assert con.calls[1][1] == ("w1", 2)
    assert [params for _, params in con.calls[2:]] == [("p1",), ("p2",)]
    assert result["revision"] == 2
    with pytest.raises(LookupError, match="^revision not found$"):
        open_workspace({}, 1)
    item = {"id": "w1", "owner": "ana", "kind": "investigation", "current_revision": 2}
    con = PolicyConnection(item, stored("p1"), {})
    with pytest.raises(LookupError, match="^workspace not found$"):
        load(PolicyDatabase(con), "rui", "w1")


def test_redaction_handles_revisions_saved_without_findings_or_reviews():
    item = {"id": "w1", "owner": "ana", "kind": "investigation", "current_revision": 1}
    content = {"evidence": [{"id": "p1", "text": "body"}]}
    record = {"workspace_id": "w1", "revision": 1, "body": {"title": "t", "content": content}}
    con = PolicyConnection(item, record, {"p1": headline(passage_active=False)})
    result = load(PolicyDatabase(con), "ana", "w1")
    assert result["body"]["content"] == {
        "evidence": [{"id": "p1", "text": "[access restricted by current source policy]"}],
        "findings": [],
        "reference_reviews": [],
    }


def test_artifact_without_evidence_loads_unchanged():
    item = {"id": "w1", "owner": "ana", "kind": "portfolio", "current_revision": 1}
    record = {"workspace_id": "w1", "revision": 1, "body": {"title": "p", "content": {"a": 1}}}
    result = load(PolicyDatabase(PolicyConnection(item, record, {})), "ana", "w1")
    assert result["body"]["content"] == {"a": 1}
    assert result["redacted_references"] == []


# Ownership


OWNED_SQL = "SELECT * FROM workspaces WHERE id=%s AND owner=%s FOR UPDATE"


class Workspaces:
    """Rows of the workspaces table; answers only the owner-filtered, locking lookup."""

    def __init__(self, *rows):
        self.rows = rows
        self.calls = []

    def execute(self, sql, params):
        self.calls.append((sql, params))
        assert sql == OWNED_SQL
        workspace_id, owner = params
        match = [r for r in self.rows if r["id"] == workspace_id and r["owner"] == owner]
        return Rows(match[0] if match else None)


def test_owner_reads_and_locks_their_own_workspace():
    mine = {"id": "w1", "owner": "ana", "kind": "check"}
    con = Workspaces(mine, {"id": "w2", "owner": "rui", "kind": "check"})
    assert owned(con, "ana", "w1") is mine
    assert con.calls == [(OWNED_SQL, ("w1", "ana"))]


@pytest.mark.parametrize("actor,workspace", [("rui", "w1"), ("ana", "w2"), ("", "w1")])
def test_foreign_or_unknown_workspaces_are_not_found(actor, workspace):
    con = Workspaces({"id": "w1", "owner": "ana", "kind": "check"})
    with pytest.raises(LookupError, match="^workspace not found$"):
        owned(con, actor, workspace)


@pytest.mark.parametrize(
    "roles,actor,allowed",
    [
        ('{"ana": "reader"}', "ana", True),
        ('{"ana": "reviewer"}', "ana", True),
        ('{"ana": "admin"}', "ana", True),
        ('{"ana": "owner"}', "ana", False),
        ('{"ana": "admin"}', "rui", False),
        ("", "ana", False),
        (None, "ana", False),
    ],
)
def test_research_access_follows_the_current_roles(roles, actor, allowed):
    assert has_research_access(roles, actor) is allowed


@pytest.mark.parametrize(
    "owner,user,visible",
    [
        ("ana", ("ana", "reader"), True),
        ("ana", ("rui", "reviewer"), True),
        ("ana", ("eva", "admin"), True),
        ("ana", ("bia", "reader"), False),
        ("ana", ("ANA", "reader"), False),
    ],
)
def test_feedback_history_is_visible_to_its_author_and_reviewers(owner, user, visible):
    assert can_read_feedback(owner, user) is visible


class Schedules:
    """Due check schedules and pending jobs, recording every statement."""

    DUE = (
        "SELECT s.* FROM check_schedules s WHERE enabled AND next_run_at<=now() "
        "ORDER BY next_run_at FOR UPDATE SKIP LOCKED LIMIT 10"
    )
    DISABLE = "UPDATE check_schedules SET enabled=false WHERE workspace_id=%s"
    PENDING = (
        "SELECT 1 FROM jobs WHERE kind='research_check' AND payload->>'workspace_id'=%s "
        "AND state IN ('queued','running') LIMIT 1"
    )
    NEXT = (
        "UPDATE check_schedules SET next_run_at=now()+(interval_minutes * interval '1 minute') "
        "WHERE workspace_id=%s"
    )

    def __init__(self, due, pending=()):
        self.due, self.pending = due, set(pending)
        self.calls = []

    def execute(self, sql, params=()):
        self.calls.append((sql, params))
        if sql == self.DUE:
            return Answer(all_rows=self.due)
        if sql == self.PENDING:
            return Answer(one=(1,) if params[0] in self.pending else None)
        assert sql in {self.DISABLE, self.NEXT}, sql
        return Answer()


class Answer:
    def __init__(self, one=None, all_rows=()):
        self.one, self.all_rows = one, list(all_rows)

    def fetchone(self):
        return self.one

    def fetchall(self):
        return self.all_rows


class ScheduleDatabase:
    def __init__(self, con, roles):
        self.con = con
        self.settings = ResearchSettings(roles=roles)
        self.enqueued = []

    @contextmanager
    def connect(self):
        yield self.con

    def enqueue_on(self, con, kind, payload, actor):
        assert con is self.con
        self.enqueued.append((kind, payload, actor))


def test_due_checks_run_only_for_owners_who_still_have_access():
    due = [
        {"workspace_id": "w1", "owner": "ana"},
        {"workspace_id": "w2", "owner": "gone"},
        {"workspace_id": "w3", "owner": "bia"},
    ]
    con = Schedules(due, pending={"w3"})
    db = ScheduleDatabase(con, json.dumps({"ana": "reader", "bia": "admin", "gone": "owner"}))
    assert schedule_due(db) == 3
    assert db.enqueued == [("research_check", {"workspace_id": "w1", "owner": "ana"}, "ana")]
    assert con.calls == [
        (Schedules.DUE, ()),
        (Schedules.PENDING, ("w1",)),
        (Schedules.NEXT, ("w1",)),
        (Schedules.DISABLE, ("w2",)),
        (Schedules.PENDING, ("w3",)),
        (Schedules.NEXT, ("w3",)),
    ]


def test_no_due_checks_means_no_work():
    con = Schedules([])
    db = ScheduleDatabase(con, "")
    assert schedule_due(db) == 0
    assert db.enqueued == [] and con.calls == [(Schedules.DUE, ())]
