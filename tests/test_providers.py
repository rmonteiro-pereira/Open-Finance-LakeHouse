"""Licence and authenticity as types, not as footnotes.

Two independent locks, tested independently: `provider` answers *who owns this row*, and
`data_class` answers *is this value real*. ANBIMA is currently blocked by both — the Feed
was denied, and the series being ingested run against a sandbox that returns fictitious
values in a format-real shape. Either lock alone would keep it out; the point of having
two is that lifting one does not silently lift the other.
"""

import inspect

import pytest

from ofl.ingestion.anbima import anbima_data_class
from ofl.ingestion.landing import land_bronze
from ofl.providers import (
    DATA_CLASSES,
    assert_known_providers,
    assert_publishable,
    is_redistributable,
    load_providers,
)
from ofl.registry import load_registry


def test_every_registry_handler_has_a_verdict():
    """The mapping the licence gate reads is `series.handler`. A handler with no entry is
    a source whose verdict nobody wrote — and under default-deny that must be visible as
    a failing test, not as a quietly unpublishable series."""
    registered = set(load_providers().handlers)
    used = set(load_registry().handlers())
    assert used - registered == set(), f"handlers with no provider verdict: {sorted(used - registered)}"


def test_verdicts_carry_a_date_and_an_author():
    """A verdict with no date is one nobody can age out."""
    for key, p in load_providers().handlers.items():
        assert p.verdict_date, key
        assert p.verdict_by, key


def test_default_is_deny():
    assert is_redistributable("a-handler-that-does-not-exist") is False


def test_the_four_open_sources_and_the_blocked_ones():
    assert is_redistributable("bacen_sgs") is True
    assert is_redistributable("bacen_focus") is True
    assert is_redistributable("tesouro_direto") is True
    assert is_redistributable("ipea") is True
    assert is_redistributable("ibge") is True

    for blocked in ("b3", "b3_cotahist", "b3_arquivos", "anbima", "yahoo"):
        assert is_redistributable(blocked) is False, blocked


def test_unverified_is_a_distinct_state_from_restricted():
    """Yahoo has no written verdict; B3 has one that says no. The catalogue must be able
    to tell a reader which of the two it is looking at."""
    handlers = load_providers().handlers
    assert handlers["yahoo"].state == "unverified"
    assert handlers["b3"].state == "restricted"
    assert handlers["bacen_sgs"].state == "open"


def test_a_misspelled_provider_is_rejected_before_the_licence_question():
    """Otherwise "no restricted provider found" is satisfied by a typo just as well as by
    a clean release — the gate would pass for the wrong reason."""
    with pytest.raises(ValueError, match="outside the registered handlers"):
        assert_known_providers(["bacen_sgs", "bacen_sqs"])


def test_assert_publishable_names_the_offender_and_its_licence():
    assert_publishable(["bacen_sgs", "tesouro_direto"])  # passes
    with pytest.raises(ValueError, match="non-redistributable"):
        assert_publishable(["bacen_sgs", "anbima"])


def test_land_bronze_has_no_default_data_class():
    """A default would supply the most permissive value to every handler that forgot to
    think about it — and the class that must never reach a public release is exactly the
    one a default would silently provide."""
    sig = inspect.signature(land_bronze)
    param = sig.parameters["data_class"]
    assert param.default is inspect.Parameter.empty
    assert param.kind is inspect.Parameter.KEYWORD_ONLY


def test_anbima_class_follows_the_host_it_actually_read(monkeypatch):
    import ofl.ingestion.anbima as mod

    monkeypatch.setattr(mod, "_DATA_BASE", "https://api-sandbox.anbima.com.br")
    assert anbima_data_class() == "sandbox"
    monkeypatch.setattr(mod, "_DATA_BASE", "https://api.anbima.com.br")
    assert anbima_data_class() == "live"


def test_data_class_domain_is_closed():
    assert DATA_CLASSES == ("live", "sandbox", "synthetic")
