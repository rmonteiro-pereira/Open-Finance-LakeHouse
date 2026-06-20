from ofl.registry import load_registry

DOMAINS = {"rates", "inflation", "fx", "fiscal", "market", "equities"}


def test_registry_loads_all_series():
    reg = load_registry("sources/registry.yml")
    assert len(reg.series) == 24
    assert set(reg.domains()) == DOMAINS


def test_known_sgs_ids():
    reg = load_registry("sources/registry.yml")
    assert reg.series["selic"].sgs_id == 11
    assert reg.series["ipca"].sgs_id == 433
    assert reg.series["usd_brl"].sgs_id == 1


def test_active_excludes_planned():
    reg = load_registry("sources/registry.yml")
    active = {s.key for s in reg.active()}
    assert "selic" in active
    assert "tesouro_direto" in active and "ibge" in active  # real handlers
    assert "b3" not in active  # legacy impl was synthetic -> planned
    assert "anbima" not in active
    assert len(active) == 21
