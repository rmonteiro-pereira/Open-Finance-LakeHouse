from ofl.registry import load_registry

DOMAINS = {"rates", "inflation", "fx", "fiscal", "market", "equities"}


def test_registry_loads_all_series():
    reg = load_registry("sources/registry.yml")
    assert len(reg.series) == 26
    assert set(reg.domains()) == DOMAINS


def test_known_sgs_ids():
    reg = load_registry("sources/registry.yml")
    assert reg.series["selic"].sgs_id == 11
    assert reg.series["ipca"].sgs_id == 433
    assert reg.series["usd_brl"].sgs_id == 1


def test_plano_real_floor_resolved_onto_sgs_series():
    reg = load_registry("sources/registry.yml")
    # Handler default floor lands on every bacen_sgs series...
    assert reg.series["ipca"].start_date == "1994-07-01"
    assert reg.series["usd_brl"].start_date == "1994-07-01"
    # ...and not on series from other handlers.
    assert reg.series["tesouro_direto"].start_date is None


def test_active_excludes_planned():
    reg = load_registry("sources/registry.yml")
    active = {s.key for s in reg.active()}
    assert "selic" in active
    # real handlers (ipea replaces old synthetic ipea_receita; b3 via Yahoo)
    assert {"tesouro_direto", "ibge", "ipea_nfsp_primario", "b3"} <= active
    # anbima handler is implemented but needs registered credentials -> planned
    assert "anbima" not in active
    assert len(active) == 25
