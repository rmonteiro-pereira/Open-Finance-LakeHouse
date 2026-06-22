from ofl.registry import load_registry

DOMAINS = {"rates", "inflation", "fx", "fiscal", "credit", "market", "equities"}


def test_registry_loads_all_series():
    reg = load_registry("sources/registry.yml")
    assert len(reg.series) == 43
    assert set(reg.domains()) == DOMAINS


def test_known_sgs_ids():
    reg = load_registry("sources/registry.yml")
    assert reg.series["selic"].sgs_id == 11
    assert reg.series["ipca"].sgs_id == 433
    assert reg.series["usd_brl"].sgs_id == 1


def test_corrected_sgs_codes():
    reg = load_registry("sources/registry.yml")
    # IGP-M / IGP-DI codes were swapped: SGS 189 = IGP-M, 190 = IGP-DI.
    assert reg.series["igp_m"].sgs_id == 189
    assert reg.series["igp_di"].sgs_id == 190
    # divida_pib is the *gross* debt (DBGG=13762); 4513 (net) moved to dlsp_pib.
    assert reg.series["divida_pib"].sgs_id == 13762
    assert reg.series["dlsp_pib"].sgs_id == 4513
    # bogus focus_pib (was CDI-252 mislabeled as a GDP series) is gone; 4389 now
    # lives as the correctly-named cdi_anual.
    assert "focus_pib" not in reg.series
    assert reg.series["cdi_anual"].sgs_id == 4389


def test_credit_domain():
    reg = load_registry("sources/registry.yml")
    assert "credit" in reg.domains()
    assert reg.series["credito_total"].domain == "credit"
    assert reg.series["inadimplencia_pf"].sgs_id == 21084


def test_focus_handler_series():
    reg = load_registry("sources/registry.yml")
    s = reg.series["focus_ipca_12m"]
    assert s.handler == "bacen_focus"
    assert s.extra["resource"] == "ExpectativasMercadoInflacao12Meses"
    assert reg.series["focus_selic_fim_ano"].extra["horizon"] == "current_year"


def test_plano_real_floor_resolved_onto_sgs_series():
    reg = load_registry("sources/registry.yml")
    # Handler default floor lands on every bacen_sgs series...
    assert reg.series["ipca"].start_date == "1994-07-01"
    assert reg.series["usd_brl"].start_date == "1994-07-01"
    # ...and not on series from other handlers (tesouro, focus).
    assert reg.series["tesouro_direto"].start_date is None
    assert reg.series["focus_ipca_12m"].start_date is None


def test_active_excludes_planned():
    reg = load_registry("sources/registry.yml")
    active = {s.key for s in reg.active()}
    assert "selic" in active
    # real handlers (ipea replaces old synthetic ipea_receita; b3 via Yahoo)
    assert {"tesouro_direto", "ibge", "ipea_nfsp_primario", "b3"} <= active
    # anbima is active against the sandbox (creds registered; ANBIMA_BASE_URL=sandbox)
    assert "anbima" in active
    assert len(active) == 43
