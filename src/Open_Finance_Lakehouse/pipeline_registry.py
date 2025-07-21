"""Project pipelines."""

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline

# Import specific pipelines that we want to register
from .pipelines.selic.pipeline import create_pipeline as create_selic_pipeline
from .pipelines.ipca.pipeline import create_pipeline as create_ipca_pipeline
from .pipelines.usd_brl.pipeline import create_pipeline as create_usd_brl_pipeline
from .pipelines.cdi.pipeline import create_pipeline as create_cdi_pipeline
from .pipelines.selic_meta.pipeline import create_pipeline as create_selic_meta_pipeline
from .pipelines.over.pipeline import create_pipeline as create_over_pipeline
from .pipelines.tlp.pipeline import create_pipeline as create_tlp_pipeline
from .pipelines.ipca_15.pipeline import create_pipeline as create_ipca_15_pipeline
from .pipelines.inpc.pipeline import create_pipeline as create_inpc_pipeline
from .pipelines.igp_di.pipeline import create_pipeline as create_igp_di_pipeline
from .pipelines.igp_m.pipeline import create_pipeline as create_igp_m_pipeline
from .pipelines.igp_10.pipeline import create_pipeline as create_igp_10_pipeline
from .pipelines.eur_brl.pipeline import create_pipeline as create_eur_brl_pipeline
from .pipelines.reservas_internacionais.pipeline import create_pipeline as create_reservas_internacionais_pipeline
from .pipelines.divida_pib.pipeline import create_pipeline as create_divida_pib_pipeline
from .pipelines.focus_pib.pipeline import create_pipeline as create_focus_pib_pipeline

# Yahoo Finance pipelines
from .pipelines.yahoo_finance.pipeline import (
    create_yahoo_finance_full_pipeline,
    create_yahoo_etf_pipeline,
    create_yahoo_commodity_pipeline,
    create_yahoo_currency_pipeline
)

# IBGE pipelines
from .pipelines.ibge.pipeline_simple import (
    create_pipeline as create_ibge_pipeline
)

# ANBIMA pipelines
from .pipelines.anbima.pipeline import (
    create_anbima_pipeline
)

# Tesouro Direto pipelines
from .pipelines.tesouro_direto.pipeline import (
    create_pipeline as create_tesouro_direto_pipeline
)


def register_pipelines() -> dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    # Try to find pipelines automatically first
    try:
        pipelines = find_pipelines()
    except Exception:
        # If find_pipelines fails, manually register pipelines
        pipelines = {}
    
    # Manually register key pipelines
    pipelines.update({
        "selic": create_selic_pipeline(),
        "ipca": create_ipca_pipeline(),
        "usd_brl": create_usd_brl_pipeline(),
        "cdi": create_cdi_pipeline(),
        "selic_meta": create_selic_meta_pipeline(),
        "over": create_over_pipeline(),
        "tlp": create_tlp_pipeline(),
        "ipca_15": create_ipca_15_pipeline(),
        "inpc": create_inpc_pipeline(),
        "igp_di": create_igp_di_pipeline(),
        "igp_m": create_igp_m_pipeline(),
        "igp_10": create_igp_10_pipeline(),
        "eur_brl": create_eur_brl_pipeline(),
        "reservas_internacionais": create_reservas_internacionais_pipeline(),
        "divida_pib": create_divida_pib_pipeline(),
        "focus_pib": create_focus_pib_pipeline(),
        # Yahoo Finance pipelines
        "yahoo_finance": create_yahoo_finance_full_pipeline(),
        "yahoo_etf": create_yahoo_etf_pipeline(),
        "yahoo_commodity": create_yahoo_commodity_pipeline(),
        "yahoo_currency": create_yahoo_currency_pipeline(),
        # IBGE pipelines
        "ibge": create_ibge_pipeline(),
        # ANBIMA pipelines
        "anbima": create_anbima_pipeline(),
        # Tesouro Direto pipelines
        "tesouro_direto": create_tesouro_direto_pipeline(),
    })
    
    pipelines["__default__"] = sum(pipelines.values())
    return pipelines
