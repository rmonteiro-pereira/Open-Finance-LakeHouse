"""
IBGE Pipeline Package

Pipeline para processamento de dados do IBGE (Instituto Brasileiro de Geografia e Estatística)
incluindo indicadores econômicos como IPCA, INPC, PIB, desemprego e outros.
"""

from .pipeline_simple import create_pipeline

__all__ = ["create_pipeline"]
