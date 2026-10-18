"""Public synthetic retrieval exercise in a temporary schema, never the live corpus."""

import json
import tempfile
import time
import uuid
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

from .archive import upsert_item
from .db import Database
from .retrieval_eval import corpus_manifest, evaluate
from .search import embed_pending

DOCUMENTS = [
    (
        "credit",
        "Política monetária",
        "A taxa básica de juros afeta o custo dos empréstimos e as condições de financiamento.",
    ),
    (
        "prices",
        "Inflação ao consumidor",
        "O índice de preços mede a variação do custo de uma cesta de bens e serviços das famílias.",
    ),
    (
        "portfolio",
        "Concentração da carteira",
        "Uma posição com peso elevado concentra a exposição dos investimentos em um único emissor.",
    ),
    (
        "letter",
        "Carta de uma gestora",
        "A gestora interpreta a taxa básica de juros como restritiva. É uma opinião atribuída, sem previsão garantida.",
    ),
    (
        "dataset",
        "Preparação de séries",
        "Para alinhar periodicidades diária e mensal, declare a frequência e mantenha os dados ausentes sem imputação.",
    ),
    (
        "weather",
        "Meteorologia",
        "Chuvas e massas de ar alteram as condições meteorológicas ao longo da semana.",
    ),
    (
        "freight",
        "Transporte",
        "O planejamento de rotas e a manutenção de caminhões afetam a logística de distribuição.",
    ),
    (
        "election",
        "Eleições",
        "Uma pesquisa eleitoral descreve a intenção de voto de uma amostra de eleitores.",
    ),
    (
        "soy",
        "Agricultura",
        "O plantio de soja depende do calendário de cultivo e das condições do solo.",
    ),
    (
        "dividend",
        "Dividendos",
        "A distribuição de lucros a acionistas depende da decisão da companhia e da data de elegibilidade.",
    ),
    (
        "security",
        "Segurança digital",
        "Autenticação e controle de acesso reduzem a exposição de credenciais de sistemas.",
    ),
    (
        "education",
        "Ensino",
        "A escola organiza aulas e avalia o aprendizado dos estudantes por atividades.",
    ),
    (
        "oil",
        "Petróleo",
        "A oferta e a demanda por petróleo influenciam as negociações dessa matéria-prima.",
    ),
    (
        "crypto",
        "Criptoativos",
        "Uma rede distribuída valida transações de ativos digitais por regras de consenso.",
    ),
    (
        "rent",
        "Imóveis",
        "Contratos de locação definem prazo, ocupação e condições de pagamento do aluguel.",
    ),
    (
        "sport",
        "Esportes",
        "A preparação física de atletas inclui descanso e treinamento para competições.",
    ),
    (
        "ocr",
        "Extração documental",
        "O reconhecimento óptico de caracteres pode errar números e precisa de revisão humana.",
    ),
    (
        "backtest",
        "Teste de estratégia",
        "Usar informação publicada depois da decisão provoca vazamento temporal em um teste histórico.",
    ),
    (
        "etf",
        "Fundo de índice",
        "Um fundo negociado em bolsa pode acompanhar uma cesta de ativos segundo regras de um índice.",
    ),
    (
        "currency",
        "Câmbio",
        "Uma cotação expressa o preço de uma moeda em unidades de outra moeda, com data e convenção.",
    ),
]
QUERIES = [
    ("credit", "custo dos empréstimos e condições de financiamento", ["credit"], {}),
    ("prices", "aumento do custo de vida das famílias", ["prices"], {}),
    ("portfolio", "investimentos muito dependentes de uma única empresa", ["portfolio"], {}),
    (
        "dataset",
        "combinar dados com periodicidades diferentes e valores faltantes",
        ["dataset"],
        {},
    ),
    ("letter", "opinião da gestora sobre a taxa básica", ["letter"], {"source": "manager_letter"}),
    ("empty", "quarks supercondutores mecânica quântica", [], {}),
]


def evaluate_fixture(settings, *, progress=lambda _: None):
    schema = "ofl_retrieval_eval_" + uuid.uuid4().hex
    scoped = settings.model_copy(
        update={
            "database_url": make_conninfo(
                settings.database_url, options=f"-csearch_path={schema},public"
            ),
            "objects_bucket": "",
        }
    )
    # Only a new, randomly named schema belongs to this exercise. No live pointers,
    # source files, user artifacts or embedding rows are changed.
    with psycopg.connect(settings.database_url, autocommit=True) as con:
        con.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    try:
        with tempfile.TemporaryDirectory(prefix="ofl-retrieval-eval-") as folder:
            scoped.root = Path(folder)
            db = Database(scoped)
            db.migrate()
            identities = {}
            with db.connect() as con:
                for key, title, text in DOCUMENTS:
                    identities[key] = upsert_item(
                        con,
                        "manager_letter" if key == "letter" else "news",
                        {
                            "source_id": key,
                            "title": "EXERCÍCIO SINTÉTICO — " + title,
                            "description": "Texto fictício para testar recuperação. " + text,
                            "collection": "synthetic-retrieval-v1",
                            "date_start": "2026-01-01",
                            "date_end": "2026-01-01",
                            "date_precision": "day",
                        },
                        key,
                    )
            began = time.perf_counter()
            count = embed_pending(db, len(DOCUMENTS), progress)
            indexing_ms = (time.perf_counter() - began) * 1000
            receipt = corpus_manifest(db)
            judged = {
                "version": 1,
                "split": "development",
                "judge": "OFL-authored synthetic fixture v1; no independent relevance judgments or participants",
                "model": receipt["model"],
                "model_revision": receipt["model_revision"],
                "corpus_sha256": receipt["sha256"],
                "cases": [
                    {
                        "id": key,
                        "query": query,
                        "relevant_item_ids": [identities[v] for v in relevant],
                        "filters": filters,
                        **({"expected_empty": True} if not relevant else {}),
                    }
                    for key, query, relevant, filters in QUERIES
                ],
            }
            manifest = Path(folder) / "judged.json"
            manifest.write_text(json.dumps(judged, ensure_ascii=False))
            report = evaluate(db, manifest)
            report["fixture"] = {
                "version": 1,
                "documents": len(DOCUMENTS),
                "newly_indexed": count,
                "indexing_ms": indexing_ms,
                "temporary_schema_removed": True,
                "qualification": "Authored synthetic development exercise. This is not a held-out, private-corpus, financial-analysis or user benchmark.",
            }
            return report
    finally:
        with psycopg.connect(settings.database_url, autocommit=True) as con:
            con.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
