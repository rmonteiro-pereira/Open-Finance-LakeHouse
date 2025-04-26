# Open Finance Lakehouse

## 📌 Visão Geral
Plataforma de Lakehouse para integração de dados financeiros públicos, incluindo CVM, BACEN e B3, utilizando PySpark, Delta Lake, Airflow e Kedro.

## 🎯 Objetivos
- Construir pipelines batch para ingestão e transformação de dados financeiros.
- Organizar o fluxo de dados em camadas (bronze → silver → gold).
- Validar a qualidade dos dados com Great Expectations.
- Orquestrar workflows com Airflow.
- Disponibilizar dados otimizados para análises via Athena/Dremio/Power BI.

## 🛠️ Tecnologias
- Python 3.10+
- PySpark
- Kedro
- Airflow
- Delta Lake
- Great Expectations
- APIs públicas (BACEN SGS, CVM dados abertos)

## 🚀 Como Rodar
```bash
pip install -r requirements.txt
kedro run
```

## 📂 Estrutura de Diretórios
Conforme padrão Kedro e Data Lake (medallion architecture).

## 📊 Fontes de Dados
- CVM Fundos de Investimento
- BACEN Série Histórica (IPCA, SELIC, CDI)
- B3 (via FTP/Scraping)

## 📚 Licença
MIT License
"""

# Pronto para iniciar o desenvolvimento! 🚀
