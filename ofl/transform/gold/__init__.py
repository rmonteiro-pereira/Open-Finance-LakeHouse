"""Gold lane (DuckDB): query-on-the-lake marts over the silver star schema.

Lean by design — SQL files in ``models/`` + a thin runner. No dbt/SQLMesh yet
(deferred; OpenLineage covers lineage). DuckDB reads silver Delta from MinIO and
the results are written back to gold Delta via delta-rs.
"""
