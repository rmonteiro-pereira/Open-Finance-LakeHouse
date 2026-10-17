"""Generate explicitly synthetic public snapshot data for build/route checks only."""
import sys
from pathlib import Path

import duckdb

from ofl.publish import LocalTarget, publish
from ofl.registry import load_registry

con = duckdb.connect()
con.execute("""CREATE TABLE mart_real_interest AS SELECT * FROM (VALUES
    (DATE '2024-01-01',11.75,4.5,6.93),(DATE '2024-02-01',11.25,4.4,6.56))
    t(month,selic_target,ipca_accum_12m,real_interest_rate)""")
con.execute("""CREATE TABLE mart_macro_dashboard AS SELECT * FROM (VALUES
    (DATE '2024-01-01',75.0),(DATE '2024-02-01',76.0)) t(month,debt_to_gdp_pct)""")
con.execute("""CREATE TABLE mart_fx AS SELECT * FROM (VALUES
    ('usd_brl',DATE '2024-01-02',4.90),('usd_brl',DATE '2024-02-01',4.95)) t(series_id,date,rate)""")
con.execute("""CREATE TABLE fact_observation AS SELECT 'selic' AS series_id,
    DATE '2024-01-02' AS date,0.043 AS value""")
publish(con, LocalTarget(Path(sys.argv[1])), registry=load_registry(),
        relation=lambda _layer, table: table, run_id="synthetic-build-fixture")
