"""The as-of percentile — the primitive that turns a level into an answer.

Two things are pinned here that a "does it run" test would miss: the rank CONVENTION,
which three DuckDB functions answer differently, and the as-of property, which is what
lets a published row survive the next release.
"""

from datetime import date
from pathlib import Path

import duckdb
import polars as pl
import pytest

RECIPE = Path(__file__).resolve().parent.parent / "ofl" / "release" / "recipes" / "percentile_asof.sql"
GOLDEN = Path(__file__).parent / "fixtures" / "golden" / "percentile_asof.expected.csv"
FIXTURE = Path(__file__).parent / "fixtures" / "release" / "fact_observation.csv"


def _run(obs: pl.DataFrame, dim: pl.DataFrame) -> pl.DataFrame:
    c = duckdb.connect()
    c.register("fact_observation", obs.to_arrow())
    c.register("dim_series", dim.to_arrow())
    return c.execute(RECIPE.read_text(encoding="utf-8")).pl()


def _tiny() -> tuple[pl.DataFrame, pl.DataFrame]:
    """Four points with a deliberate tie, so the mid-rank term is exercised."""
    obs = pl.DataFrame(
        {
            "series_id": ["s"] * 4,
            "date": [date(2020, 1, 1), date(2021, 1, 1), date(2022, 1, 1), date(2023, 1, 1)],
            "value": [1.0, 5.0, 5.0, 3.0],
        }
    )
    dim = pl.DataFrame({"series_id": ["s"], "frequency": ["annual"]})
    return obs, dim


def test_mid_rank_is_computed_by_hand():
    out = _run(*_tiny()).filter(pl.col("window_label") == "full").sort("date")
    rows = out.to_dicts()

    # 2020: window {1}. below=0 ties=1 n=1 -> 0.5
    assert rows[0]["pct_rank"] == pytest.approx(0.5)
    # 2021: window {1,5}. below=1 ties=1 n=2 -> 0.75
    assert rows[1]["pct_rank"] == pytest.approx(0.75)
    # 2022: window {1,5,5}. below=1 ties=2 n=3 -> (1+1)/3
    assert rows[2]["pct_rank"] == pytest.approx(2 / 3)
    assert (rows[2]["n_below"], rows[2]["n_ties"], rows[2]["n_obs"]) == (1, 2, 3)
    # 2023 (value 3): window {1,5,5,3}. below=1 ties=1 -> 1.5/4
    assert rows[3]["pct_rank"] == pytest.approx(0.375)


def test_mid_rank_is_not_percent_rank_and_not_cume_dist():
    """The reason the convention is fixed at all: all three are called "percentile", and
    on the tied point they disagree. A recipe swapped for either would keep running and
    publish a different number under the same name."""
    obs, dim = _tiny()
    out = _run(obs, dim).filter((pl.col("window_label") == "full") & (pl.col("date") == date(2022, 1, 1)))
    mid = out["pct_rank"][0]

    c = duckdb.connect()
    c.register("o", obs.filter(pl.col("date") <= date(2022, 1, 1)).to_arrow())
    other = c.execute(
        "SELECT percent_rank() OVER (ORDER BY value) AS pr, cume_dist() OVER (ORDER BY value) AS cd, "
        "value, date FROM o QUALIFY date = DATE '2022-01-01' LIMIT 1"
    ).pl()
    assert mid != pytest.approx(other["pr"][0])
    assert mid != pytest.approx(other["cd"][0])


def test_as_of_ignores_the_future():
    """A row's rank must not move when a later observation arrives — otherwise every
    published row is rewritten on every release and nothing is pinnable."""
    obs, dim = _tiny()
    before = _run(obs, dim).filter(
        (pl.col("date") == date(2021, 1, 1)) & (pl.col("window_label") == "full")
    )
    extended = pl.concat(
        [obs, pl.DataFrame({"series_id": ["s"], "date": [date(2024, 1, 1)], "value": [0.1]})]
    )
    after = _run(extended, dim).filter(
        (pl.col("date") == date(2021, 1, 1)) & (pl.col("window_label") == "full")
    )
    assert before["pct_rank"][0] == after["pct_rank"][0]
    assert before["inputs_sha256"][0] == after["inputs_sha256"][0]


def test_a_revision_of_the_past_changes_the_inputs_hash():
    """As-of removes the dependence on the FUTURE, not on revisions of the past — and the
    sources revise. The hash exists so a consumer can see it rather than be promised it
    never happens."""
    obs, dim = _tiny()
    before = _run(obs, dim).filter(
        (pl.col("date") == date(2022, 1, 1)) & (pl.col("window_label") == "full")
    )
    revised = obs.with_columns(
        pl.when(pl.col("date") == date(2020, 1, 1)).then(1.5).otherwise(pl.col("value")).alias("value")
    )
    after = _run(revised, dim).filter(
        (pl.col("date") == date(2022, 1, 1)) & (pl.col("window_label") == "full")
    )
    assert before["inputs_sha256"][0] != after["inputs_sha256"][0]


def test_window_is_closed_on_the_left():
    """(date - k years, date]. An observation exactly k years back is OUT, so two runs on
    consecutive days cannot disagree about whether a boundary point belongs."""
    obs = pl.DataFrame(
        {
            "series_id": ["s"] * 3,
            "date": [date(2015, 1, 1), date(2020, 1, 1), date(2020, 1, 2)],
            "value": [99.0, 1.0, 2.0],
        }
    )
    dim = pl.DataFrame({"series_id": ["s"], "frequency": ["annual"]})
    out = _run(obs, dim).filter((pl.col("window_label") == "5y") & (pl.col("date") == date(2020, 1, 1)))
    assert out["window_start"][0] == date(2015, 1, 1)
    assert out["n_obs"][0] == 1  # the 2015-01-01 point is excluded by the open bound


def test_a_short_series_is_refused_instead_of_ranked():
    """`ibc_br` in the corpus has four points. A percentile over four observations is a
    number, and publishing it would be worse than publishing nothing."""
    obs = pl.read_csv(FIXTURE, try_parse_dates=True)
    dim = pl.DataFrame(
        {"series_id": ["ipca", "selic", "ibc_br"], "frequency": ["monthly", "daily", "monthly"]}
    )
    out = _run(obs, dim)
    short = out.filter(pl.col("series_id") == "ibc_br")
    assert short.height > 0
    assert not short["percentile_allowed"].any()


def test_coverage_floor_is_relative_to_the_declared_cadence():
    """An absolute floor of 120 observations disqualified every monthly series, whose 5y
    window holds 60 points by construction. The floor is a ratio plus an absolute
    minimum, so a monthly series can qualify and a gap-ridden daily one cannot."""
    months = [date(2015 + i // 12, i % 12 + 1, 1) for i in range(120)]
    obs = pl.DataFrame(
        {"series_id": ["m"] * 120, "date": months, "value": [float(i % 17) for i in range(120)]}
    )
    dim = pl.DataFrame({"series_id": ["m"], "frequency": ["monthly"]})
    out = _run(obs, dim).filter((pl.col("window_label") == "5y") & (pl.col("date") == months[-1]))
    assert out["n_expected"][0] == 60
    assert out["n_obs"][0] >= 54
    assert bool(out["percentile_allowed"][0]) is True


def test_the_golden_matches_and_a_mutated_recipe_would_not():
    """The gate's own falsifiability: run the frozen recipe, then run a mutated one, and
    require that the golden distinguishes them. A golden that both versions satisfy is
    the self-referential gate this project has been bitten by twice."""
    obs = pl.read_csv(FIXTURE, try_parse_dates=True)
    dim = pl.DataFrame(
        {"series_id": ["ipca", "selic", "ibc_br"], "frequency": ["monthly", "daily", "monthly"]}
    )
    actual = _run(obs, dim).select(
        "series_id", "date", "window_label", "n_obs", "n_below", "n_ties", "pct_rank", "percentile_allowed"
    )
    expected = pl.read_csv(GOLDEN, comment_prefix="#", try_parse_dates=True)
    assert actual.sort("series_id", "date", "window_label").equals(
        expected.sort("series_id", "date", "window_label")
    )

    mutated = RECIPE.read_text(encoding="utf-8").replace(
        "(s.n_below + 0.5 * s.n_ties) / s.n_obs", "(s.n_below + s.n_ties)::DOUBLE / s.n_obs"
    )
    c = duckdb.connect()
    c.register("fact_observation", obs.to_arrow())
    c.register("dim_series", dim.to_arrow())
    mutant = c.execute(mutated).pl().select("series_id", "date", "window_label", "pct_rank")
    assert not mutant.sort("series_id", "date", "window_label")["pct_rank"].equals(
        expected.sort("series_id", "date", "window_label")["pct_rank"]
    )
