"""The S3A keys the Spark session sets, checked without a JVM.

``pyspark`` is not installed in the unit-test lane, so a stand-in builder records
every ``.config`` call instead of starting Spark.
"""

from __future__ import annotations

import sys
import types

import pytest

from ofl import config
from ofl.platform import spark


class _Builder:
    def __init__(self) -> None:
        self.conf: dict[str, str] = {}

    def appName(self, _name: str) -> _Builder:  # noqa: N802 - mirrors pyspark
        return self

    def config(self, key: str, value: str) -> _Builder:
        self.conf[key] = value
        return self

    def getOrCreate(self) -> dict[str, str]:  # noqa: N802 - mirrors pyspark
        return self.conf


def _session_conf(monkeypatch: pytest.MonkeyPatch, **env: str) -> dict[str, str]:
    builder = _Builder()
    sql = types.ModuleType("pyspark.sql")
    sql.SparkSession = types.SimpleNamespace(builder=builder)  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    monkeypatch.setenv("S3_ENDPOINT", "http://localhost:9000")
    monkeypatch.setenv("OFL_SPARK_JARS_PACKAGED", "1")
    monkeypatch.delenv("AWS_REGION", raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    config.get_settings.cache_clear()
    try:
        return spark.build_spark_session()
    finally:
        config.get_settings.cache_clear()


def test_directory_markers_are_kept(monkeypatch: pytest.MonkeyPatch) -> None:
    conf = _session_conf(monkeypatch)
    assert conf["spark.hadoop.fs.s3a.directory.marker.retention"] == "keep"


def test_signing_region_defaults_to_us_east_1(monkeypatch: pytest.MonkeyPatch) -> None:
    conf = _session_conf(monkeypatch)
    assert conf["spark.hadoop.fs.s3a.endpoint.region"] == "us-east-1"


def test_signing_region_follows_aws_region(monkeypatch: pytest.MonkeyPatch) -> None:
    conf = _session_conf(monkeypatch, AWS_REGION="sa-east-1")
    assert conf["spark.hadoop.fs.s3a.endpoint.region"] == "sa-east-1"
