"""Real pandas/Polars/Spark Parquet integration tests; mandatory on Colab.

These are intentionally skipped if a developer environment lacks PyArrow,
Polars or PySpark. The notebook asserts zero skipped tests, so a successful
Colab run must execute all three engines, including a real local Spark JVM.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path
import shutil

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
pytest.importorskip("pandas")
pytest.importorskip("polars")
pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("Java is required for real local Spark tests", allow_module_level=True)

from fdic_pipeline.processing_engines import compare_rows, evaluate, engine_plan, spark_session


@pytest.fixture(scope="module")
def spark():
    session = spark_session()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def small_panel(tmp_path):
    schema = pa.schema([
        pa.field("CERT", pa.int64()), pa.field("REPDTE", pa.date32()),
        pa.field("ASSET", pa.float64()), pa.field("DEP", pa.float64()),
        pa.field("ROA", pa.float64()), pa.field("DQ_EXTREME_ROE", pa.bool_()),
    ])
    rows = [
        (1, date(2019, 12, 31), 100.0, 80.0, 1.0, False),
        (1, date(2020, 3, 31), 110.0, 90.0, None, True),
        (1, date(2020, 6, 30), 121.0, 100.0, 2.0, False),
        (1, date(2020, 12, 31), 150.0, 110.0, -5.0, False),
        (2, date(2020, 3, 31), 200.0, 180.0, 0.5, False),
        (2, date(2020, 6, 30), 210.0, None, None, False),
        (2, date(2020, 9, 30), 220.0, 200.0, 1.0, False),
        (3, date(2020, 12, 31), 300.0, 250.0, 2.0, False),
    ]
    years = {}
    for cert, reporting, asset, dep, roa, flag in rows:
        key = (reporting.year, (reporting.month - 1) // 3 + 1)
        years.setdefault(key, []).append({"CERT": cert, "REPDTE": reporting,
                                           "ASSET": asset, "DEP": dep,
                                           "ROA": roa, "DQ_EXTREME_ROE": flag})
    for (y, q), group in years.items():
        output = tmp_path / "panel" / f"year={y}" / f"quarter={q}" / "part-00000.parquet"
        output.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(group, schema=schema), output, compression="snappy")
    return tmp_path / "panel"


@pytest.mark.parametrize("workload", ["quarter_summary", "adjacent_quarter_growth"])
def test_all_three_engines_agree(small_panel, spark, workload):
    pandas = evaluate("pandas", small_panel, workload)
    polars = evaluate("polars", small_panel, workload)
    spark_out = evaluate("pyspark", small_panel, workload, spark=spark)
    compare_rows(pandas, polars, workload)
    compare_rows(pandas, spark_out, workload)
    assert len(pandas) == 5
    assert "Parquet" in engine_plan("polars", small_panel, workload) or \
        "PARQUET" in engine_plan("polars", small_panel, workload).upper()
    assert engine_plan("pyspark", small_panel, workload, spark=spark).strip()
    if workload == "quarter_summary":
        march = next(r for r in pandas if r["REPDTE"] == "2020-03-31")
        assert march["BANK_QUARTERS"] == 2
        assert march["TOTAL_ASSETS"] == 310.0
        assert march["EXTREME_ROE_COUNT"] == 1
    else:
        march = next(r for r in pandas if r["REPDTE"] == "2020-03-31")
        june = next(r for r in pandas if r["REPDTE"] == "2020-06-30")
        december = next(r for r in pandas if r["REPDTE"] == "2020-12-31")
        assert march["CONSECUTIVE_PAIRS"] == 1
        assert june["CONSECUTIVE_PAIRS"] == 2
        assert december["CONSECUTIVE_PAIRS"] == 0  # missing 2020Q3 for CERT=1
