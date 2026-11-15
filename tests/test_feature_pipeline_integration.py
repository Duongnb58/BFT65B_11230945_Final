"""Real PyArrow/sklearn end-to-end integration, mandatory in Colab.

The Part 2 fixture contains a documented failure shortly after a source quarter.
It must be censored by the explicit 90-day publication-lag assumption.
"""
from pathlib import Path
import shutil
import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
sklearn = pytest.importorskip("sklearn")

from test_panel_integration import _fixture
from fdic_pipeline.panel import build_panel
from fdic_pipeline.feature_pipeline import (
    build_feature_pipeline, verify_feature_pipeline, FeaturePipelineError,
    feature_table_schema, model_schema,
)
from fdic_pipeline.feature_policy import LABEL_NAME, FEATURE_NAMES


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("part4_integration")
    source = _fixture(tmp_path)
    part2 = tmp_path / "data_quality_panel" / "run_fixture"
    build_panel(source, part2, git_commit="test_revision")
    out = tmp_path / "feature_pipeline" / "run_fixture"
    result = build_feature_pipeline(source, part2, out, "test_revision", min_test_failed_banks=0)
    return source, part2, out, result


def test_full_feature_artifacts_and_label_censoring(run):
    acq, p2, out, verdict = run
    assert verdict["success"]
    assert verdict["verified_feature_partitions"] == 166
    assert verdict["development_only_preprocessing"] is True
    assert len(list((out / "feature_table").rglob("*.parquet"))) == 166
    assert len(list((out / "model_data/development").rglob("*.parquet"))) == 92
    assert len(list((out / "model_data/test_holdout").rglob("*.parquet"))) == 28
    table = pq.ParquetFile(out / "feature_table/year=1984/quarter=1/part-00000.parquet").read()
    assert table.schema == feature_table_schema()
    assert table.column("CERT").to_pylist() == [101]
    assert table.column(LABEL_NAME).to_pylist() == [None]
    assert table.column("LABEL_STATUS").to_pylist() == ["failure_before_assumed_publication"]
    assert not {"FAILDATE", "TERMINAL_CLASS", "ACTIVE", "ENDEFYMD"} & set(table.column_names)
    test = pq.ParquetFile(out / "model_data/test_holdout/year=2008/quarter=1/part-00000.parquet")
    assert test.schema_arrow == model_schema()
    assert test.metadata.num_rows == 1
    assert set(FEATURE_NAMES) <= set(test.schema_arrow.names)
    assert verify_feature_pipeline(acq, p2, out)["success"]


def test_failure_on_modified_evidence(run, tmp_path):
    source, part2, out, _ = run
    tampered = tmp_path / "tampered_output"
    shutil.copytree(out, tampered)
    target = tampered / "quarterly_label_audit.csv"
    target.write_text(target.read_text() + "modified\n")
    with pytest.raises(FeaturePipelineError, match="Modified/missing"):
        verify_feature_pipeline(source, part2, tampered)


def test_failure_on_changed_part2_source(run, tmp_path):
    source, part2, out, _ = run
    tampered_part2 = tmp_path / "tampered_part2"
    shutil.copytree(part2, tampered_part2)
    target = tampered_part2 / "terminal_exits.csv"
    target.write_text(target.read_text() + "modified\n")
    with pytest.raises(FeaturePipelineError, match="audit hash mismatch"):
        verify_feature_pipeline(source, tampered_part2, out)


def test_refuse_output_overwrite(run):
    source, part2, out, _ = run
    with pytest.raises(FeaturePipelineError, match="new Part 4 output"):
        build_feature_pipeline(source, part2, out, min_test_failed_banks=0)


def test_train_only_preprocessor(run):
    import joblib
    import pandas as pd
    source, part2, out, _ = run
    fit = __import__("json").loads((out / "preprocessing_fit.json").read_text())
    assert fit["fitted_on"] == "development_only"
    assert not fit["test_holdout_transformed"]
    assert fit["training_rows"] > 0
    pipeline = joblib.load(out / "training_preprocessor.joblib")
    data = pd.read_parquet(out / "model_data/development/year=1984/quarter=2/part-00000.parquet")
    x = pipeline.transform(data[list(FEATURE_NAMES)])
    assert x.shape[0] == 1
