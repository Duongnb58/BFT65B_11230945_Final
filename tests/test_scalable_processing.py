"""Part 3: deterministic checks independent of FDIC and optional engines."""
from __future__ import annotations

import csv
import json
from datetime import date
from pathlib import Path

import pytest

from fdic_pipeline.processing_engines import (
    ProcessingError, compare_rows, normalize_rows, OUTPUT_FIELDS, REPORT_START,
)
from fdic_pipeline.scalable_processing import (
    SAMPLES, WORKLOADS, ENGINES, ScaleError, source_contract, save_json,
    sample_rows, summarize_trials, verify_result, sha256,
)
from fdic_pipeline.acquisition import quarter_ends, DEFAULT_START, DEFAULT_END


def row(date_string="2020-03-31", n=2, f=1.0):
    return {"REPDTE": date_string, "BANK_QUARTERS": n, "TOTAL_ASSETS": 200.0,
            "TOTAL_DEPOSITS": 160.0, "MEAN_ROA": f, "EXTREME_ROE_COUNT": 0}


def lagrow(date_string="2020-03-31", n=2, p=1, g=10.0):
    return {"REPDTE": date_string, "BANK_QUARTERS": n,
            "CONSECUTIVE_PAIRS": p, "MEAN_ASSET_GROWTH_PCT": g}


def test_normalize_date_and_numeric_float():
    val = row(date(2020, 3, 31), 2, float("nan"))
    assert normalize_rows([val], "quarter_summary")[0]["MEAN_ROA"] is None


def test_compare_sorted_dates():
    a = [row("2020-06-30"), row("2020-03-31")]
    b = list(reversed(a))
    compare_rows(a, b, "quarter_summary")


def test_compare_small_float_tolerance():
    a = [row(f=1.0)]
    b = [row(f=1.0 + 1e-9)]
    compare_rows(a, b, "quarter_summary")

@pytest.mark.parametrize("col,value", [
    ("BANK_QUARTERS", 3), ("EXTREME_ROE_COUNT", 1),
    ("REPDTE", "2020-06-30"), ("MEAN_ROA", 2.1),
    ("TOTAL_ASSETS", 201.0),
])
def test_comparison_fails_on_material_disagreement(col, value):
    a = [row()]
    b = [dict(a[0], **{col: value})]
    with pytest.raises(ProcessingError):
        compare_rows(a, b, "quarter_summary")


def test_missingness_mismatch():
    with pytest.raises(ProcessingError):
        compare_rows([row(f=None)], [row(f=1.0)], "quarter_summary")


def test_invalid_integer_aggregate():
    with pytest.raises(ProcessingError):
        normalize_rows([row(n=1.5)], "quarter_summary")


def test_duplicate_result_date():
    with pytest.raises(ProcessingError):
        normalize_rows([row(), row()], "quarter_summary")


def test_invalid_nan_total_assets():
    with pytest.raises(ProcessingError):
        normalize_rows([row() | {"TOTAL_ASSETS": float("nan")}], "quarter_summary")


def test_growth_no_mean_when_no_consecutive_pairs():
    from fdic_pipeline.scalable_processing import assert_result_semantics
    with pytest.raises(ScaleError):
        assert_result_semantics([lagrow(n=1, p=0, g=10)], "adjacent_quarter_growth", 1)


def test_growth_row_accounting():
    from fdic_pipeline.scalable_processing import assert_result_semantics
    with pytest.raises(ScaleError):
        assert_result_semantics([lagrow(n=2)], "adjacent_quarter_growth", 3)


def fixture_snapshot():
    q = quarter_ends(DEFAULT_START, DEFAULT_END)
    return {"partitions": [{"quarter": value, "rows": 1} for value in q]}


def test_sample_rows_half_and_full():
    s = fixture_snapshot()
    assert sample_rows(s, "half", "adjacent_quarter_growth") == 85
    assert sample_rows(s, "full", "adjacent_quarter_growth") == 170
    assert sample_rows(s, "half", "quarter_summary") == 85
    assert sample_rows(s, "full", "quarter_summary") == 106


def create_worker_outputs(out: Path, corruption: str | None = None):
    snapshot = fixture_snapshot()
    for sample in SAMPLES:
        for workload in WORKLOADS:
            quarters = snapshot["partitions"][-85:] if sample == "half" else snapshot["partitions"]
            if workload == "quarter_summary":
                quarters = [q for q in quarters if date.fromisoformat(f'{q["quarter"][:4]}-'
                           f'{q["quarter"][4:6]}-{q["quarter"][6:]}') >= REPORT_START]
            for engine in ENGINES:
                rows = []
                for index, q in enumerate(quarters):
                    d = date.fromisoformat(f'{q["quarter"][:4]}-{q["quarter"][4:6]}-{q["quarter"][6:]}')
                    if workload == "quarter_summary":
                        rows.append({"REPDTE": d.isoformat(), "BANK_QUARTERS": 1,
                                     "TOTAL_ASSETS": 100.0, "TOTAL_DEPOSITS": 80.0,
                                     "MEAN_ROA": 1.0, "EXTREME_ROE_COUNT": 0})
                    else:
                        rows.append({"REPDTE": d.isoformat(), "BANK_QUARTERS": 1,
                                     "CONSECUTIVE_PAIRS": int(index > 0),
                                     "MEAN_ASSET_GROWTH_PCT": 0.0 if index > 0 else None})
                observations = []
                for t in range(-1, 3):
                    candidate = [dict(r) for r in rows]
                    if corruption == "numeric" and engine == "polars" and sample == "full" and t == 1:
                        candidate[0]["BANK_QUARTERS"] = 2
                    observations.append({"trial_index": t, "warmup": t < 0,
                                         "elapsed_s": 0.2 + 0.01 * (t + 1),
                                         "baseline_tree_rss_mib": 100.0,
                                         "peak_tree_rss_mib": 110.0,
                                         "output_rows": len(candidate), "result": candidate})
                filename = out / "workers" / f"{sample}_{workload}_{engine}.json"
                save_json(filename, {"engine": engine, "workload": workload,
                                     "plan": "PARQUET SCAN FILTER GROUPBY Exchange Window",
                                     "observations": observations})
    return snapshot


def test_summarize_complete_fair_trial_matrix(tmp_path):
    snapshot = create_worker_outputs(tmp_path)
    output = summarize_trials(tmp_path, snapshot)
    assert len(output["trials"]) == 48
    assert sum(not x["warmup"] for x in output["trials"]) == 36
    assert len(output["comparisons"]) == 4
    assert len(output["scaling"]) == 6
    assert (tmp_path / "equivalence.json").is_file()
    with (tmp_path / "trials.csv").open() as f:
        assert len(list(csv.DictReader(f))) == 48


def test_summarize_detects_cross_engine_disagreement(tmp_path):
    snapshot = create_worker_outputs(tmp_path, corruption="numeric")
    with pytest.raises((ScaleError, ProcessingError)):
        summarize_trials(tmp_path, snapshot)


def test_summarize_detects_missing_worker(tmp_path):
    snapshot = create_worker_outputs(tmp_path)
    (tmp_path / "workers" / "half_quarter_summary_pandas.json").unlink()
    with pytest.raises(ScaleError, match="Missing worker"):
        summarize_trials(tmp_path, snapshot)


def test_summarize_detects_trial_flag_change(tmp_path):
    snapshot = create_worker_outputs(tmp_path)
    fn = tmp_path / "workers" / "half_quarter_summary_pandas.json"
    data = json.loads(fn.read_text())
    data["observations"][0]["warmup"] = False
    save_json(fn, data)
    with pytest.raises(ScaleError, match="trial identity"):
        summarize_trials(tmp_path, snapshot)


def test_summarize_detects_non_finite_time(tmp_path):
    snapshot = create_worker_outputs(tmp_path)
    fn = tmp_path / "workers" / "half_quarter_summary_pandas.json"
    data = json.loads(fn.read_text())
    data["observations"][0]["elapsed_s"] = -1
    save_json(fn, data)
    with pytest.raises(ScaleError, match="timing"):
        summarize_trials(tmp_path, snapshot)


def test_source_contract_reject_missing(tmp_path):
    with pytest.raises(Exception):
        source_contract(tmp_path / "acq", tmp_path / "part2")


def test_normalize_missing_columns():
    incomplete = row()
    del incomplete["TOTAL_ASSETS"]
    with pytest.raises(ProcessingError, match="Unexpected result fields"):
        normalize_rows([incomplete], "quarter_summary")


def test_evidence_manifest_validation_and_tamper_rejection(tmp_path, monkeypatch):
    """Exercise the complete report/manifest verifier without Parquet engines."""
    import fdic_pipeline.scalable_processing as sp
    out = tmp_path / "evidence"
    snapshot = create_worker_outputs(out)
    audited = summarize_trials(out, snapshot)
    sp.write_figures(out, audited)
    sp.write_plan_discussion(out, audited)
    snapshot.update({"part2_run_id": "run_20261114T175314Z",
                     "part2_manifest_sha256": "a" * 64,
                     "part2_validation_sha256": "b" * 64,
                     "part2_panel_rows": 170})
    save_json(out / "input_snapshot.json", snapshot)
    save_json(out / "environment.json", {"seed": 11230945})
    files = [*sp.OUTPUT_FILES, *audited["worker_paths"]]
    files += [f"plans/{s}_{w}_{e}.txt"
              for s in SAMPLES for w in WORKLOADS for e in ENGINES]
    manifest = {"input_snapshot_sha256": sha256(out / "input_snapshot.json"),
                "environment_sha256": sha256(out / "environment.json"),
                "part2_run_id": snapshot["part2_run_id"],
                "part2_manifest_sha256": snapshot["part2_manifest_sha256"],
                "part2_validation_sha256": snapshot["part2_validation_sha256"],
                "source_rows": 170, "equivalence_passed": True,
                "measured_trials": 36, "warmup_trials": 12,
                "git_commit": "0" * 40,
                "assets": {name: sha256(out / name) for name in files}}
    save_json(out / "part3_manifest.json", manifest)
    orig = {"part2_run_id": snapshot["part2_run_id"],
            "part2_manifest_sha256": snapshot["part2_manifest_sha256"],
            "part2_validation_sha256": snapshot["part2_validation_sha256"],
            "part2_panel_rows": snapshot["part2_panel_rows"],
            "partitions": snapshot["partitions"]}
    monkeypatch.setattr(sp, "source_contract", lambda _a, _b: orig)
    verdict = sp.verify_result(tmp_path / "raw", tmp_path / "clean", out)
    assert verdict["success"] is True
    save_json(out / "part3_validation.json", verdict)
    assert sp.verify_result(tmp_path / "raw", tmp_path / "clean", out)["success"]
    with (out / "trials.csv").open("a") as f:
        f.write("unaudited trial\n")
    with pytest.raises(ScaleError, match="checksum mismatch"):
        sp.verify_result(tmp_path / "raw", tmp_path / "clean", out)
