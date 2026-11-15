"""Part 4 / Verified bank-risk features, four-quarter labels, and train-only preprocessing.

Source: completed Part 2 run, with its Part 1 acquisition hashes. Outputs are
new run-scoped artifacts. No API calls, updates to Part 1–3, or model evaluation.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import math
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .acquisition import sha256, quarter_ends, DEFAULT_START, DEFAULT_END
from .feature_policy import (
    AVAILABILITY_LAG_DAYS, DEV_END, FEATURE_NAMES, LABEL_LAST_QUARTER,
    LABEL_NAME, STUDY_END, TEST_END, TEST_START, add_quarters,
    build_features, label_for_report, quarter_ordinal, split_for_quarter,
    VALID_TERMINAL_CLASSES, FEATURE_DETAILS,
)
from .panel import panel_schema
from .workflow import acquisition_contract

LOG = logging.getLogger("fdic_part4")
VERSION = 1
RUN_FILES = (
    "quarterly_label_audit.csv", "label_status_summary.csv",
    "feature_dictionary.csv", "sample_summary.json", "preprocessing_fit.json",
    "training_preprocessor.joblib", "figures/labels_by_period.png",
    "figures/development_feature_missingness.png",
)


class FeaturePipelineError(RuntimeError):
    """Source-lineage, target, feature or output-evidence validation failed."""


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def read_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise FeaturePipelineError(f"Expected JSON object: {path}")
    return data


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_csv(path: Path, columns: tuple[str, ...], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="raise")
        writer.writeheader()
        writer.writerows(rows)


def source_contract(acquisition_dir: Path, part2_dir: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    """Check Part 2 is the exact completed derivative of Part 1."""
    acquisition_dir, part2_dir = Path(acquisition_dir), Path(part2_dir)
    acquisition = acquisition_contract(acquisition_dir)
    m = read_json(part2_dir / "part2_manifest.json")
    v = read_json(part2_dir / "part2_validation.json")
    if (v.get("success") is not True
        or v.get("manifest_sha256") != sha256(part2_dir / "part2_manifest.json")
        or m.get("source_manifest_sha256") != acquisition["manifest_sha256"]
        or m.get("source_verification_sha256") != acquisition["verification_report_sha256"]
        or m.get("source_rows") != acquisition["financial_rows"]
        or m.get("panel_rows") != v.get("clean_rows")
        or m.get("panel_rows") + m.get("quarantined_rows", -1) != acquisition["financial_rows"]
        or m.get("quarters") != 170 or v.get("quarters") != 170
        or m.get("exit_classification_is_retrospective_only") is not True
        or m.get("lag_assumption_days") != AVAILABILITY_LAG_DAYS):
        raise FeaturePipelineError("Part 1–2 lineage, schema scope, or availability policy disagrees")
    entries = m.get("partitions", [])
    expected_quarters = set(quarter_ends(DEFAULT_START, DEFAULT_END))
    if len(entries) != 170 or {x["quarter"] for x in entries} != expected_quarters:
        raise FeaturePipelineError("Incomplete Part 2 source partition inventory")
    if sum(x["rows"] for x in entries) != m["panel_rows"]:
        raise FeaturePipelineError("Part 2 row accounting mismatch")
    for name in ("terminal_exits.csv", "failure_linkage.csv"):
        p = part2_dir / name
        if not p.is_file() or sha256(p) != m.get("audit_hashes", {}).get(name):
            raise FeaturePipelineError(f"Retrospective event audit hash mismatch: {name}")
    if m.get("source_distinct_certs") != acquisition["distinct_financial_certs"]:
        raise FeaturePipelineError("Inconsistent number of source institutions")
    source_id = {
        "part1_manifest_sha256": acquisition["manifest_sha256"],
        "part1_verification_sha256": acquisition["verification_report_sha256"],
        "part2_run_id": part2_dir.name,
        "part2_manifest_sha256": sha256(part2_dir / "part2_manifest.json"),
        "part2_validation_sha256": sha256(part2_dir / "part2_validation.json"),
        "part2_panel_rows": m["panel_rows"],
        "part2_quarters": 170,
        "part2_terminal_audit_sha256": sha256(part2_dir / "terminal_exits.csv"),
    }
    return source_id, m


def load_terminal_events(part2_dir: Path, expected_certs: int) -> dict[int, dict[str, str]]:
    events: dict[int, dict[str, str]] = {}
    with (part2_dir / "terminal_exits.csv").open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            cert = int(row["CERT"])
            if cert <= 0 or cert in events:
                raise FeaturePipelineError(f"Repeated/invalid terminal CERT: {cert}")
            if row.get("TERMINAL_CLASS") not in VALID_TERMINAL_CLASSES:
                raise FeaturePipelineError(f"Unknown terminal class: {row.get('TERMINAL_CLASS')}")
            events[cert] = row
    if len(events) != expected_certs:
        raise FeaturePipelineError("Exit audit must account for all source CERTs")
    return events


def feature_table_schema() -> Any:
    import pyarrow as pa
    fields = [pa.field("CERT", pa.int64(), nullable=False),
              pa.field("REPDTE", pa.date32(), nullable=False),
              pa.field("ASSUMED_AVAILABLE_FROM", pa.date32(), nullable=False)]
    fields += [pa.field(name, pa.float64(), nullable=True) for name in FEATURE_NAMES]
    fields += [pa.field(LABEL_NAME, pa.int8(), nullable=True),
               pa.field("LABEL_STATUS", pa.string(), nullable=False),
               pa.field("SPLIT", pa.string(), nullable=False)]
    return pa.schema(fields)


def model_schema() -> Any:
    import pyarrow as pa
    fields = [pa.field("CERT", pa.int64(), nullable=False),
              pa.field("REPDTE", pa.date32(), nullable=False)]
    fields += [pa.field(name, pa.float64(), nullable=True) for name in FEATURE_NAMES]
    fields += [pa.field(LABEL_NAME, pa.int8(), nullable=False)]
    return pa.schema(fields)


def _write_partition(output: Path, relative: Path, rows: list[dict[str, Any]], schema: Any) -> dict[str, Any]:
    import pyarrow as pa
    import pyarrow.parquet as pq
    p = output / relative
    p.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, p, compression="snappy", row_group_size=100_000)
    if pq.ParquetFile(p).schema_arrow != schema:
        raise FeaturePipelineError(f"Output schema mismatch: {relative}")
    return {"path": relative.as_posix(), "rows": len(rows), "sha256": sha256(p)}


def make_unfitted_preprocessor() -> Any:
    """A fresh sklearn template; clone/refit it inside each Part 5 time fold."""
    from sklearn.impute import SimpleImputer
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import RobustScaler
    return Pipeline([
        ("median_imputer", SimpleImputer(strategy="median", add_indicator=True,
                                         keep_empty_features=True)),
        ("robust_scaler", RobustScaler(quantile_range=(25.0, 75.0))),
    ])


def fit_development_pipeline(output: Path, dev_entries: list[dict[str, Any]],
                             feature_names: tuple[str, ...] = FEATURE_NAMES) -> dict[str, Any]:
    """Fit transforms ONLY on development rows, without accessing test files."""
    import joblib
    import numpy as np
    import pandas as pd
    import sklearn
    import pyarrow.dataset as ds
    if not dev_entries or not all(x["path"].startswith("model_data/development/") for x in dev_entries):
        raise FeaturePipelineError("Preprocessor cannot fit without development-only inputs")
    paths = [str(output / entry["path"]) for entry in dev_entries]
    table = ds.dataset(paths, format="parquet").to_table(columns=list(feature_names))
    df = table.to_pandas()
    if list(df.columns) != list(feature_names) or len(df) != sum(e["rows"] for e in dev_entries):
        raise FeaturePipelineError("Development fit input mismatch")
    all_missing = [name for name in feature_names if df[name].isna().all()]
    preprocessor = make_unfitted_preprocessor()
    preprocessor.fit(df)
    transformed = preprocessor.transform(df.iloc[:min(1000, len(df))])
    if not np.isfinite(transformed).all():
        raise FeaturePipelineError("Development-only preprocessing yields nonfinite values")
    model_path = output / "training_preprocessor.joblib"
    joblib.dump(preprocessor, model_path, compress=3)
    check = joblib.load(model_path)
    check_out = check.transform(df.iloc[:min(10, len(df))])
    if not np.allclose(check_out, preprocessor.transform(df.iloc[:min(10, len(df))])):
        raise FeaturePipelineError("Serialized training preprocessor changed its fitted transform")
    fit_data = {
        "fitted_on": "development_only",
        "development_end": DEV_END.isoformat(),
        "test_start": TEST_START.isoformat(),
        "test_holdout_transformed": False,
        "training_rows": len(df),
        "n_raw_features": len(feature_names),
        "n_transformed_columns": int(transformed.shape[1]),
        "feature_names": list(feature_names),
        "all_missing_in_development": all_missing,
        "transform_steps": ["median_imputer(add_indicator=True,keep_empty_features=True)",
                            "robust_scaler(25th,75th)"],
        "sklearn_version": sklearn.__version__,
        "pandas_version": pd.__version__,
        "numpy_version": np.__version__,
        "dev_files_sha256": {x["path"]: x["sha256"] for x in dev_entries},
        "preprocessor_sha256": sha256(model_path),
    }
    write_json(output / "preprocessing_fit.json", fit_data)
    return fit_data


def write_figures(output: Path, quarters: list[dict[str, Any]],
                  dev_miss: Counter, dev_rows: int) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    out = output / "figures"
    out.mkdir(parents=True, exist_ok=True)
    q = [r for r in quarters if r["split"] in {"development", "test_holdout"}]
    labels = ["Development", "Test holdout"]
    positive = [sum(r["positive_rows"] for r in q if r["split"] == s)
                for s in ("development", "test_holdout")]
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.bar(labels, positive)
    ax.set_ylabel("Positive bank-quarter labels")
    ax.set_title("Four-quarter failure labels by temporal sample")
    fig.tight_layout()
    fig.savefig(out / "labels_by_period.png", dpi=160)
    plt.close(fig)
    names = list(FEATURE_NAMES)
    missing = [100 * dev_miss[x] / dev_rows for x in names]
    order = sorted(range(len(names)), key=lambda i: missing[i], reverse=True)[:15]
    fig, ax = plt.subplots(figsize=(9, 5.4))
    ax.barh([names[i] for i in reversed(order)], [missing[i] for i in reversed(order)])
    ax.set_xlabel("Missing in development sample (%)")
    ax.set_title("Feature missingness before development-only preprocessing")
    fig.tight_layout()
    fig.savefig(out / "development_feature_missingness.png", dpi=160)
    plt.close(fig)


def build_feature_pipeline(acquisition_dir: Path, part2_dir: Path, output: Path,
                           git_commit: str = "not_recorded", min_test_failed_banks: int = 50) -> dict[str, Any]:
    """Create one immutable Part 4 run; never overwrite source or a previous run."""
    import pyarrow.parquet as pq
    if min_test_failed_banks < 0:
        raise ValueError("Minimum test failures must be nonnegative")
    acquisition_dir, part2_dir, output = map(lambda x: Path(x).resolve(),
                                             (acquisition_dir, part2_dir, output))
    if output.exists() or any(
        output == root or output in root.parents or root in output.parents
        for root in (part2_dir, acquisition_dir)
    ):
        raise FeaturePipelineError("Use a new Part 4 output directory outside Part 1–2")
    lineage, m = source_contract(acquisition_dir, part2_dir)
    terminals = load_terminal_events(part2_dir, m["source_distinct_certs"])
    entry_map = {x["quarter"]: x for x in m["partitions"]}
    expected_source_schema = panel_schema()
    output.mkdir(parents=True)
    history: dict[int, dict[int, dict[str, Any]]] = defaultdict(dict)
    files: list[dict[str, Any]] = []
    dev_files: list[dict[str, Any]] = []
    test_files: list[dict[str, Any]] = []
    audits: list[dict[str, Any]] = []
    counts = Counter()
    positive_certs = {"development": set(), "test_holdout": set()}
    dev_miss = Counter()
    dev_rows = 0
    full_feature_schema = feature_table_schema()
    train_schema = model_schema()
    prev_date: date | None = None
    for idx, (quarter, part) in enumerate(sorted(entry_map.items()), start=1):
        qdate = datetime.strptime(quarter, "%Y%m%d").date()
        src = (part2_dir / part["relative_path"]).resolve()
        if part2_dir not in src.parents or not src.is_file() or sha256(src) != part["sha256"]:
            raise FeaturePipelineError(f"Source Parquet digest mismatch: {quarter}")
        parquet = pq.ParquetFile(src)
        if parquet.schema_arrow != expected_source_schema or parquet.metadata.num_rows != part["rows"]:
            raise FeaturePipelineError(f"Source Parquet schema/row mismatch: {quarter}")
        rows = parquet.read().to_pylist()
        if any(r["REPDTE"] != qdate for r in rows):
            raise FeaturePipelineError(f"Unexpected report date in {quarter}")
        if prev_date is not None and quarter_ordinal(qdate) != quarter_ordinal(prev_date) + 1:
            raise FeaturePipelineError("Source quarter calendar gap")
        prev_date = qdate
        if qdate > LABEL_LAST_QUARTER:
            # The final four source quarters are necessary for label ascertainment
            # but cannot themselves be modelling observations.
            continue
        split = split_for_quarter(qdate)
        qidx = quarter_ordinal(qdate)
        full_rows: list[dict[str, Any]] = []
        model_rows: list[dict[str, Any]] = []
        status = Counter()
        observed_certs: set[int] = set()
        for row in rows:
            cert = row["CERT"]
            if cert in observed_certs or cert not in terminals:
                raise FeaturePipelineError(f"Duplicate/unknown CERT in quarter {quarter}: {cert}")
            observed_certs.add(cert)
            available = row["ASSUMED_AVAILABLE_FROM"]
            if available != qdate + timedelta(days=AVAILABILITY_LAG_DAYS):
                raise FeaturePipelineError("Part 2 availability assumption differs from Part 4")
            past = history[cert]
            derived = build_features(row, past)
            y, reason = label_for_report(qdate, terminals[cert], AVAILABILITY_LAG_DAYS)
            status[reason] += 1
            counts[reason] += 1
            full_rows.append({"CERT": cert, "REPDTE": qdate,
                              "ASSUMED_AVAILABLE_FROM": available, **derived,
                              LABEL_NAME: y, "LABEL_STATUS": reason, "SPLIT": split})
            if split in ("development", "test_holdout") and y is not None:
                model_rows.append({"CERT": cert, "REPDTE": qdate, **derived, LABEL_NAME: y})
                if y == 1:
                    positive_certs[split].add(cert)
                if split == "development":
                    dev_rows += 1
                    for name, value in derived.items():
                        if value is None:
                            dev_miss[name] += 1
            past[qidx] = {"ASSET": row.get("ASSET"), "DEP": row.get("DEP"),
                          "ROA": row.get("ROA")}
            for old in [key for key in past if key < qidx - 4]:
                del past[old]
        yr, qu = qdate.year, qdate.month // 3
        rel = Path(f"feature_table/year={yr}/quarter={qu}/part-00000.parquet")
        feature_record = _write_partition(output, rel, full_rows, full_feature_schema)
        feature_record["quarter"] = quarter
        files.append(feature_record)
        if split in ("development", "test_holdout"):
            model_rel = Path(f"model_data/{split}/year={yr}/quarter={qu}/part-00000.parquet")
            model_entry = _write_partition(output, model_rel, model_rows, train_schema)
            model_entry["quarter"] = quarter
            if split == "development":
                dev_files.append(model_entry)
            else:
                test_files.append(model_entry)
        audits.append({"quarter": quarter, "split": split, "source_rows": len(rows),
                       "feature_rows": len(full_rows), "labeled_rows": len(model_rows),
                       "positive_rows": sum(r[LABEL_NAME] == 1 for r in model_rows),
                       "censored_rows": len(rows) - sum(v for k, v in status.items()
                                                          if k in ("failure_within_4q", "observed_no_failure_4q")),
                       "status_counts": dict(status)})
        if idx % 25 == 0:
            LOG.info("Created features through %s (%s/%s source quarters)", quarter, idx, 170)
    if len(files) != 166 or len(dev_files) != 92 or len(test_files) != 28:
        raise FeaturePipelineError("Unexpected model quarter coverage")
    dev_positive_banks = len(positive_certs["development"])
    test_positive_banks = len(positive_certs["test_holdout"])
    if test_positive_banks < min_test_failed_banks:
        raise FeaturePipelineError(
            f"Test interval contains only {test_positive_banks} distinct positive CERTs; "
            f"minimum is {min_test_failed_banks}. Do not silently change the split.")
    if dev_rows < 1:
        raise FeaturePipelineError("Development sample is empty")
    write_csv(output / "quarterly_label_audit.csv",
              ("quarter", "split", "source_rows", "feature_rows", "labeled_rows",
               "positive_rows", "censored_rows"),
              [{k: v for k, v in a.items() if k != "status_counts"} for a in audits])
    write_csv(output / "label_status_summary.csv", ("status", "rows"),
              [{"status": k, "rows": v} for k, v in sorted(counts.items())])
    # The dictionary records the economic purpose, exact calculation and
    # information timing of each fixed predictor. This is not learned from test.
    write_csv(output / "feature_dictionary.csv",
              ("feature", "category", "calculation", "risk_rationale", "input_timing"),
              [{"feature": name, "category": FEATURE_DETAILS[name][0],
                "calculation": FEATURE_DETAILS[name][1],
                "risk_rationale": FEATURE_DETAILS[name][2],
                "input_timing": FEATURE_DETAILS[name][3]}
               for name in FEATURE_NAMES])
    summary = {
        "development": {"start": "1984-03-31", "end": DEV_END.isoformat(),
                        "rows": sum(x["rows"] for x in dev_files),
                        "positive_bank_quarters": sum(x["positive_rows"] for x in audits if x["split"] == "development"),
                        "distinct_positive_certs": dev_positive_banks},
        "purge_gap": {"start": "2007-03-31", "end": "2007-12-31", "quarters": 4},
        "test_holdout": {"start": TEST_START.isoformat(), "end": TEST_END.isoformat(),
                         "rows": sum(x["rows"] for x in test_files),
                         "positive_bank_quarters": sum(x["positive_rows"] for x in audits if x["split"] == "test_holdout"),
                         "distinct_positive_certs": test_positive_banks,
                         "evaluated_in_part4": False},
        "unused_post_test": {"start": "2015-03-31", "end": LABEL_LAST_QUARTER.isoformat()},
        "model_last_eligible_report": LABEL_LAST_QUARTER.isoformat(),
        "label_definition": "1{FAILDATE in (REPDTE, REPDTE+4 calendar quarters]} with publication/exit censoring",
        "availability_lag_assumption_days": AVAILABILITY_LAG_DAYS,
        "label_status_counts": dict(counts),
    }
    write_json(output / "sample_summary.json", summary)
    fit_info = fit_development_pipeline(output, dev_files)
    write_figures(output, audits, dev_miss, dev_rows)
    manifest = {
        "version": VERSION, "created_utc": now_utc(),
        "git_commit": git_commit, "lineage": lineage,
        "source_terminal_audit_sha256": lineage["part2_terminal_audit_sha256"],
        "policy": {"target": LABEL_NAME, "label_max_quarter": LABEL_LAST_QUARTER.isoformat(),
                   "availability_lag_days": AVAILABILITY_LAG_DAYS,
                   "development_end": DEV_END.isoformat(),
                   "gap_quarters": 4, "test_start": TEST_START.isoformat(),
                   "test_end": TEST_END.isoformat(),
                   "minimum_test_positive_banks": min_test_failed_banks,
                   "feature_names": list(FEATURE_NAMES),
                   "test_used_for_preprocessing": False,
                   "ambiguous_and_competing_exits_censored": True},
        "features_partitions": files, "development_partitions": dev_files,
        "test_holdout_partitions": test_files,
        "feature_rows": sum(x["rows"] for x in files),
        "development_rows": sum(x["rows"] for x in dev_files),
        "test_rows": sum(x["rows"] for x in test_files),
        "test_positive_banks": test_positive_banks,
        "fit_development_rows": fit_info["training_rows"],
        "file_sha256": {name: sha256(output / name) for name in RUN_FILES},
    }
    write_json(output / "part4_manifest.json", manifest)
    verdict = verify_feature_pipeline(acquisition_dir, part2_dir, output)
    write_json(output / "part4_validation.json", verdict)
    return verdict


def verify_feature_pipeline(acquisition_dir: Path, part2_dir: Path, output: Path) -> dict[str, Any]:
    """Read-only independent verification; no source or report outputs overwritten."""
    import pyarrow.parquet as pq
    output = Path(output).resolve()
    lineage, src = source_contract(acquisition_dir, part2_dir)
    manifest = read_json(output / "part4_manifest.json")
    if manifest.get("lineage") != lineage or manifest.get("policy", {}).get("feature_names") != list(FEATURE_NAMES):
        raise FeaturePipelineError("Feature lineage or feature selection has changed")
    # Verify source bytes, not only the manifest's own checksum. A source file
    # rewritten in place must invalidate the Part 4 evidence.
    for source_entry in src["partitions"]:
        original_file = (Path(part2_dir).resolve() / source_entry["relative_path"]).resolve()
        if Path(part2_dir).resolve() not in original_file.parents or not original_file.is_file():
            raise FeaturePipelineError("Missing Part 2 input partition")
        if sha256(original_file) != source_entry["sha256"]:
            raise FeaturePipelineError("Changed Part 2 input partition")
    if manifest.get("policy", {}).get("test_used_for_preprocessing") is not False:
        raise FeaturePipelineError("Unverified train-only preprocessing")
    if manifest.get("policy", {}).get("gap_quarters") < 4:
        raise FeaturePipelineError("Temporal purge gap is shorter than four quarters")
    if manifest.get("test_positive_banks", 0) < manifest.get("policy", {}).get("minimum_test_positive_banks", 50):
        raise FeaturePipelineError("Insufficient distinct test failure certificates")
    checks = (("features_partitions", 166, feature_table_schema()),
              ("development_partitions", 92, model_schema()),
              ("test_holdout_partitions", 28, model_schema()))
    totals = {}
    for key, required, schema in checks:
        entries = manifest.get(key, [])
        if len(entries) != required:
            raise FeaturePipelineError(f"Unexpected {key} count")
        seen = set()
        totals[key] = 0
        for item in entries:
            q = item["quarter"]
            if q in seen:
                raise FeaturePipelineError("Repeated quarter in Part 4 output")
            seen.add(q)
            p = (output / item["path"]).resolve()
            if output not in p.parents or not p.is_file() or sha256(p) != item["sha256"]:
                raise FeaturePipelineError(f"Output partition hash mismatch: {p}")
            pf = pq.ParquetFile(p)
            if pf.schema_arrow != schema or pf.metadata.num_rows != item["rows"]:
                raise FeaturePipelineError(f"Output partition schema/row mismatch: {p}")
            totals[key] += item["rows"]
    if (totals["features_partitions"] != manifest.get("feature_rows")
        or totals["development_partitions"] != manifest.get("development_rows")
        or totals["test_holdout_partitions"] != manifest.get("test_rows")):
        raise FeaturePipelineError("Part 4 output row accounting mismatch")
    input_before_cutoff = sum(x["rows"] for x in src["partitions"]
                              if x["quarter"] <= LABEL_LAST_QUARTER.strftime("%Y%m%d"))
    if totals["features_partitions"] != input_before_cutoff:
        raise FeaturePipelineError("Some eligible source bank-quarter records are missing")
    # Recalculate labels and split assignments from the immutable terminal-event
    # audit and cross-check the exact model-row projection. This independent
    # pass ensures that target and eligibility logic did not silently change.
    events = load_terminal_events(Path(part2_dir), src["source_distinct_certs"])
    expected_models = {
        "development": {e["quarter"]: e for e in manifest["development_partitions"]},
        "test_holdout": {e["quarter"]: e for e in manifest["test_holdout_partitions"]},
    }
    audit_positive_cert: dict[str, set[int]] = {"development": set(), "test_holdout": set()}
    for entry in manifest["features_partitions"]:
        records = pq.ParquetFile(output / entry["path"]).read().to_pylist()
        qdate = datetime.strptime(entry["quarter"], "%Y%m%d").date()
        split = split_for_quarter(qdate)
        model_expected = []
        unique_keys = set()
        for record in records:
            cert = record["CERT"]
            if cert in unique_keys or record["REPDTE"] != qdate or record["SPLIT"] != split:
                raise FeaturePipelineError("Invalid feature-table key/date/split")
            unique_keys.add(cert)
            if cert not in events or record["ASSUMED_AVAILABLE_FROM"] != qdate + timedelta(days=90):
                raise FeaturePipelineError("Unverified feature availability or terminal CERT")
            expected_label, expected_status = label_for_report(qdate, events[cert])
            if record[LABEL_NAME] != expected_label or record["LABEL_STATUS"] != expected_status:
                raise FeaturePipelineError(f"Label/eligibility mismatch at {cert}/{qdate}")
            if split in expected_models and expected_label is not None:
                model_expected.append({"CERT": cert, "REPDTE": qdate,
                                       **{name: record[name] for name in FEATURE_NAMES},
                                       LABEL_NAME: expected_label})
                if expected_label == 1:
                    audit_positive_cert[split].add(cert)
        if split in expected_models:
            actual = pq.ParquetFile(output / expected_models[split][entry["quarter"]]["path"]).read().to_pylist()
            if actual != model_expected:
                raise FeaturePipelineError("Model data is not an exact labeled feature projection")
    if len(audit_positive_cert["test_holdout"]) != manifest["test_positive_banks"]:
        raise FeaturePipelineError("Distinct positive test-certificate count mismatch")
    for filename in RUN_FILES:
        p = (output / filename).resolve()
        if output not in p.parents or not p.is_file() or sha256(p) != manifest["file_sha256"].get(filename):
            raise FeaturePipelineError(f"Modified/missing Part 4 evidence: {filename}")
    pre = read_json(output / "preprocessing_fit.json")
    if (pre.get("fitted_on") != "development_only"
        or pre.get("test_holdout_transformed") is not False
        or pre.get("training_rows") != totals["development_partitions"]
        or pre.get("preprocessor_sha256") != sha256(output / "training_preprocessor.joblib")
        or pre.get("feature_names") != list(FEATURE_NAMES)
        or pre.get("dev_files_sha256") != {x["path"]: x["sha256"] for x in manifest["development_partitions"]}):
        raise FeaturePipelineError("Fitted preprocessor does not match development-only input")
    existing = output / "part4_validation.json"
    if existing.is_file():
        old = read_json(existing)
        if old.get("success") is not True or old.get("manifest_sha256") != sha256(output / "part4_manifest.json"):
            raise FeaturePipelineError("Stale validation record")
    return {"success": True, "verified_at_utc": now_utc(),
            "part2_run_id": lineage["part2_run_id"],
            "manifest_sha256": sha256(output / "part4_manifest.json"),
            "feature_rows": totals["features_partitions"],
            "development_rows": totals["development_partitions"],
            "test_holdout_rows": totals["test_holdout_partitions"],
            "test_distinct_positive_certs": manifest["test_positive_banks"],
            "feature_count": len(FEATURE_NAMES),
            "development_only_preprocessing": True,
            "holdout_not_evaluated": True,
            "verified_feature_partitions": 166}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="command", required=True)
    for cmd in ("run", "verify"):
        sp = sub.add_parser(cmd)
        sp.add_argument("--acquisition-dir", required=True, type=Path)
        sp.add_argument("--part2-dir", required=True, type=Path)
        sp.add_argument("--output-dir", required=True, type=Path)
        if cmd == "run":
            sp.add_argument("--git-commit", default="not_recorded")
            sp.add_argument("--min-test-failed-banks", type=int, default=50)
    a = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    try:
        if a.command == "run":
            v = build_feature_pipeline(a.acquisition_dir, a.part2_dir, a.output_dir,
                                       a.git_commit, a.min_test_failed_banks)
        else:
            v = verify_feature_pipeline(a.acquisition_dir, a.part2_dir, a.output_dir)
        print(json.dumps(v, indent=2))
        return 0
    except (FeaturePipelineError, OSError, ValueError, KeyError, ImportError, RuntimeError) as exc:
        LOG.error("Part 4 failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
