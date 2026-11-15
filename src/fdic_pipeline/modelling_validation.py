"""Part 5 / Development-only modelling, cost-aware alarms and single test evaluation.

Execution states are deliberately separate: train creates a sealed model-selection
record without reading holdout Parquet; evaluate reads holdout after sealing and
writes a one-attempt marker; verify only checks immutable evidence.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import statistics
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from .acquisition import sha256
from .data_quality import parse_source_date
from .feature_pipeline import source_contract, verify_feature_pipeline, model_schema
from .feature_policy import DEV_END, TEST_START, TEST_END, FEATURE_NAMES, LABEL_NAME
from .model_policy import (
    SEED, FOLDS, MODEL_NAMES, CANDIDATE_MODELS, FALSE_ALARM_ASSET_RATE,
    TOP_FRACTIONS, make_candidate, select_by_average_precision, validate_folds,
)


class ModelError(RuntimeError):
    """A temporal, evidentiary, or once-only test protocol condition failed."""


BASE_ASSETS = (
    "development_folds.csv", "development_predictions.csv",
    "development_model_comparison.csv", "model_costs.json", "threshold_policy.json",
    "development_summary.json", "spark_mllib_demo.json", "selected_model.joblib",
    "baseline_model.joblib", "figures/development_pr.png",
    "figures/development_fold_ap.png",
)
TEST_ASSETS = (
    "test_metrics.csv", "test_predictions.parquet", "test_summary.json",
    "figures/test_pr.png", "figures/test_confusion.png",
)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_csv(path: Path, fields: tuple[str, ...], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="raise")
        writer.writeheader()
        writer.writerows(rows)


def load_model_partition_set(folder: Path, entries: list[dict], require_dates: tuple[date, date]):
    """Read exactly the manifest-listed Part 4 model-data partitions."""
    import pandas as pd
    import pyarrow.dataset as ds
    import pyarrow.parquet as pq

    expected = sorted((folder / e["path"]).resolve() for e in entries)
    if not expected or any(not p.is_file() for p in expected):
        raise ModelError("Missing Part 4 model input")
    for p, entry in zip(expected, sorted(entries, key=lambda e: str((folder / e["path"]).resolve()))):
        if folder.resolve() not in p.parents or sha256(p) != entry["sha256"]:
            raise ModelError("Changed/unsafe Part 4 model file")
        pf = pq.ParquetFile(p)
        if pf.schema_arrow != model_schema() or pf.metadata.num_rows != entry["rows"]:
            raise ModelError("Part 4 model schema or row count changed")
    names = ["CERT", "REPDTE", *FEATURE_NAMES, LABEL_NAME]
    frame = ds.dataset([str(p) for p in expected], format="parquet").to_table(columns=names).to_pandas()
    if len(frame) != sum(int(x["rows"]) for x in entries):
        raise ModelError("Part 4 model rows disagree with manifest")
    if list(frame.columns) != names:
        raise ModelError("Unexpected model columns")
    frame["REPDTE"] = pd.to_datetime(frame["REPDTE"]).dt.date
    if frame["REPDTE"].min() < require_dates[0] or frame["REPDTE"].max() > require_dates[1]:
        raise ModelError("Model records violate frozen temporal split")
    if frame.duplicated(["CERT", "REPDTE"]).any() or frame[LABEL_NAME].isna().any():
        raise ModelError("Duplicate keys or unknown labels in supervised input")
    if not frame[LABEL_NAME].isin([0, 1]).all():
        raise ModelError("Target is not binary")
    if any(frame[c].dtype.kind not in "fi" for c in FEATURE_NAMES):
        raise ModelError("Non-numeric predictor entered model data")
    return frame.sort_values(["REPDTE", "CERT"], kind="stable").reset_index(drop=True)


def load_development(part4: Path, manifest: dict):
    return load_model_partition_set(part4, manifest["development_partitions"],
                                    (date(1984, 3, 31), DEV_END))


def load_holdout(part4: Path, manifest: dict):
    return load_model_partition_set(part4, manifest["test_holdout_partitions"],
                                    (TEST_START, TEST_END))


def metrics_for_ranking(y_true: Any, scores: Any) -> dict:
    import numpy as np
    from sklearn.metrics import average_precision_score, roc_auc_score, brier_score_loss

    y = np.asarray(y_true, dtype=np.int8)
    p = np.asarray(scores, dtype=np.float64)
    if len(y) == 0 or len(y) != len(p) or not set(np.unique(y)) == {0, 1}:
        raise ModelError("Evaluation slice must contain positive and negative outcomes")
    if not np.isfinite(p).all() or (p < 0).any() or (p > 1).any():
        raise ModelError("Invalid prediction probabilities")
    order = np.argsort(-p, kind="stable")
    positive_total = int(y.sum())
    result = {
        "observations": int(len(y)), "positives": positive_total,
        "prevalence": float(y.mean()),
        "average_precision": float(average_precision_score(y, p)),
        "roc_auc": float(roc_auc_score(y, p)),
        "brier_score": float(brier_score_loss(y, p)),
    }
    for fraction in TOP_FRACTIONS:
        k = max(1, math.ceil(len(y) * fraction))
        found = int(y[order[:k]].sum())
        result[f"top_{round(fraction*100)}pct_precision"] = found / k
        result[f"top_{round(fraction*100)}pct_recall"] = found / positive_total
    return result


def confusion_cost(y_true: Any, scores: Any, threshold: float, fp_cost: float, fn_cost: float) -> dict:
    import numpy as np

    y = np.asarray(y_true, dtype=np.int8)
    p = np.asarray(scores, dtype=np.float64)
    if not (math.isfinite(threshold) and 0 <= threshold <= 1.000001):
        raise ModelError("Invalid threshold")
    alarm = p >= threshold
    tp = int(((y == 1) & alarm).sum())
    fp = int(((y == 0) & alarm).sum())
    tn = int(((y == 0) & ~alarm).sum())
    fn = int(((y == 1) & ~alarm).sum())
    return {
        "threshold": threshold, "tp": tp, "fp": fp, "tn": tn, "fn": fn,
        "precision": tp / (tp + fp) if tp + fp else 0.,
        "recall": tp / (tp + fn) if tp + fn else 0.,
        "false_alarms_per_1000": 1000.0 * fp / len(y),
        "alerts": tp + fp,
        "estimated_cost_thousands_usd": fp * fp_cost + fn * fn_cost,
    }


def cost_minimizing_threshold(y_true: Any, scores: Any, fp_cost: float, fn_cost: float) -> dict:
    """Optimize total dev OOF decision loss in O(n log n), break ties to fewer alerts.

    The score decision is >= threshold; grouped tied scores must be included
    together. No holdout rows may be used to estimate or tune this threshold.
    """
    import numpy as np

    y = np.asarray(y_true, dtype=np.int8)
    p = np.asarray(scores, dtype=np.float64)
    if len(y) != len(p) or not len(y) or not set(np.unique(y)) <= {0, 1}:
        raise ModelError("Invalid cost-minimization input")
    if not np.isfinite(p).all() or np.any((p < 0) | (p > 1)):
        raise ModelError("Out-of-range scores")
    if not all(math.isfinite(x) and x > 0 for x in (fp_cost, fn_cost)):
        raise ModelError("Both decision costs must be strictly positive")
    idx = np.argsort(-p, kind="stable")
    scores_sorted = p[idx]
    labels_sorted = y[idx]
    tp_cumulative = np.cumsum(labels_sorted, dtype=np.int64)
    fp_cumulative = np.arange(1, len(y) + 1) - tp_cumulative
    n_pos = int(y.sum())
    # A strict all-negative option is permitted even if the highest probability is 1.
    best_cost = float(n_pos * fn_cost)
    best_threshold = 1.000001
    best_alerts = 0
    unique_end = np.r_[np.flatnonzero(np.diff(scores_sorted) != 0), len(y)-1]
    for i in unique_end:
        false_positives = int(fp_cumulative[i])
        misses = int(n_pos - tp_cumulative[i])
        cost = false_positives * fp_cost + misses * fn_cost
        alarms = int(i + 1)
        threshold = float(scores_sorted[i])
        if cost < best_cost - 1e-9 or (abs(cost-best_cost) <= 1e-9 and alarms < best_alerts):
            best_cost, best_threshold, best_alerts = cost, threshold, alarms
    counts = confusion_cost(y, p, best_threshold, fp_cost, fn_cost)
    if not math.isclose(counts["estimated_cost_thousands_usd"], best_cost, rel_tol=1e-11, abs_tol=1e-6):
        raise ModelError("Threshold arithmetic mismatch")
    return counts


def estimate_development_costs(dev, failures: list[dict]) -> dict:
    """FDIC COST is observed; false-alert review cost is explicitly a scenario.

    COST and ASSET are in USD thousands. Costs are taken only from FDIC failures
    occurring on/before the frozen development end; non-positive/missing excluded.
    """
    import numpy as np

    observed = []
    distinct = set()
    development_certs = set(int(x) for x in dev["CERT"].unique())
    for record in failures:
        day = parse_source_date(record.get("FAILDATE"))
        cert = record.get("CERT")
        try:
            loss = float(record["COST"])
        except (ValueError, TypeError):
            continue
        if (day is None or day > DEV_END or cert is None or
            int(cert) not in development_certs or not math.isfinite(loss) or loss <= 0):
            continue
        ident = (int(cert), day.isoformat())
        if ident not in distinct:
            distinct.add(ident)
            observed.append(loss)
    if len(observed) < 20:
        raise ModelError("Not enough development-period positive FDIC COST observations")
    # log1p(ASSET) feature; inverse stays in FDIC's USD-thousands units.
    values = dev["LOG_ASSET"].to_numpy(dtype=float)
    values = values[np.isfinite(values)]
    if not len(values):
        raise ModelError("No valid development asset-scale feature")
    median_assets = float(np.expm1(np.median(values)))
    c_fp = median_assets * FALSE_ALARM_ASSET_RATE
    c_fn = statistics.median(observed)
    if c_fp <= 0 or c_fn <= 0:
        raise ModelError("Nonpositive decision-cost estimate")
    return {
        "false_alarm_cost_thousands_usd": c_fp,
        "missed_failure_cost_thousands_usd": c_fn,
        "failure_cost_source": "FDIC Failures.COST positive events dated <= development end and CERT in development",
        "usable_development_failure_cost_records": len(observed),
        "median_development_assets_thousands_usd": median_assets,
        "false_alarm_assumed_fraction_of_median_assets": FALSE_ALARM_ASSET_RATE,
        "false_alarm_cost_kind": "assumption-based supervisory review proxy; not an observed FDIC expense",
        "missed_failure_cost_kind": "FDIC resolution COST proxy; not causal loss prevented by early warning",
        "cost_unit": "thousand USD per bank-quarter decision",
    }


def _validate_part4(acquisition: Path, part2: Path, part4: Path) -> dict:
    """Verify completed Part 4 without reading test predictor contents."""
    if (part4 / "part4_validation.json").is_file():
        check = read_json(part4 / "part4_validation.json")
        if check.get("success") is not True:
            raise ModelError("Part 4 has not passed verification")
    else:
        raise ModelError("Part 4 validation is missing")
    # This contract checks hashes/manifest. Part 5 uses exact input file hashes
    # separately before reading each development/test partition.
    source, _ = source_contract(acquisition, part2)
    manifest = read_json(part4 / "part4_manifest.json")
    if manifest.get("lineage") != source:
        raise ModelError("Part 4 source lineage mismatch")
    if manifest.get("policy", {}).get("test_used_for_preprocessing") is not False:
        raise ModelError("Part 4 preprocessing touched holdout")
    if manifest.get("test_rows", 0) < 1 or manifest.get("development_rows", 0) < 1:
        raise ModelError("Empty Part 4 sample")
    if manifest.get("test_positive_banks", 0) < 50:
        raise ModelError("Part 4 does not satisfy test-failure minimum")
    if read_json(part4 / "part4_validation.json").get("manifest_sha256") != sha256(part4 / "part4_manifest.json"):
        raise ModelError("Stale Part 4 validation")
    return manifest


def _verify_part3(part3: Path | None, part2: Path) -> dict | None:
    """Optional read-only methodological provenance. Not a feature source."""
    if part3 is None:
        return None
    validation = read_json(part3 / "part3_validation.json")
    manifest = read_json(part3 / "part3_manifest.json")
    if validation.get("success") is not True or manifest.get("part2_run_id") != part2.name:
        raise ModelError("Part 3 provenance contradicts Part 2")
    if validation.get("manifest_sha256") != sha256(part3 / "part3_manifest.json"):
        raise ModelError("Part 3 manifest hash no longer matches its validation")
    return {"run_id": part3.name, "manifest_sha256": sha256(part3 / "part3_manifest.json")}


def _show_development_figures(output: Path, fold_rows: list[dict], y, p) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from sklearn.metrics import precision_recall_curve

    figdir = output / "figures"
    figdir.mkdir(parents=True, exist_ok=True)
    prec, recall, _ = precision_recall_curve(y, p)
    fig, ax = plt.subplots(figsize=(7.2, 4.5))
    ax.plot(recall, prec)
    ax.axhline(sum(y) / len(y), linestyle="--", label="OOF prevalence")
    ax.set(xlabel="Recall", ylabel="Precision", title="Development out-of-fold precision–recall")
    ax.legend()
    fig.tight_layout()
    fig.savefig(figdir / "development_pr.png", dpi=160)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(8, 4.2))
    for model in MODEL_NAMES:
        vals = [float(r["average_precision"]) for r in fold_rows if r["model"] == model]
        ax.plot(range(1, len(FOLDS) + 1), vals, "o-", label=model)
    ax.set(xlabel="Chronological validation fold", ylabel="Average precision",
           title="Development-only model comparison")
    ax.set_xticks(range(1, len(FOLDS) + 1))
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(figdir / "development_fold_ap.png", dpi=160)
    plt.close(fig)


def spark_mllib_development_demo(dev, output: Path) -> dict:
    """Small, chronological, development-only Spark MLlib proof of execution.

    Uses a fixed 5-column feature subset, Spark-native Imputer and LogisticRegression.
    Not a candidate in the sklearn model selection and never opens the test data.
    """
    import numpy as np
    from pyspark.sql import SparkSession
    from pyspark.ml import Pipeline as SparkPipeline
    from pyspark.ml.feature import Imputer, VectorAssembler
    from pyspark.ml.classification import LogisticRegression as SparkLogisticRegression

    fold = FOLDS[0]
    training = dev.loc[dev["REPDTE"] <= fold.training_end]
    validation = dev.loc[(dev["REPDTE"] >= fold.validation_start) &
                         (dev["REPDTE"] <= fold.validation_end)]
    features = ("LOG_ASSET", "EQUITY_ASSETS_PCT", "DEPOSITS_ASSETS_PCT",
                "LOANS_ASSETS_PCT", "ROA_PCT")

    def sample_frame(frame, max_negative, seed):
        pos = frame.loc[frame[LABEL_NAME] == 1]
        neg = frame.loc[frame[LABEL_NAME] == 0]
        if len(neg) > max_negative:
            neg = neg.sample(n=max_negative, random_state=seed)
        return __import__("pandas").concat([pos, neg], ignore_index=True)

    train_sample = sample_frame(training, 25000, SEED)
    val_sample = sample_frame(validation, 12000, SEED + 1)
    if train_sample[LABEL_NAME].nunique() != 2 or val_sample[LABEL_NAME].nunique() != 2:
        raise ModelError("Spark demo lacks class diversity")

    def as_records(frame):
        import pandas as pd
        data = frame[list(features) + [LABEL_NAME]].to_numpy(dtype=object)
        result = []
        for row in data:
            values = []
            for value in row[:len(features)]:
                if pd.isna(value):
                    values.append(float("nan"))
                else:
                    numeric = float(value)
                    values.append(numeric if math.isfinite(numeric) else float("nan"))
            result.append(tuple([*values, int(row[-1])]))
        return result

    import os
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    session = (SparkSession.builder.master("local[2]")
               .appName("BFT65B-Part5-Spark-MLlib-Demo")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.sql.session.timeZone", "UTC")
               .getOrCreate())
    try:
        session.sparkContext.setLogLevel("ERROR")
        names = list(features) + ["label"]
        train_s = session.createDataFrame(as_records(train_sample), schema=names)
        val_s = session.createDataFrame(as_records(val_sample), schema=names)
        clean = ["clean_" + x for x in features]
        pipe = SparkPipeline(stages=[
            Imputer(strategy="median", inputCols=list(features), outputCols=clean),
            VectorAssembler(inputCols=clean, outputCol="features"),
            SparkLogisticRegression(featuresCol="features", labelCol="label",
                                    maxIter=35, regParam=0.1, elasticNetParam=0.,
                                    standardization=True),
        ])
        fitted = pipe.fit(train_s)
        predicted = fitted.transform(val_s).select("label", "probability")
        outcome = predicted.rdd.map(lambda r: (int(r["label"]), float(r["probability"][1]))).collect()
        y = np.asarray([r[0] for r in outcome])
        p = np.asarray([r[1] for r in outcome])
        score = metrics_for_ranking(y, p)
        result = {
            "engine": "Spark MLlib, local[2]", "training_source": "development only",
            "fold": fold.name, "train_end": fold.training_end.isoformat(),
            "validation_start": fold.validation_start.isoformat(),
            "validation_end": fold.validation_end.isoformat(),
            "training_rows": len(train_sample), "validation_rows": len(val_sample),
            "predictors": list(features), "seed": SEED, "model": "Spark logistic regression",
            "preprocessing": "Spark Imputer(median) fitted on demo training only",
            "validation_metrics": score,
            "purpose": "API and time-ordered demonstration; not used for model selection or the holdout",
            "sampling_caveat": "All positive demo rows retained; negatives subsampled. Demo AP is not a population estimate.",
        }
    finally:
        session.stop()
    write_json(output / "spark_mllib_demo.json", result)
    return result


def train_development(acquisition_dir: Path, part2_dir: Path, part4_dir: Path,
                      output: Path, git_commit: str, part3_dir: Path | None = None,
                      spark_demo: bool = True) -> dict:
    """Estimate costs, compare models on purged historical folds and seal choices.

    By design this function never calls load_holdout(), and refuses to read test
    predictors/labels. Evaluation has a separate command and entry point.
    """
    import joblib
    import numpy as np
    import pandas as pd
    import pyarrow.parquet as pq

    for p in (acquisition_dir, part2_dir, part4_dir, output):
        if not isinstance(p, Path):
            raise TypeError("Paths must be pathlib.Path")
    if output.exists() or output.resolve() in part4_dir.resolve().parents or part4_dir.resolve() in output.resolve().parents:
        raise ModelError("Refuse to overwrite Part 4 or any existing modelling run")
    validate_folds()
    upstream = _validate_part4(acquisition_dir, part2_dir, part4_dir)
    p3 = _verify_part3(part3_dir, part2_dir)
    # Validate the actual Part 1 Failures Parquet and derive costs ONLY from dates
    # known by the development cutoff. No test-period event enters this estimate.
    acq_manifest = read_json(acquisition_dir / "manifest.json")
    failpath = acquisition_dir / "failures.parquet"
    from .acquisition import arrow_schema
    expected = acq_manifest["tables"]["failures"]
    fpf = pq.ParquetFile(failpath)
    if sha256(failpath) != expected["sha256"] or fpf.schema_arrow != arrow_schema("failures"):
        raise ModelError("Changed failure-cost evidence")
    failures = fpf.read(columns=["CERT", "FAILDATE", "COST"]).to_pylist()

    dev = load_development(part4_dir, upstream)
    costs = estimate_development_costs(dev, failures)
    dates = dev["REPDTE"]
    X = dev[list(FEATURE_NAMES)]
    y = dev[LABEL_NAME].astype(int).to_numpy()
    output.mkdir(parents=True, exist_ok=False)
    fold_metrics: list[dict] = []
    oof = {}
    fold_ranges = []
    for fold in FOLDS:
        train_idx = np.flatnonzero((dates <= fold.training_end).to_numpy())
        valid_idx = np.flatnonzero(((dates >= fold.validation_start) &
                                     (dates <= fold.validation_end)).to_numpy())
        if len(train_idx) == 0 or len(valid_idx) == 0 or len(set(y[train_idx])) != 2 or len(set(y[valid_idx])) != 2:
            raise ModelError(f"Invalid class balance or empty fixed fold: {fold.name}")
        if dates.iloc[train_idx].max() >= dates.iloc[valid_idx].min():
            raise ModelError("Temporal leakage between fold train/validation")
        fold_ranges.append({"fold":fold.name, "train_end":fold.training_end.isoformat(),
                            "validation_start":fold.validation_start.isoformat(),
                            "validation_end":fold.validation_end.isoformat(),
                            "training_rows":len(train_idx),"validation_rows":len(valid_idx),
                            "training_positives":int(y[train_idx].sum()),
                            "validation_positives":int(y[valid_idx].sum())})
        for name in MODEL_NAMES:
            estimator = make_candidate(name)  # fresh preprocessing per fold
            estimator.fit(X.iloc[train_idx], y[train_idx])
            pred = estimator.predict_proba(X.iloc[valid_idx])[:, 1]
            scores = metrics_for_ranking(y[valid_idx], pred)
            fold_metrics.append({"fold":fold.name,"model":name, **scores,
                                 "training_rows":len(train_idx),
                                 "validation_start":fold.validation_start.isoformat(),
                                 "validation_end":fold.validation_end.isoformat()})
            oof[(fold.name, name)] = (valid_idx, pred)

    selected = select_by_average_precision(fold_metrics)
    oof_indices = np.concatenate([oof[(fold.name, selected)][0] for fold in FOLDS])
    oof_scores = np.concatenate([oof[(fold.name, selected)][1] for fold in FOLDS])
    if len(set(oof_indices.tolist())) != len(oof_indices):
        raise ModelError("Overlapping validation indices")
    oof_y = y[oof_indices]
    scenarios = []
    for factor in (0.5, 1.0, 2.0):
        result = cost_minimizing_threshold(
            oof_y, oof_scores,
            costs["false_alarm_cost_thousands_usd"] * factor,
            costs["missed_failure_cost_thousands_usd"])
        scenarios.append({"false_alarm_multiplier":factor, **result})
    policy = {
        "model": selected,
        "method": "minimize empirical FP*C_FP+FN*C_FN using selected-model OOF development predictions",
        "threshold": next(x["threshold"] for x in scenarios if x["false_alarm_multiplier"] == 1.),
        "threshold_estimated_from": "purged development out-of-fold predictions only",
        "costs": costs, "scenarios":scenarios,
        "probability_calibration": "uncalibrated; threshold optimized empirically rather than treated as Bayes calibrated probability",
    }
    write_json(output / "threshold_policy.json", policy)
    write_json(output / "model_costs.json", costs)
    write_csv(output / "development_folds.csv", tuple(fold_metrics[0]), fold_metrics)
    write_csv(output / "development_model_comparison.csv",
              ("model", "mean_fold_ap", "mean_fold_roc_auc", "selected"),
              [{"model":name,
                "mean_fold_ap":statistics.mean(r["average_precision"] for r in fold_metrics if r["model"] == name),
                "mean_fold_roc_auc":statistics.mean(r["roc_auc"] for r in fold_metrics if r["model"] == name),
                "selected":name==selected} for name in MODEL_NAMES])
    rows = []
    for fold in FOLDS:
        idx, p = oof[(fold.name, selected)]
        for item, prob in zip(idx, p):
            rows.append({"fold":fold.name,"CERT":int(dev.iloc[item]["CERT"]),
                         "REPDTE":dev.iloc[item]["REPDTE"].isoformat(),
                         "Y_FAIL_4Q":int(y[item]),"probability":float(prob)})
    write_csv(output / "development_predictions.csv",
              ("fold", "CERT", "REPDTE", "Y_FAIL_4Q", "probability"), rows)
    _show_development_figures(output, fold_metrics, oof_y, oof_scores)
    # Final models trained only after CV and threshold are fixed. Neither model
    # has seen any test observation at this point.
    fitted_selected = make_candidate(selected)
    fitted_selected.fit(X, y)
    fitted_baseline = make_candidate("prior_baseline")
    fitted_baseline.fit(X, y)
    joblib.dump(fitted_selected, output / "selected_model.joblib", compress=3)
    joblib.dump(fitted_baseline, output / "baseline_model.joblib", compress=3)
    if spark_demo:
        spark_result = spark_mllib_development_demo(dev, output)
    else:
        spark_result = {"skipped":True}
        write_json(output / "spark_mllib_demo.json", spark_result)
    summary = {
        "run_id":output.name, "part4_run_id":part4_dir.name, "git_commit":git_commit,
        "model_input_features":list(FEATURE_NAMES), "development_rows":len(dev),
        "development_positive_labels":int(y.sum()),
        "development_distinct_positive_certs":int(dev.loc[y==1,"CERT"].nunique()),
        "fold_definitions":fold_ranges, "selection_metric":"equal-weight mean validation average precision",
        "selected_model":selected, "threshold":policy["threshold"],
        "holdout_read":False,"holdout_evaluated":False,
        "part3_provenance_only":p3,
        "spark_mllib_demo_success":not spark_result.get("skipped", False),
    }
    write_json(output / "development_summary.json", summary)
    assets = {name:sha256(output / name) for name in BASE_ASSETS}
    lock = {
        "state":"DEVELOPMENT_SELECTION_SEALED", "run_id":output.name,
        "source_part4_manifest_sha256":sha256(part4_dir / "part4_manifest.json"),
        "source_part4_validation_sha256":sha256(part4_dir / "part4_validation.json"),
        "source_part2_run_id":part2_dir.name,
        "source_part4_run_id":part4_dir.name,
        "part3_provenance_only":p3,
        "source_development_partitions":{e["path"]:e["sha256"] for e in upstream["development_partitions"]},
        "source_test_partitions":{e["path"]:e["sha256"] for e in upstream["test_holdout_partitions"]},
        "selected_model":selected, "threshold":policy["threshold"],
        "git_commit":git_commit, "files_sha256":assets,
    }
    write_json(output / "selection.lock.json", lock)
    return {"state":lock["state"],"selected_model":selected,"threshold":policy["threshold"],
            "development_rows":len(dev),"folds":len(FOLDS),"test_accessed":False,
            "spark_demo_passed":not spark_result.get("skipped",False),
            "output":str(output)}


def verify_selection_lock(acquisition: Path, part2: Path, part4: Path,
                          output: Path, lock: dict | None = None) -> tuple[dict, dict]:
    manifest = _validate_part4(acquisition, part2, part4)
    if lock is None:
        lock = read_json(output / "selection.lock.json")
    if lock.get("state") != "DEVELOPMENT_SELECTION_SEALED" or lock.get("source_part4_run_id") != part4.name:
        raise ModelError("Development selection is not sealed for the specified Part 4 run")
    if (lock.get("source_part4_manifest_sha256") != sha256(part4 / "part4_manifest.json") or
        lock.get("source_part4_validation_sha256") != sha256(part4 / "part4_validation.json") or
        lock.get("source_part2_run_id") != part2.name):
        raise ModelError("Upstream lineage changed since development selection")
    expected_dev = {e["path"]:e["sha256"] for e in manifest["development_partitions"]}
    expected_test = {e["path"]:e["sha256"] for e in manifest["test_holdout_partitions"]}
    if lock.get("source_development_partitions") != expected_dev or lock.get("source_test_partitions") != expected_test:
        raise ModelError("Part 4 input identity changed")
    if set(lock.get("files_sha256",{})) != set(BASE_ASSETS):
        raise ModelError("Incomplete selection evidence")
    for rel, checksum in lock["files_sha256"].items():
        if sha256(output / rel) != checksum:
            raise ModelError(f"Development evidence changed after selection: {rel}")
    threshold = read_json(output / "threshold_policy.json")
    if threshold.get("model") != lock["selected_model"] or threshold.get("threshold") != lock["threshold"]:
        raise ModelError("Locked decision policy changed")
    if read_json(output / "spark_mllib_demo.json").get("skipped"):
        raise ModelError("Mandatory Spark demo was skipped")
    return manifest, lock


def _test_figures(output: Path, y, score, stats: dict) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from sklearn.metrics import precision_recall_curve

    prec, recall, _ = precision_recall_curve(y, score)
    fig, ax = plt.subplots(figsize=(7.0, 4.5))
    ax.plot(recall, prec, label="Selected model")
    ax.axhline(sum(y)/len(y), linestyle="--", label="Holdout prevalence")
    ax.set(xlabel="Recall", ylabel="Precision", title="Final holdout precision–recall")
    ax.legend()
    fig.tight_layout()
    fig.savefig(output / "figures/test_pr.png", dpi=160)
    plt.close(fig)
    fig, ax = plt.subplots(figsize=(5.7, 4.5))
    matrix = [[stats["tn"], stats["fp"]], [stats["fn"], stats["tp"]]]
    im = ax.imshow(matrix, cmap="Blues")
    for i in range(2):
        for j in range(2):
            ax.text(j, i, format(matrix[i][j], ","), ha="center", va="center", color="black")
    ax.set_xticks([0, 1], labels=["No alarm", "Alarm"])
    ax.set_yticks([0, 1], labels=["No failure", "Failure"])
    ax.set(xlabel="Alarm decision", ylabel="Observed label", title="Frozen threshold: holdout confusion")
    fig.colorbar(im, ax=ax, shrink=0.78)
    fig.tight_layout()
    fig.savefig(output / "figures/test_confusion.png", dpi=160)
    plt.close(fig)


def evaluate_holdout_once(acquisition: Path, part2: Path, part4: Path,
                          output: Path) -> dict:
    """One access to held-out labelled records after all choices are sealed.

    The attempt sentinel is created *before* the first test Parquet load. An
    interrupted evaluation is not automatically retried; inspect and replan.
    """
    import joblib
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    manifest, lock = verify_selection_lock(acquisition, part2, part4, output)
    if (output / "test_attempt.json").exists() or (output / "evaluation_manifest.json").exists():
        raise ModelError("Holdout evaluation already attempted for this run; no repeat allowed")
    with (output / "test_attempt.json").open("x", encoding="utf-8") as f:
        json.dump({"state":"HOLDOUT_EVALUATION_STARTED", "utc":datetime.now(timezone.utc).isoformat(),
                   "development_lock_sha256":sha256(output / "selection.lock.json")}, f)
    # From this point onward the test data are opened once for evaluation.
    test = load_holdout(part4, manifest)
    X = test[list(FEATURE_NAMES)]
    y = test[LABEL_NAME].astype(int).to_numpy()
    if int(test.loc[y == 1, "CERT"].nunique()) != int(manifest["test_positive_banks"]):
        raise ModelError("Holdout positive-certificate count differs from the sealed Part 4 manifest")
    selected = joblib.load(output / "selected_model.joblib")
    baseline = joblib.load(output / "baseline_model.joblib")
    chosen_scores = selected.predict_proba(X)[:, 1]
    baseline_scores = baseline.predict_proba(X)[:, 1]
    policy = read_json(output / "threshold_policy.json")
    cost = policy["costs"]
    threshold = float(lock["threshold"])
    all_rows = []
    for name, probabilities in (("prior_baseline", baseline_scores), (lock["selected_model"], chosen_scores)):
        metric = metrics_for_ranking(y, probabilities)
        counts = confusion_cost(y, probabilities, threshold,
                                cost["false_alarm_cost_thousands_usd"],
                                cost["missed_failure_cost_thousands_usd"])
        all_rows.append({"model":name, **metric, **counts})
    write_csv(output / "test_metrics.csv", tuple(all_rows[0]), all_rows)
    prediction_rows = pd.DataFrame({
        "CERT":test["CERT"].to_numpy(dtype=np.int64),
        "REPDTE":pd.to_datetime(test["REPDTE"]),
        LABEL_NAME:y.astype(np.int8),
        "BASELINE_PROB":baseline_scores,
        "SELECTED_PROB":chosen_scores,
        "ALARM":(chosen_scores >= threshold).astype(np.int8),
    })
    path = output / "test_predictions.parquet"
    pq.write_table(pa.Table.from_pandas(prediction_rows, preserve_index=False), path,
                   compression="snappy")
    _test_figures(output, y, chosen_scores, all_rows[1])
    summary = {
        "evaluated_once":True,"test_rows":len(y),"test_positives":int(y.sum()),
        "distinct_positive_certs":int(test.loc[y == 1,"CERT"].nunique()),
        "test_period":[TEST_START.isoformat(), TEST_END.isoformat()],
        "model_locked_before_test":lock["selected_model"],
        "threshold_locked_before_test":threshold,
        "model_choice_or_threshold_changed_after_test":False,
        "test_only_used_for_metrics":True,
        "test_metrics":all_rows,
        "decision_costs":cost,
    }
    write_json(output / "test_summary.json", summary)
    assets = {rel:sha256(output / rel) for rel in TEST_ASSETS}
    evaluation_manifest = {
        "state":"FINAL_HOLDOUT_EVALUATED_ONCE",
        "development_lock_sha256":sha256(output / "selection.lock.json"),
        "test_attempt_sha256":sha256(output / "test_attempt.json"),
        "selected_model":lock["selected_model"],
        "locked_threshold":threshold,
        "test_input_sha256":lock["source_test_partitions"],
        "test_assets_sha256":assets,
    }
    write_json(output / "evaluation_manifest.json", evaluation_manifest)
    verdict = verify_evidence(acquisition, part2, part4, output, require_test=True)
    write_json(output / "part5_validation.json", verdict)
    return verdict


def verify_evidence(acquisition: Path, part2: Path, part4: Path,
                    output: Path, require_test: bool = True) -> dict:
    """Independent check from saved scored holdout; never score/retrain test again."""
    import pandas as pd
    import numpy as np
    import pyarrow.parquet as pq

    _, lock = verify_selection_lock(acquisition, part2, part4, output)
    if not require_test:
        return {"success":True, "state":"DEVELOPMENT_SELECTION_SEALED", "holdout_evaluated":False}
    em = read_json(output / "evaluation_manifest.json")
    if (em.get("state") != "FINAL_HOLDOUT_EVALUATED_ONCE" or
        em.get("development_lock_sha256") != sha256(output / "selection.lock.json") or
        em.get("test_attempt_sha256") != sha256(output / "test_attempt.json") or
        em.get("selected_model") != lock["selected_model"] or
        em.get("locked_threshold") != lock["threshold"] or
        em.get("test_input_sha256") != lock["source_test_partitions"]):
        raise ModelError("Invalid once-only evaluation lineage")
    if set(em.get("test_assets_sha256",{})) != set(TEST_ASSETS):
        raise ModelError("Incomplete test evidence inventory")
    for rel, checksum in em["test_assets_sha256"].items():
        if sha256(output / rel) != checksum:
            raise ModelError("Changed test evidence: " + rel)
    # Recompute *only* from already-saved predictions. No new test predictions.
    table = pq.read_table(output / "test_predictions.parquet").to_pandas()
    y = table[LABEL_NAME].to_numpy(dtype=np.int8)
    score = table["SELECTED_PROB"].to_numpy(dtype=float)
    base = table["BASELINE_PROB"].to_numpy(dtype=float)
    if table.duplicated(["CERT", "REPDTE"]).any():
        raise ModelError("Duplicate test scoring keys")
    threshold = float(lock["threshold"])
    if not np.array_equal(table["ALARM"].to_numpy(dtype=np.int8),
                          (score >= threshold).astype(np.int8)):
        raise ModelError("Saved test alarm decisions changed")
    policy = read_json(output / "threshold_policy.json")["costs"]
    rows = []
    for name, arr in (("prior_baseline",base),(lock["selected_model"],score)):
        rows.append({"model":name, **metrics_for_ranking(y,arr),
                     **confusion_cost(y,arr,threshold,
                        policy["false_alarm_cost_thousands_usd"],
                        policy["missed_failure_cost_thousands_usd"])})
    with (output / "test_metrics.csv").open(newline="", encoding="utf-8") as f:
        saved = list(csv.DictReader(f))
    if len(saved) != len(rows):
        raise ModelError("Missing test model results")
    for observed, expected in zip(saved, rows):
        for key, val in expected.items():
            if isinstance(val, str):
                if observed[key] != val:
                    raise ModelError("Changed test model name")
            elif not math.isclose(float(observed[key]), float(val), rel_tol=1e-10, abs_tol=1e-7):
                raise ModelError(f"Recomputed test metric changed: {key}")
    old = output / "part5_validation.json"
    if old.is_file():
        prior = read_json(old)
        if prior.get("success") is not True or prior.get("evaluation_manifest_sha256") != sha256(output / "evaluation_manifest.json"):
            raise ModelError("Stale Part 5 verification")
    return {"success":True, "state":"FINAL_HOLDOUT_EVALUATED_ONCE",
            "source_part4_run":part4.name, "model":lock["selected_model"],
            "threshold":threshold,"development_folds":len(FOLDS),
            "test_rows":len(y), "test_positives":int(y.sum()),
            "holdout_evaluated_once":True,
            "evaluation_manifest_sha256":sha256(output / "evaluation_manifest.json")}


def cli(argv: list[str] | None = None) -> int:
    import logging
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("train", "evaluate", "verify"):
        p = sub.add_parser(name)
        p.add_argument("--acquisition-dir", type=Path, required=True)
        p.add_argument("--part2-dir", type=Path, required=True)
        p.add_argument("--part4-dir", type=Path, required=True)
        p.add_argument("--output-dir", type=Path, required=True)
        if name == "train":
            p.add_argument("--part3-dir", type=Path)
            p.add_argument("--git-commit", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "train":
            result = train_development(args.acquisition_dir, args.part2_dir,
                                       args.part4_dir, args.output_dir,
                                       args.git_commit, part3_dir=args.part3_dir)
        elif args.command == "evaluate":
            result = evaluate_holdout_once(args.acquisition_dir, args.part2_dir,
                                           args.part4_dir, args.output_dir)
        else:
            result = verify_evidence(args.acquisition_dir, args.part2_dir,
                                     args.part4_dir, args.output_dir)
        print(json.dumps(result, indent=2, allow_nan=False))
        return 0
    except (ModelError, ValueError, TypeError, OSError, KeyError, ImportError) as exc:
        logging.error("Part 5 failed: %s", exc)
        return 1
