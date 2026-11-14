"""Part 3 / Controlled pandas–Polars–PySpark benchmark and evidence audit.

Inputs: immutable Part 2 Parquet panel and Part 1/2 integrity metadata.
Outputs: timed trials, full cross-engine numerical parity checks, optimized
execution plans, measured two-size scaling, figures, and provenance manifests.

The Spark configuration is local[2]; no distributed-cluster performance is claimed.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import math
import os
import random
import shutil
import statistics
import subprocess
import sys
import threading
import time
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from .acquisition import sha256, quarter_ends, DEFAULT_END, DEFAULT_START
from .processing_engines import (
    ENGINES, WORKLOADS, REPORT_START, ProcessingError, compare_rows,
    engine_plan, evaluate, spark_session,
)
from .workflow import acquisition_contract

LOG = logging.getLogger("part3")
SAMPLES = ("half", "full")
SEED = 11230945
WARMUPS = 1
REPETITIONS = 3
SCHEMA_FINGERPRINT = 1
OUTPUT_FILES = (
    "input_snapshot.json", "environment.json", "trials.csv",
    "performance_summary.csv", "cross_engine_comparisons.csv", "scaling.csv",
    "equivalence.json", "plan_inspection.csv", "execution_plan_discussion.md",
    "figures/runtime_comparison.png", "figures/scaling_comparison.png",
    "figures/peak_process_rss.png",
)


class ScaleError(RuntimeError):
    """A benchmark stage, parity check, or provenance condition failed."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def save_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False,
                               allow_nan=False) + "\n", encoding="utf-8")


def read_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def write_csv(path: Path, fields: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="raise")
        writer.writeheader()
        writer.writerows(rows)


def source_contract(acquisition_dir: Path, part2_run_dir: Path) -> dict[str, Any]:
    """Fail closed if Part 2 does not belong to the verified Part 1 source."""
    if not part2_run_dir.name.startswith("run_"):
        raise ScaleError("Part 2 input must be a named run_... directory")
    acq = acquisition_contract(acquisition_dir)
    manifest_path = part2_run_dir / "part2_manifest.json"
    validation_path = part2_run_dir / "part2_validation.json"
    if not manifest_path.is_file() or not validation_path.is_file():
        raise ScaleError("Part 2 manifest or validation report is missing")
    m = read_json(manifest_path)
    v = read_json(validation_path)
    if v.get("success") is not True or v.get("manifest_sha256") != sha256(manifest_path):
        raise ScaleError("Part 2 validation report is failed or stale")
    if (m.get("source_manifest_sha256") != acq["manifest_sha256"] or
            m.get("source_verification_sha256") != acq["verification_report_sha256"]):
        raise ScaleError("Part 2 source lineage differs from verified Part 1")
    if (m.get("source_rows") != acq["financial_rows"] or
            m.get("panel_rows") != v.get("clean_rows") or
            m.get("quarters") != 170 or
            v.get("quarters") != 170 or
            v.get("source_rows") != acq["financial_rows"] or
            m.get("panel_rows", -1) + m.get("quarantined_rows", -1) != acq["financial_rows"]):
        raise ScaleError("Part 1–2 record accounting mismatch")
    if m.get("exit_classification_is_retrospective_only") is not True:
        raise ScaleError("Retrospective exit data not separated in Part 2")
    expected = set(quarter_ends(DEFAULT_START, DEFAULT_END))
    partitions = m.get("partitions")
    if not isinstance(partitions, list) or len(partitions) != len(expected):
        raise ScaleError("Part 2 partition inventory is incomplete")
    quarters: set[str] = set()
    total = 0
    records: list[dict[str, Any]] = []
    for entry in partitions:
        q = entry.get("quarter")
        if q not in expected or q in quarters or not isinstance(entry.get("rows"), int):
            raise ScaleError(f"Invalid Part 2 quarter record: {q}")
        p = (part2_run_dir / entry.get("relative_path", "")).resolve()
        if part2_run_dir.resolve() not in p.parents or not p.is_file():
            raise ScaleError(f"Unsafe/missing Part 2 partition path: {p}")
        if not p.relative_to(part2_run_dir.resolve()).parts[0] == "panel":
            raise ScaleError("Part 2 input path is outside panel/")
        if not isinstance(entry.get("sha256"), str) or len(entry["sha256"]) != 64:
            raise ScaleError(f"Missing source checksum for {q}")
        quarters.add(q)
        total += entry["rows"]
        records.append({"quarter": q, "rows": entry["rows"],
                        "sha256": entry["sha256"],
                        "relative_path": str(p.relative_to(part2_run_dir.resolve()))})
    if quarters != expected or total != m["panel_rows"]:
        raise ScaleError("Unexpected quarter coverage or total panel rows")
    records.sort(key=lambda d: d["quarter"])
    return {
        "part1_manifest_sha256": acq["manifest_sha256"],
        "part1_verification_sha256": acq["verification_report_sha256"],
        "part2_run_id": part2_run_dir.name,
        "part2_manifest_sha256": sha256(manifest_path),
        "part2_validation_sha256": sha256(validation_path),
        "part2_panel_rows": total,
        "part2_quarters": 170,
        "panel_fields": 44,
        "partitions": records,
    }


def check_local_inputs(part2: Path, snap: dict[str, Any], full_root: Path,
                       half_root: Path | None = None, copy: bool = False) -> dict[str, int]:
    """Verify all source hashes and stage immutable Parquet copies onto local disk.

    The half corpus is the last 85 quarters. No synthetic rows, replication or
    interpolation are introduced. Both engines read identical local files.
    """
    records = snap["partitions"]
    half = {r["quarter"] for r in records[-85:]}
    expected_rows = {"half": sum(r["rows"] for r in records[-85:]),
                     "full": sum(r["rows"] for r in records)}
    for entry in records:
        rel = Path(entry["relative_path"])
        src = part2 / rel
        if sha256(src) != entry["sha256"]:
            raise ScaleError(f"Part 2 source checksum changed: {rel}")
        dest = full_root / rel
        if copy:
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dest)
        if not dest.is_file() or sha256(dest) != entry["sha256"]:
            raise ScaleError(f"Local panel file mismatch: {dest}")
        if half_root is not None and entry["quarter"] in half:
            half_dest = half_root / rel
            if copy:
                half_dest.parent.mkdir(parents=True, exist_ok=True)
                # Hardlink on the same local filesystem avoids doubling disk usage.
                try:
                    os.link(dest, half_dest)
                except OSError:
                    shutil.copy2(dest, half_dest)
            if not half_dest.is_file() or sha256(half_dest) != entry["sha256"]:
                raise ScaleError(f"Half-sample file mismatch: {half_dest}")
    return expected_rows


def sample_rows(snapshot: dict[str, Any], sample: str, workload: str) -> int:
    records = snapshot["partitions"][-85:] if sample == "half" else snapshot["partitions"]
    if workload == "quarter_summary":
        return sum(r["rows"] for r in records
                   if datetime.strptime(r["quarter"], "%Y%m%d").date() >= REPORT_START)
    return sum(r["rows"] for r in records)


def _rss_tree_mib() -> float:
    import psutil
    main = psutil.Process(os.getpid())
    try:
        procs = [main, *main.children(recursive=True)]
    except psutil.Error:
        procs = [main]
    total = 0
    for p in procs:
        try:
            total += p.memory_info().rss
        except psutil.Error:
            pass
    return total / (1024 ** 2)


def measured_call(func: Any) -> tuple[Any, float, float, float]:
    """Clock execution+result materialization; sample Python+child/JVM RSS.

    Resident memory is a sampled process-tree estimate (50 ms resolution).
    It is not a precise heap statistic and is not directly comparable to a
    single-process ru_maxrss reading from Part 1.
    """
    stop = threading.Event()
    values = [_rss_tree_mib()]
    baseline = values[0]

    def sample() -> None:
        while not stop.wait(0.05):
            try:
                values.append(_rss_tree_mib())
            except OSError:
                pass

    monitor = threading.Thread(target=sample, daemon=True)
    monitor.start()
    begin = time.perf_counter()
    try:
        output = func()
    finally:
        elapsed = time.perf_counter() - begin
        stop.set()
        monitor.join(timeout=2)
        values.append(_rss_tree_mib())
    return output, elapsed, baseline, max(values)


def run_worker(panel_dir: Path, engine: str, workload: str, target: Path,
               warmups: int = WARMUPS, repetitions: int = REPETITIONS) -> None:
    """One process/session per engine × input size × workload scenario."""
    if engine not in ENGINES or workload not in WORKLOADS:
        raise ScaleError("Unknown engine/workload")
    if warmups != 1 or repetitions != 3:
        raise ScaleError("Protocol requires exactly one warmup and three measured trials")
    if not panel_dir.is_dir():
        raise ScaleError(f"Missing local panel: {panel_dir}")
    spark = None
    if engine == "pyspark":
        spark = spark_session()
        spark.sparkContext.setLogLevel("ERROR")
    try:
        plan = engine_plan(engine, panel_dir, workload, spark=spark)
        outcomes = []
        reference = None
        for index in range(-warmups, repetitions):
            rows, secs, rss_base, rss_peak = measured_call(
                lambda: evaluate(engine, panel_dir, workload, spark=spark)
            )
            if reference is None:
                reference = rows
            else:
                compare_rows(reference, rows, workload)
            outcomes.append({
                "trial_index": index, "warmup": index < 0,
                "elapsed_s": secs, "baseline_tree_rss_mib": rss_base,
                "peak_tree_rss_mib": rss_peak, "output_rows": len(rows),
                "result": rows,
            })
            LOG.info("%s/%s trial=%s elapsed=%.3fs peak=%.1fMiB", engine, workload,
                     index, secs, rss_peak)
        save_json(target, {"engine": engine, "workload": workload,
                           "plan": plan, "observations": outcomes})
    finally:
        if spark is not None:
            spark.stop()


def assert_result_semantics(rows: list[dict[str, Any]], workload: str,
                            expected_input_rows: int) -> None:
    if not rows or sum(x["BANK_QUARTERS"] for x in rows) != expected_input_rows:
        raise ScaleError(f"Row accounting mismatch for {workload}")
    if workload == "adjacent_quarter_growth":
        if any(r["CONSECUTIVE_PAIRS"] > r["BANK_QUARTERS"] or
               (r["CONSECUTIVE_PAIRS"] == 0 and
                r["MEAN_ASSET_GROWTH_PCT"] is not None) for r in rows):
            raise ScaleError("Incorrect adjacent-quarter semantics")


def summarize_trials(output: Path, snapshot: dict[str, Any],
                     warmups: int = WARMUPS, repetitions: int = REPETITIONS
                     ) -> dict[str, Any]:
    """Independent numerical audit of every saved worker/trial result."""
    trials: list[dict[str, Any]] = []
    overview = []
    plan_inspection = []
    eq = []
    worker_paths: list[str] = []
    for sample in SAMPLES:
        for workload in WORKLOADS:
            ref = None
            for engine in ENGINES:
                rel = f"workers/{sample}_{workload}_{engine}.json"
                path = output / rel
                if not path.is_file():
                    raise ScaleError(f"Missing worker results: {rel}")
                worker_paths.append(rel)
                data = read_json(path)
                if data.get("engine") != engine or data.get("workload") != workload:
                    raise ScaleError(f"Worker identity mismatch: {rel}")
                obs = data.get("observations", [])
                if len(obs) != warmups + repetitions:
                    raise ScaleError(f"Wrong number of trials in {rel}")
                sample_count = sample_rows(snapshot, sample, workload)
                for idx, row in enumerate(obs):
                    trial_index = idx - warmups
                    if (row.get("trial_index") != trial_index or
                            row.get("warmup") is not (trial_index < 0)):
                        raise ScaleError(f"Incorrect trial identity at {rel}/{idx}")
                    seconds = row.get("elapsed_s")
                    base = row.get("baseline_tree_rss_mib")
                    peak = row.get("peak_tree_rss_mib")
                    if not all(isinstance(v, (int, float)) and math.isfinite(v)
                               for v in (seconds, base, peak)) or not (seconds > 0 and 0 <= base <= peak):
                        raise ScaleError("Invalid timing or sampled memory")
                    actual = row.get("result", [])
                    assert_result_semantics(actual, workload, sample_count)
                    if row.get("output_rows") != len(actual):
                        raise ScaleError("Incorrect output row count")
                    if ref is None:
                        ref = actual
                    else:
                        compare_rows(ref, actual, workload)
                    trials.append({
                        "sample": sample, "workload": workload, "engine": engine,
                        "trial_index": trial_index, "warmup": trial_index < 0,
                        "input_rows": sample_count, "output_rows": len(actual),
                        "elapsed_s": seconds, "baseline_tree_rss_mib": base,
                        "peak_tree_rss_mib": peak,
                    })
                if engine == "pandas":
                    reference_rows = ref
                else:
                    compare_rows(reference_rows, ref, workload)
                eq.append({"sample": sample, "workload": workload, "engine": engine,
                           "matches_pandas": True, "rows_compared": len(ref),
                           "rtol": 1e-10, "atol": 1e-7})
                measured = [t for t in trials if t["sample"] == sample and
                            t["workload"] == workload and t["engine"] == engine and
                            not t["warmup"]]
                overview.append({
                    "sample": sample, "workload": workload, "engine": engine,
                    "input_rows": sample_count, "output_rows": len(ref),
                    "measured_repetitions": repetitions,
                    "median_seconds": statistics.median(t["elapsed_s"] for t in measured),
                    "min_seconds": min(t["elapsed_s"] for t in measured),
                    "max_seconds": max(t["elapsed_s"] for t in measured),
                    "median_peak_tree_rss_mib": statistics.median(t["peak_tree_rss_mib"]
                                                              for t in measured),
                })
                plan = data.get("plan")
                if not isinstance(plan, str) or not plan.strip():
                    raise ScaleError("Missing execution plan")
                plan_rel = f"plans/{sample}_{workload}_{engine}.txt"
                (output / plan_rel).parent.mkdir(parents=True, exist_ok=True)
                (output / plan_rel).write_text(plan, encoding="utf-8")
                lower = plan.lower()
                plan_inspection.append({
                    "sample": sample, "workload": workload, "engine": engine,
                    "plan_file": plan_rel,
                    "parquet_scan": engine != "pandas" and
                    any(token in lower for token in ("parquet scan", "scan parquet", "parquetscan", "scan ", "file scan")),
                    "filter_in_plan": "selection:" in lower or "filter" in lower or "pushedfilters" in lower,
                    "aggregation_in_plan": "aggregate" in lower or "groupby" in lower or "group_by" in lower,
                    "exchange_or_shuffle": "exchange" in lower or "shuffle" in lower,
                    "window_in_plan": "window" in lower,
                })
    if len(trials) != len(SAMPLES) * len(WORKLOADS) * len(ENGINES) * (warmups+repetitions):
        raise ScaleError("Incomplete trial matrix")
    by_key = {(r["sample"], r["workload"], r["engine"]): r for r in overview}
    comparisons = []
    for sample in SAMPLES:
        for workload in WORKLOADS:
            panda = by_key[(sample, workload, "pandas")]["median_seconds"]
            polars = by_key[(sample, workload, "polars")]["median_seconds"]
            spark = by_key[(sample, workload, "pyspark")]["median_seconds"]
            comparisons.append({"sample": sample, "workload": workload,
                                "input_rows": by_key[(sample, workload, "pandas")]["input_rows"],
                                "pandas_median_s": panda,
                                "polars_median_s": polars,
                                "pyspark_median_s": spark,
                                "pandas_over_polars": panda / polars,
                                "pandas_over_pyspark": panda / spark})
    scaling = []
    for engine in ENGINES:
        for workload in WORKLOADS:
            half = by_key[("half", workload, engine)]
            full = by_key[("full", workload, engine)]
            scaling.append({"engine": engine, "workload": workload,
                            "half_input_rows": half["input_rows"],
                            "full_input_rows": full["input_rows"],
                            "row_count_ratio": full["input_rows"] / half["input_rows"],
                            "half_median_s": half["median_seconds"],
                            "full_median_s": full["median_seconds"],
                            "time_ratio": full["median_seconds"] / half["median_seconds"]})
    write_csv(output / "trials.csv", ["sample", "workload", "engine", "trial_index",
             "warmup", "input_rows", "output_rows", "elapsed_s",
             "baseline_tree_rss_mib", "peak_tree_rss_mib"], trials)
    write_csv(output / "performance_summary.csv", ["sample", "workload", "engine",
             "input_rows", "output_rows", "measured_repetitions", "median_seconds",
             "min_seconds", "max_seconds", "median_peak_tree_rss_mib"], overview)
    write_csv(output / "cross_engine_comparisons.csv", ["sample", "workload", "input_rows",
             "pandas_median_s", "polars_median_s", "pyspark_median_s",
             "pandas_over_polars", "pandas_over_pyspark"], comparisons)
    write_csv(output / "scaling.csv", ["engine", "workload", "half_input_rows",
             "full_input_rows", "row_count_ratio", "half_median_s", "full_median_s",
             "time_ratio"], scaling)
    write_csv(output / "plan_inspection.csv", ["sample", "workload", "engine",
             "plan_file", "parquet_scan", "filter_in_plan", "aggregation_in_plan",
             "exchange_or_shuffle", "window_in_plan"], plan_inspection)
    save_json(output / "equivalence.json", {"success": True,
              "comparison": "All result rows, exact dates/counts/nulls, numeric rtol=1e-10 atol=1e-7",
              "audited_scenarios": eq})
    return {"trials": trials, "summary": overview, "comparisons": comparisons,
            "scaling": scaling, "plans": plan_inspection, "worker_paths": worker_paths}


def write_figures(output: Path, audited: dict[str, Any]) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    figdir = output / "figures"
    figdir.mkdir(parents=True, exist_ok=True)
    summary = audited["summary"]
    for what, filename, ylabel in [
        ("median_seconds", "runtime_comparison.png", "Median elapsed time (seconds)"),
        ("median_peak_tree_rss_mib", "peak_process_rss.png", "Median sampled process-tree RSS (MiB)"),
    ]:
        full = [r for r in summary if r["sample"] == "full"]
        labels = list(WORKLOADS)
        x = list(range(len(labels)))
        fig, ax = plt.subplots(figsize=(8, 4.3))
        for idx, engine in enumerate(ENGINES):
            values = [next(r[what] for r in full if r["workload"] == w and r["engine"] == engine)
                      for w in labels]
            bars = ax.bar([k + (idx - 1) * 0.25 for k in x], values, width=0.24, label=engine)
            ax.bar_label(bars, fmt="%.2f", fontsize=7, padding=3)
        ax.set_xticks(x, labels=["Quarterly aggregation", "Adjacent-quarter growth"])
        ax.set_ylabel(ylabel)
        ax.legend(ncol=3)
        ax.set_ylim(bottom=0, top=max(r[what] for r in full) * 1.28)
        fig.tight_layout()
        fig.savefig(figdir / filename, dpi=160)
        plt.close(fig)
    fig, ax = plt.subplots(figsize=(8, 4.5))
    for w, marker in (("quarter_summary", "o"), ("adjacent_quarter_growth", "s")):
        for engine in ENGINES:
            data = sorted([r for r in summary if r["engine"] == engine and r["workload"] == w],
                          key=lambda r: r["input_rows"])
            ax.plot([r["input_rows"] / 1e6 for r in data],
                    [r["median_seconds"] for r in data], label=f"{engine} / {w}", marker=marker)
    ax.set_xlabel("Observed input rows (millions; no duplication)")
    ax.set_ylabel("Median elapsed time (seconds)")
    ax.legend(fontsize=7, ncol=2)
    fig.tight_layout()
    fig.savefig(figdir / "scaling_comparison.png", dpi=160)
    plt.close(fig)


def write_plan_discussion(output: Path, audits: dict[str, Any]) -> None:
    plans = audits["plans"]
    def get(sample: str, workload: str, engine: str) -> dict[str, Any]:
        return next(r for r in plans if (r["sample"], r["workload"], r["engine"]) ==
                    (sample, workload, engine))
    polars_agg = get("full", "quarter_summary", "polars")
    polars_lag = get("full", "adjacent_quarter_growth", "polars")
    spark_agg = get("full", "quarter_summary", "pyspark")
    spark_lag = get("full", "adjacent_quarter_growth", "pyspark")
    summary = audits["comparisons"]
    lines = [
        "# Part 3 — Execution plans and scaling interpretation", "",
        "## Scope", "",
        "One local Colab machine, two CPU workers, the immutable Part 2 panel, and two",
        "observed sample sizes (latest 85 versus all 170 quarterly partitions).",
        "The results are *not* measurements from a Spark cluster.", "",
        "## Polars LazyFrame", "",
        "The source is scanned lazily. No input Parquet data are materialized during",
        "query construction or `.explain(optimized=True)`; `.collect()` triggers execution.",
        f"For quarterly aggregation, the captured plan contains a Parquet scan: {polars_agg['parquet_scan']};",
        f"and a filter/selection indicator: {polars_agg['filter_in_plan']}.",
        "The optimized plan must be inspected directly for the number of projected columns",
        "and predicate pushdown; an indicator alone does not establish pushdown.",
        f"For adjacent-quarter growth, a window indicator was found: {polars_lag['window_in_plan']}.",
        "The global sort and CERT-partitioned shift require ordering; the adjacent-quarter",
        "check prevents treating missing reporting quarters as consecutive observations.", "",
        "## Spark SQL physical plan", "",
        f"Quarterly aggregation shows an exchange/shuffle marker: {spark_agg['exchange_or_shuffle']}.",
        f"The chronological workload shows a Window marker: {spark_lag['window_in_plan']},",
        f"and exchange/shuffle marker: {spark_lag['exchange_or_shuffle']}.",
        "Inspect formatted physical plans for FileScan, PushedFilters, aggregate, sort,",
        "window and exchange operators. Adaptive query execution may revise the final plan.",
        "Spark is configured as local[2], so an Exchange is a shuffle on one machine,",
        "not evidence of network traffic on a real multi-node cluster.", "",
        "## Fairness and interpretation", "",
        "All engines use the same staged Parquet files and return identical quarter-level",
        "tables within declared floating-point tolerances. Pandas uses PyArrow reader",
        "column and predicate pushdown, avoiding an artificially weak pandas baseline.",
        "The timed interval includes Parquet reading, computation and result materialization",
        "but excludes engine imports, SparkSession/JVM startup and plotting.",
        "All scenarios perform one warmup followed by three measured trials in one process.",
        "Sampled RSS adds Python processes and their children (including Spark JVM);",
        "it is an approximate whole-process footprint, not net dataframe allocation.",
        "The first/recent-85 sample changes the observed history available to the lag",
        "operator. Therefore results across sizes are different but internally equivalent",
        "across engines for each size; scalability is described, not claimed linear.", "",
        "## Cluster decision", "",
        "Use `performance_summary.csv` and `scaling.csv` to judge the observed full-panel",
        "time and RAM requirements. If the measured Polars workload fits comfortably",
        "on one machine, these results do not support renting a Spark cluster now.",
        "A cluster may become appropriate when production data exceed single-node memory,",
        "I/O throughput, required concurrency, or operational deadlines. Establishing an",
        "actual break-even point requires a larger workload and a real multi-node Spark run,",
        "including provisioning, shuffle, storage and communication overhead.", "",
        "## Selected full-panel median timings", "",
    ]
    for r in summary:
        if r["sample"] == "full":
            lines.append(f"- {r['workload']}: pandas {r['pandas_median_s']:.3f}s, "
                         f"Polars {r['polars_median_s']:.3f}s, Spark {r['pyspark_median_s']:.3f}s; "
                         f"pandas/Polars {r['pandas_over_polars']:.3f}×")
    lines += ["", "Execution plans are preserved verbatim in `plans/`.", ""]
    (output / "execution_plan_discussion.md").write_text("\n".join(lines), encoding="utf-8")


def environment_info(seed: int, repo_sha: str) -> dict[str, Any]:
    import pandas
    import polars
    import pyspark
    import pyarrow
    import psutil
    return {"timestamp_utc": utc_now(), "python": sys.version.split()[0],
            "pandas": pandas.__version__, "polars": polars.__version__,
            "pyspark": pyspark.__version__, "pyarrow": pyarrow.__version__,
            "psutil": psutil.__version__, "cpu_logical": os.cpu_count(),
            "limited_worker_threads": 2, "spark_master": "local[2]",
            "spark_shuffle_partitions": 8, "benchmark_seed": seed,
            "warmups_per_scenario": WARMUPS,
            "measured_repetitions_per_scenario": REPETITIONS,
            "memory_method": "sampled RSS: Python process + descendants, 50ms, including Spark JVM",
            "time_method": "monotonic wall clock: Parquet read to materialized result, excludes initialization",
            "repo_git_sha": repo_sha}


def benchmark(source_acq: Path, part2: Path, output: Path, local: Path,
              repo_sha: str, seed: int = SEED) -> dict[str, Any]:
    """Execute a new Part 3 study. Does not overwrite a previous result."""
    source_acq, part2, output, local = map(Path, (source_acq, part2, output, local))
    if output.exists():
        raise ScaleError(f"Output exists; choose a new unique run ID: {output}")
    if output.resolve() in part2.resolve().parents or part2.resolve() in output.resolve().parents:
        raise ScaleError("Benchmark output must not be written inside Part 2")
    if not isinstance(repo_sha, str) or len(repo_sha) != 40 or any(c not in "0123456789abcdef" for c in repo_sha):
        raise ScaleError("A full 40-character Git commit SHA is required")
    contract = source_contract(source_acq, part2)
    local_full = local / "full" / "panel"
    local_half = local / "half" / "panel"
    if local.exists():
        raise ScaleError(f"Local staging directory exists; choose a fresh directory: {local}")
    free = shutil.disk_usage(local.parent).free / (1024 ** 3)
    if free < 5.0:
        raise ScaleError(f"At least 5 GiB local free disk required; available {free:.2f}")
    local.mkdir(parents=True)
    output.mkdir(parents=True)
    expected_counts = check_local_inputs(part2, contract, local_full, local_half, copy=True)
    contract["sample_input_rows"] = expected_counts
    contract["sample_calendar"] = {
        "half": [r["quarter"] for r in contract["partitions"][-85:]],
        "full": [r["quarter"] for r in contract["partitions"]],
    }
    contract["part2_dir"] = str(part2)
    contract["acquisition_dir"] = str(source_acq)
    save_json(output / "input_snapshot.json", contract)
    save_json(output / "environment.json", environment_info(seed, repo_sha))
    order = [(s, w, e) for s in SAMPLES for w in WORKLOADS for e in ENGINES]
    random.Random(seed).shuffle(order)
    script = Path(__file__).resolve().parents[2] / "run_scalable_processing.py"
    if not script.is_file():
        raise ScaleError(f"Missing benchmark worker entry point: {script}")
    for i, (sample, workload, engine) in enumerate(order, 1):
        result_path = output / "workers" / f"{sample}_{workload}_{engine}.json"
        result_path.parent.mkdir(parents=True, exist_ok=True)
        cmd = [sys.executable, str(script), "worker", "--panel-dir",
               str(local / sample / "panel"), "--engine", engine,
               "--workload", workload, "--result-file", str(result_path)]
        LOG.info("Experiment %s/%s: %s/%s/%s", i, len(order), sample, workload, engine)
        proc = subprocess.run(cmd, text=True, capture_output=True, timeout=1200,
                              env={**os.environ, "POLARS_MAX_THREADS": "2",
                                   "OMP_NUM_THREADS": "2", "OPENBLAS_NUM_THREADS": "2",
                                   "MKL_NUM_THREADS": "2", "ARROW_NUM_THREADS": "2"})
        if proc.returncode != 0:
            raise ScaleError(f"Worker failed {sample}/{workload}/{engine}:\n"
                             f"STDOUT:\n{proc.stdout[-3000:]}\nSTDERR:\n{proc.stderr[-7000:]}")
        if not result_path.is_file():
            raise ScaleError("Worker did not write its result file")
    results = summarize_trials(output, contract)
    write_figures(output, results)
    write_plan_discussion(output, results)
    assets = [*OUTPUT_FILES, *results["worker_paths"]]
    assets += [f"plans/{sample}_{workload}_{engine}.txt"
               for sample in SAMPLES for workload in WORKLOADS for engine in ENGINES]
    manifest = {"version": SCHEMA_FINGERPRINT, "timestamp_utc": utc_now(),
                "input_snapshot_sha256": sha256(output / "input_snapshot.json"),
                "environment_sha256": sha256(output / "environment.json"),
                "part2_run_id": contract["part2_run_id"],
                "part2_manifest_sha256": contract["part2_manifest_sha256"],
                "part2_validation_sha256": contract["part2_validation_sha256"],
                "source_rows": contract["part2_panel_rows"],
                "git_commit": repo_sha, "measured_trials": len([t for t in results["trials"] if not t["warmup"]]),
                "warmup_trials": len([t for t in results["trials"] if t["warmup"]]),
                "equivalence_passed": True,
                "assets": {name: sha256(output / name) for name in assets}}
    save_json(output / "part3_manifest.json", manifest)
    result = verify_result(source_acq, part2, output)
    save_json(output / "part3_validation.json", result)
    return result


def verify_result(source_acq: Path, part2: Path, output: Path) -> dict[str, Any]:
    """Read-only verification of all outputs and their original Part 2 source."""
    original = source_contract(source_acq, part2)
    snapshot = read_json(output / "input_snapshot.json")
    manifest = read_json(output / "part3_manifest.json")
    if (snapshot.get("part2_run_id") != original["part2_run_id"] or
        snapshot.get("part2_manifest_sha256") != original["part2_manifest_sha256"] or
        snapshot.get("part2_validation_sha256") != original["part2_validation_sha256"] or
        snapshot.get("part2_panel_rows") != original["part2_panel_rows"] or
        snapshot.get("partitions") != original["partitions"]):
        raise ScaleError("Part 3 snapshot differs from current Part 1/2 source")
    if (manifest.get("input_snapshot_sha256") != sha256(output / "input_snapshot.json") or
        manifest.get("environment_sha256") != sha256(output / "environment.json") or
        manifest.get("part2_run_id") != original["part2_run_id"] or
        manifest.get("part2_manifest_sha256") != original["part2_manifest_sha256"] or
        manifest.get("part2_validation_sha256") != original["part2_validation_sha256"] or
        manifest.get("source_rows") != original["part2_panel_rows"] or
        manifest.get("equivalence_passed") is not True or
        manifest.get("measured_trials") != 36 or manifest.get("warmup_trials") != 12):
        raise ScaleError("Part 3 manifest inconsistency")
    required = set(OUTPUT_FILES)
    required |= {f"workers/{s}_{w}_{e}.json" for s in SAMPLES for w in WORKLOADS for e in ENGINES}
    required |= {f"plans/{s}_{w}_{e}.txt" for s in SAMPLES for w in WORKLOADS for e in ENGINES}
    if set(manifest.get("assets", {})) != required:
        raise ScaleError("Missing or unexpected evidence file inventory")
    for filename, checksum in manifest["assets"].items():
        candidate = (output / filename).resolve()
        if output.resolve() not in candidate.parents or not candidate.is_file():
            raise ScaleError(f"Invalid evidence path: {filename}")
        if sha256(candidate) != checksum:
            raise ScaleError(f"Evidence checksum mismatch: {filename}")
    old = output / "part3_validation.json"
    if old.is_file():
        prior = read_json(old)
        if prior.get("success") is not True or prior.get("manifest_sha256") != sha256(output / "part3_manifest.json"):
            raise ScaleError("Existing Part 3 validation is stale")
    return {"success": True, "verified_at_utc": utc_now(),
            "part2_run_id": original["part2_run_id"],
            "source_rows": original["part2_panel_rows"],
            "measured_trials": 36, "warmup_trials": 12,
            "scenario_count": 12, "all_engine_results_match": True,
            "manifest_sha256": sha256(output / "part3_manifest.json"),
            "git_commit": manifest["git_commit"]}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="operation", required=True)
    worker = sub.add_parser("worker")
    worker.add_argument("--panel-dir", required=True, type=Path)
    worker.add_argument("--engine", required=True, choices=ENGINES)
    worker.add_argument("--workload", required=True, choices=WORKLOADS)
    worker.add_argument("--result-file", required=True, type=Path)
    run = sub.add_parser("run")
    run.add_argument("--acquisition-dir", required=True, type=Path)
    run.add_argument("--part2-dir", required=True, type=Path)
    run.add_argument("--output-dir", required=True, type=Path)
    run.add_argument("--local-dir", required=True, type=Path)
    run.add_argument("--git-commit", required=True)
    verify = sub.add_parser("verify")
    verify.add_argument("--acquisition-dir", required=True, type=Path)
    verify.add_argument("--part2-dir", required=True, type=Path)
    verify.add_argument("--output-dir", required=True, type=Path)
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    try:
        if args.operation == "worker":
            run_worker(args.panel_dir, args.engine, args.workload, args.result_file)
            return 0
        if args.operation == "run":
            result = benchmark(args.acquisition_dir, args.part2_dir, args.output_dir,
                               args.local_dir, args.git_commit)
        else:
            result = verify_result(args.acquisition_dir, args.part2_dir, args.output_dir)
        print(json.dumps(result, indent=2))
        return 0
    except (ScaleError, ProcessingError, OSError, ValueError, KeyError,
            subprocess.TimeoutExpired, ImportError) as exc:
        LOG.error("Part 3 failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
