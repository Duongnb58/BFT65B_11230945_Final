"""Part 2 / Automated FDIC data-quality audit and clean bank-quarter panel.

One chronological scan of the 170 verified Financials partitions produces:
(i) field and rule audit tables, (ii) a typed panel preserving observed source
values, (iii) a separate retrospective terminal-exit review, and (iv) a
hash-verified evidence package. No API requests or future-value imputation.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import math
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from .acquisition import (
    DEFAULT_END, ENDPOINT_FIELDS, FINANCIAL_FIELDS, NUMERIC_FIELDS,
    arrow_schema, financial_path, quarter_ends, sha256, table_path,
)
from .data_quality import (
    EXIT_TYPES, FLAG_NAMES, asset_quarantine_reason,
    assumed_available_from, classify_terminal_record, parse_source_date,
    preceding_quarter_gap, quality_flags, summarize_missing,
)
from .workflow import acquisition_contract

LOG = logging.getLogger("fdic_part2")
NUMERIC = tuple(f for f in FINANCIAL_FIELDS if f in NUMERIC_FIELDS["financials"])
OUTPUT_FIELDS = (
    "MISSING_FINANCIAL_COUNT", "PRIOR_REPORT_COUNT", "REPORT_GAP_QUARTERS",
    "ASSUMED_AVAILABLE_FROM", *FLAG_NAMES,
)
QUARTERS = quarter_ends(date(1984, 3, 31), DEFAULT_END)
MAX_EXAMPLES_PER_RULE = 20
AUDIT_FILES = (
    "quality_variables.csv", "quality_by_year.csv", "quality_rules.csv",
    "coverage_by_quarter.csv", "quarantined_rows.csv", "anomaly_examples.csv",
    "failure_linkage.csv", "terminal_exits.csv", "quality_report.md",
    "figures/field_missingness.png", "figures/quarterly_coverage.png",
    "figures/terminal_exits.png",
)


class PanelError(RuntimeError):
    """Part 2 source-integrity, data-rule or output-validation failure."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def write_json(path: Path, content: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(content, f, indent=2, sort_keys=True, ensure_ascii=False)
        f.write("\n")


def load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        value = json.load(f)
    if not isinstance(value, dict):
        raise PanelError(f"Expected JSON object: {path}")
    return value


def write_csv(path: Path, headers: tuple[str, ...], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(headers), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def panel_schema() -> Any:
    import pyarrow as pa
    source = arrow_schema("financials")
    extras = [
        pa.field("MISSING_FINANCIAL_COUNT", pa.int16(), nullable=False),
        pa.field("PRIOR_REPORT_COUNT", pa.int32(), nullable=False),
        pa.field("REPORT_GAP_QUARTERS", pa.int16(), nullable=True),
        pa.field("ASSUMED_AVAILABLE_FROM", pa.date32(), nullable=False),
    ]
    extras += [pa.field(name, pa.bool_(), nullable=False) for name in FLAG_NAMES]
    return pa.schema(list(source) + extras)


def validate_source(source: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    """Pass the Part 1 manifest/report contract before opening any parquet."""
    checked = acquisition_contract(source)
    manifest = load_json(source / "manifest.json")
    return checked, manifest


def audit_parquet(path: Path, endpoint: str, manifest_entry: dict[str, Any],
                  expected_quarter: str | None = None) -> Any:
    import pyarrow.parquet as pq
    if not path.is_file() or sha256(path) != manifest_entry.get("sha256"):
        raise PanelError(f"Source file checksum mismatch: {path}")
    pfile = pq.ParquetFile(path)
    if pfile.schema_arrow != arrow_schema(endpoint):
        raise PanelError(f"Source schema mismatch: {path}")
    if pfile.metadata.num_rows != manifest_entry.get("rows"):
        raise PanelError(f"Source row count mismatch: {path}")
    table = pfile.read()
    if table.num_rows != pfile.metadata.num_rows:
        raise PanelError(f"Unable to read all source rows: {path}")
    if expected_quarter is not None:
        dates = set(table.column("REPDTE").to_pylist())
        qdate = datetime.strptime(expected_quarter, "%Y%m%d").date()
        if dates != {qdate}:
            raise PanelError(f"Wrong REPDTE values in quarter {expected_quarter}: {dates}")
    return table


def load_reference_events(source: Path, manifest: dict[str, Any]) -> tuple[dict[int, list[date]],
    dict[int, date | None], list[dict[str, Any]], dict[str, int]]:
    """Link by CERT only; do not guess identities from bank names."""
    failures = audit_parquet(table_path(source, "failures"), "failures",
                             manifest["tables"]["failures"]).to_pylist()
    institutions = audit_parquet(table_path(source, "institutions"), "institutions",
                                 manifest["tables"]["institutions"]).to_pylist()
    dates: dict[int, list[date]] = defaultdict(list)
    linkage: list[dict[str, Any]] = []
    summary = Counter()
    for row in failures:
        cert = row["CERT"]
        failure_date = None
        status = "dated_event"
        try:
            failure_date = parse_source_date(row.get("FAILDATE"))
        except ValueError:
            status = "unparseable_failure_date"
        if failure_date is None and status == "dated_event":
            status = "missing_failure_date"
        if cert is None:
            summary["missing_failure_cert"] += 1
            status = "missing_certificate" if status == "dated_event" else f"missing_certificate_and_{status}"
        elif failure_date is not None:
            dates[int(cert)].append(failure_date)
        summary[status] += 1
        linkage.append({
            "CERT": cert, "FAILDATE": row.get("FAILDATE"),
            "PARSED_FAILDATE": failure_date.isoformat() if failure_date else "",
            "LINKAGE_STATUS": status,
        })
    end_dates: dict[int, date | None] = {}
    for row in institutions:
        cert = int(row["CERT"])
        raw = row.get("ENDEFYMD")
        try:
            end_dates[cert] = parse_source_date(raw)
        except ValueError:
            end_dates[cert] = None
            summary["unparseable_institution_end_date"] += 1
    summary["failure_rows"] = len(failures)
    summary["institution_rows"] = len(institutions)
    return dates, end_dates, linkage, dict(summary)


def build_visuals(output: Path, fields: list[dict[str, Any]],
                  coverage: list[dict[str, Any]], exit_counts: Counter) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    out = output / "figures"
    out.mkdir(parents=True, exist_ok=True)
    ranked = sorted(fields, key=lambda r: (-r["missing_pct"], r["field"]))[:15]
    fig, ax = plt.subplots(figsize=(9, 5.5))
    ax.barh([r["field"] for r in reversed(ranked)], [r["missing_pct"] for r in reversed(ranked)])
    ax.set_xlabel("Missing observations (% of Financials rows)")
    ax.set_title("Field missingness: 15 highest rates")
    fig.tight_layout()
    fig.savefig(out / "field_missingness.png", dpi=160)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(10, 4.2))
    ax.plot(list(range(len(coverage))), [r["source_rows"] for r in coverage], linewidth=1.5)
    ax.set_xticks(list(range(0, len(coverage), 20)))
    ax.set_xticklabels([coverage[j]["quarter"] for j in range(0, len(coverage), 20)], rotation=35)
    ax.set_ylabel("Financials records")
    ax.set_title("Quarterly reporting coverage")
    fig.tight_layout()
    fig.savefig(out / "quarterly_coverage.png", dpi=160)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(9, 5))
    labels = [k for k in EXIT_TYPES if exit_counts.get(k, 0)]
    ax.barh(list(reversed(labels)), [exit_counts[k] for k in reversed(labels)])
    ax.set_xlabel("Banks (distinct CERT)")
    ax.set_title("Retrospective terminal-record classification")
    fig.tight_layout()
    fig.savefig(out / "terminal_exits.png", dpi=160)
    plt.close(fig)


def write_methodology(output: Path, ctx: dict[str, Any]) -> None:
    counts = ctx["rule_counts"]
    rows = [
        "# Part 2 — Automated data-quality and bank-quarter panel audit", "",
        "## 1. Source and scope", "",
        f"- Financials scope: 1984Q1–2026Q2; {ctx['quarters']} quarters.",
        f"- Verified source records: {ctx['source_rows']:,}.",
        f"- Clean panel records: {ctx['panel_rows']:,}.",
        f"- Quarantined (missing/nonpositive assets): {ctx['quarantined_rows']:,}.",
        f"- Source manifest SHA-256: `{ctx['manifest_sha256']}`.",
        f"- Source verification SHA-256: `{ctx['verification_sha256']}`.", "",
        "## 2. Detection and treatment policy", "",
        "| Issue | Detection | Treatment |", "|---|---|---|",
        "| Null values | Null rate by field and year | Preserve nulls; no global imputation |",
        "| Missing or nonpositive total assets | ASSET null, NaN or <= 0 | Quarantine observation; retain keyed audit record |",
        "| Balance-sheet inconsistencies | Current-report deposits, loans and assets | Flag; retain reported values |",
        "| Extreme profitability ratios | Absolute ROA/ROE above 100 percentage points | Flag for review; no winsorization |",
        "| Duplicate bank–quarter keys | (CERT, REPDTE) uniqueness | Stop pipeline, do not select an arbitrary duplicate |",
        "| Gaps in reporting | Earlier report of the same CERT | Record historical gap; never forward-fill |",
        "| Failure records without CERT | Null CERT in Failures | Preserve event, count as unlinked, do not infer identity |",
        "| Non-failure cessation | Dated institution end, no linked in-window failure | Classify separately from confirmed failure; not automatically 'merger' |",
        "| Unexplained cessation | No dated failure or institutional exit | Leave unresolved; do not call a failure |",
        "| Incomplete terminal observation | Last report equals 2026Q2 | Right-censor rather than classify as exit |",
        "", "## 3. Point-in-time interpretation", "",
        "The clean panel contains observed Financials values and flags computed only from",
        "the current and earlier reports of the same CERT. It does not include current",
        "Institutions ACTIVE/CHANGEC1, future FAILDATE, retrospective exit status,",
        "post-observation balances, or failure labels. These are kept in separate",
        "audit files, not joined to predictor rows.", "",
        f"`ASSUMED_AVAILABLE_FROM = REPDTE + {ctx['availability_lag_days']} days` is",
        "an explicit planning assumption, **not a measured FDIC publication date**.",
        "Any Part 4 modelling design must enforce information availability,",
        "exclude observations whose information was unavailable at the prediction",
        "cutoff, and use only matured outcome windows (up to 2025Q2 per exam).",
        "The FDIC API provides a revisable contemporary extract rather than a",
        "frozen historical vintage. True point-in-time vintage reconstruction",
        "requires archived publication timestamps or snapshots not in Part 1.", "",
        "## 4. Automated rule counts", "",
        "| Rule | Flagged source records |", "|---|---:|",
    ]
    for key in ("invalid_assets", *FLAG_NAMES, "reporting_gaps"):
        rows.append(f"| {key} | {counts.get(key, 0):,} |")
    rows += ["", "## 5. Exit review", "",
             "The classification is retrospective and bank-level, based on the last",
             "Financials date, dated FDIC failures (CERT matches), and dated",
             "institution termination. A lack of a failure match is not itself",
             "evidence of a merger. Missing CERT events remain unlinked; multiple",
             "failure dates and contradictory chronology are explicitly flagged.", "",
             "| Classification | Banks |", "|---|---:|"]
    for key in EXIT_TYPES:
        rows.append(f"| {key} | {ctx['exit_counts'].get(key, 0):,} |")
    rows += ["", "## 6. Evidence files", "",
             "- `quality_variables.csv`: raw field-level missingness and extrema.",
             "- `quality_by_year.csv`: field-year missingness (detects time-varying availability).",
             "- `quality_rules.csv`, `anomaly_examples.csv`, `quarantined_rows.csv`: policy evidence.",
             "- `coverage_by_quarter.csv`: coverage, exclusions, and bank counts.",
             "- `failure_linkage.csv`, `terminal_exits.csv`: event linkage and exit classifications.",
             "- `panel/year=YYYY/quarter=Q/part-00000.parquet`: clean typed panel.",
             "- `part2_manifest.json`, `part2_validation.json`: source lineage and hashes.",
             "- `figures/`: descriptive quality and exit charts.", "",
             "## 7. Limitations", "",
             "- The nine selected Institutions fields do not supply a complete dated",
             "  merger or structural-change history; 'other exit' is not a merger label.",
             "- Extreme-value flags are descriptive diagnostics, not proof of errors.",
             f"- A {ctx['availability_lag_days']}-day availability proxy is not a verified disclosure schedule.",
             "- Source Financials were checked for completeness in Part 1; statistical",
             "  quality and historical-vintage correctness are distinct questions.",
             "", "Sources: FDIC BankFind Suite API (https://api.fdic.gov/banks/docs/);",
             "BFT65B Final Individual Report examination instructions.", "",
    ]
    (output / "quality_report.md").write_text("\n".join(rows), encoding="utf-8")


def build_panel(source: Path, output: Path, availability_lag_days: int = 90,
                git_commit: str = "not_recorded") -> dict[str, Any]:
    """Audit one verified FDIC acquisition and create a new, immutable run folder."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    source = source.resolve()
    output = output.resolve()
    if source == output or source in output.parents or output in source.parents:
        raise PanelError("Part 2 output must be outside the source acquisition directory")
    if output.exists():
        raise PanelError(f"Output already exists; choose a new run ID: {output}")
    assumed_available_from(date(2020, 3, 31), availability_lag_days)
    checked, manifest = validate_source(source)
    if checked["quarters"] != len(QUARTERS):
        raise PanelError("Source quarter count does not match examination scope")
    output.mkdir(parents=True)
    LOG.info("Source integrity contract passed: %d Financials records", checked["financial_rows"])
    failures, institution_ends, failure_linkage, reference_info = load_reference_events(source, manifest)
    source_certs: set[int] = set()
    status: dict[int, dict[str, Any]] = {}
    missing_by_field = Counter()
    nonnull_by_field = Counter()
    numeric_min: dict[str, float] = {}
    numeric_max: dict[str, float] = {}
    per_year_field = defaultdict(lambda: [0, 0])
    rules = Counter()
    example_count = Counter()
    examples: list[dict[str, Any]] = []
    quarantine: list[dict[str, Any]] = []
    coverage: list[dict[str, Any]] = []
    output_schema = panel_schema()
    partition_info: list[dict[str, Any]] = []
    total_source = 0
    total_clean = 0
    for idx, quarter in enumerate(QUARTERS, start=1):
        source_file = financial_path(source, quarter)
        source_table = audit_parquet(source_file, "financials",
                                     manifest["financials"][quarter], quarter)
        rows = source_table.to_pylist()
        qdate = datetime.strptime(quarter, "%Y%m%d").date()
        year = qdate.year
        clean: list[dict[str, Any]] = []
        quarter_certs: set[int] = set()
        n_excluded = 0
        for record in rows:
            cert = record["CERT"]
            if not isinstance(cert, int) or cert <= 0 or cert in quarter_certs:
                raise PanelError(f"Invalid/duplicate CERT {cert!r} in quarter {quarter}")
            quarter_certs.add(cert)
            source_certs.add(cert)
            hist = status.setdefault(cert, {"first": qdate, "last": None,
                                            "last_clean": None, "count": 0,
                                            "clean_count": 0, "gaps": 0})
            gap = preceding_quarter_gap(hist["last"], qdate)
            prior = hist["count"]
            hist["last"] = qdate
            hist["count"] += 1
            if gap:
                rules["reporting_gaps"] += 1
                hist["gaps"] += gap
            for field in FINANCIAL_FIELDS:
                val = record.get(field)
                per_year_field[(year, field)][0] += 1
                if val is None:
                    missing_by_field[field] += 1
                    per_year_field[(year, field)][1] += 1
                else:
                    nonnull_by_field[field] += 1
                    if field in NUMERIC:
                        if not isinstance(val, (int, float)) or not math.isfinite(float(val)):
                            raise PanelError(f"Nonfinite/non-numeric value {field} for {cert}/{quarter}")
                        numeric_min[field] = min(numeric_min.get(field, float(val)), float(val))
                        numeric_max[field] = max(numeric_max.get(field, float(val)), float(val))
            flags = quality_flags(record)
            for flag, triggered in flags.items():
                if triggered:
                    rules[flag] += 1
                    if example_count[flag] < MAX_EXAMPLES_PER_RULE:
                        examples.append({"rule": flag, "CERT": cert,
                                         "REPDTE": qdate.isoformat(),
                                         "ASSET": record.get("ASSET"),
                                         "DEP": record.get("DEP"),
                                         "LNLSGR": record.get("LNLSGR"),
                                         "NCLNLS": record.get("NCLNLS"),
                                         "ROA": record.get("ROA"), "ROE": record.get("ROE")})
                        example_count[flag] += 1
            reason = asset_quarantine_reason(record.get("ASSET"))
            if reason is not None:
                n_excluded += 1
                rules["invalid_assets"] += 1
                quarantine.append({"CERT": cert, "REPDTE": qdate.isoformat(),
                                   "ASSET": record.get("ASSET"), "reason": reason})
                continue
            record.update({
                "MISSING_FINANCIAL_COUNT": summarize_missing(record, NUMERIC),
                "PRIOR_REPORT_COUNT": prior,
                "REPORT_GAP_QUARTERS": gap,
                "ASSUMED_AVAILABLE_FROM": assumed_available_from(qdate, availability_lag_days),
                **flags,
            })
            clean.append(record)
            hist["last_clean"] = qdate
            hist["clean_count"] += 1
        dst = (output / "panel" / f"year={year}" /
               f"quarter={(qdate.month - 1) // 3 + 1}" / "part-00000.parquet")
        dst.parent.mkdir(parents=True, exist_ok=True)
        ptable = pa.Table.from_pylist(clean, schema=output_schema)
        pq.write_table(ptable, dst, compression="snappy", row_group_size=100_000)
        if pq.ParquetFile(dst).schema_arrow != output_schema:
            raise PanelError(f"Unexpected output Parquet schema: {dst}")
        partition_info.append({"quarter": quarter, "rows": len(clean),
                               "relative_path": str(dst.relative_to(output)),
                               "sha256": sha256(dst), "size_bytes": dst.stat().st_size})
        total_source += len(rows)
        total_clean += len(clean)
        coverage.append({"quarter": quarter, "source_rows": len(rows),
                         "clean_rows": len(clean), "quarantined_rows": n_excluded,
                         "distinct_certs": len(quarter_certs)})
        if idx % 10 == 0 or idx == len(QUARTERS):
            LOG.info("Verified/cleaned %d/%d quarters; %d retained rows",
                     idx, len(QUARTERS), total_clean)
    if total_source != checked["financial_rows"]:
        raise PanelError("Processed source count differs from verified acquisition")
    if len(source_certs) != checked["distinct_financial_certs"]:
        raise PanelError("Distinct CERT count differs from verified acquisition")
    if total_clean + len(quarantine) != total_source:
        raise PanelError("Clean + quarantined records do not equal source count")

    # 3. Audit failure linkage; retrospective events never join to panel rows.
    for row in failure_linkage:
        cert = row["CERT"]
        if cert is not None and row["LINKAGE_STATUS"] == "dated_event":
            if cert not in source_certs:
                row["LINKAGE_STATUS"] = "certificate_not_in_financials"
            elif parse_source_date(row["FAILDATE"]) > DEFAULT_END:
                row["LINKAGE_STATUS"] = "event_after_study_end"
            elif parse_source_date(row["FAILDATE"]) < date(1984, 3, 31):
                row["LINKAGE_STATUS"] = "event_before_study_start"
            else:
                row["LINKAGE_STATUS"] = "linked_in_study"

    terminal: list[dict[str, Any]] = []
    exit_counts: Counter = Counter()
    for cert, hist in sorted(status.items()):
        event_type, event_date, evidence = classify_terminal_record(
            last_report=hist["last"], study_end=DEFAULT_END,
            failure_dates=failures.get(cert, []),
            institution_end=institution_ends.get(cert),
        )
        exit_counts[event_type] += 1
        terminal.append({
            "CERT": cert,
            "FIRST_REPDTE": hist["first"].isoformat(),
            "LAST_REPDTE": hist["last"].isoformat(),
            "LAST_CLEAN_REPDTE": hist["last_clean"].isoformat() if hist["last_clean"] else "",
            "SOURCE_REPORTS": hist["count"], "CLEAN_REPORTS": hist["clean_count"],
            "MISSING_INTERIOR_QUARTERS": hist["gaps"],
            "TERMINAL_CLASS": event_type,
            "EVENT_DATE": event_date.isoformat() if event_date else "",
            "EVIDENCE": evidence,
        })

    # 4. Persist reproducible audit evidence.
    fields = [
        {"field": f, "source_rows": total_source,
         "missing_rows": missing_by_field[f],
         "missing_pct": round(100 * missing_by_field[f] / total_source, 6),
         "nonmissing_rows": nonnull_by_field[f],
         "observed_min": numeric_min.get(f, ""),
         "observed_max": numeric_max.get(f, "")}
        for f in FINANCIAL_FIELDS
    ]
    years = [
        {"year": y, "field": f, "source_rows": per_year_field[(y, f)][0],
         "missing_rows": per_year_field[(y, f)][1],
         "missing_pct": round(100 * per_year_field[(y, f)][1] /
                              per_year_field[(y, f)][0], 6)}
        for y in range(1984, 2027) for f in FINANCIAL_FIELDS
        if per_year_field[(y, f)][0]
    ]
    rule_rows = [{"rule": name, "flagged_source_rows": rules[name],
                  "flagged_pct": round(100 * rules[name] / total_source, 6)}
                 for name in ("invalid_assets", *FLAG_NAMES, "reporting_gaps")]
    write_csv(output / "quality_variables.csv",
              ("field", "source_rows", "missing_rows", "missing_pct", "nonmissing_rows",
               "observed_min", "observed_max"), fields)
    write_csv(output / "quality_by_year.csv",
              ("year", "field", "source_rows", "missing_rows", "missing_pct"), years)
    write_csv(output / "quality_rules.csv",
              ("rule", "flagged_source_rows", "flagged_pct"), rule_rows)
    write_csv(output / "coverage_by_quarter.csv",
              ("quarter", "source_rows", "clean_rows", "quarantined_rows", "distinct_certs"), coverage)
    write_csv(output / "quarantined_rows.csv",
              ("CERT", "REPDTE", "ASSET", "reason"), quarantine)
    write_csv(output / "anomaly_examples.csv",
              ("rule", "CERT", "REPDTE", "ASSET", "DEP", "LNLSGR", "NCLNLS", "ROA", "ROE"),
              examples)
    write_csv(output / "failure_linkage.csv",
              ("CERT", "FAILDATE", "PARSED_FAILDATE", "LINKAGE_STATUS"), failure_linkage)
    write_csv(output / "terminal_exits.csv",
              ("CERT", "FIRST_REPDTE", "LAST_REPDTE", "LAST_CLEAN_REPDTE",
               "SOURCE_REPORTS", "CLEAN_REPORTS", "MISSING_INTERIOR_QUARTERS",
               "TERMINAL_CLASS", "EVENT_DATE", "EVIDENCE"), terminal)
    build_visuals(output, fields, coverage, exit_counts)
    context = {
        "quarters": len(QUARTERS), "source_rows": total_source,
        "panel_rows": total_clean, "quarantined_rows": len(quarantine),
        "manifest_sha256": checked["manifest_sha256"],
        "verification_sha256": checked["verification_report_sha256"],
        "availability_lag_days": availability_lag_days,
        "rule_counts": dict(rules), "exit_counts": dict(exit_counts),
    }
    write_methodology(output, context)
    audit_hashes = {name: sha256(output / name) for name in AUDIT_FILES}
    manifest_out = {
        "version": 1, "created_utc": utc_now(),
        "source_dir": str(source), "source_manifest_sha256": checked["manifest_sha256"],
        "source_verification_sha256": checked["verification_report_sha256"],
        "source_rows": total_source, "source_distinct_certs": len(source_certs),
        "source_failures_rows": checked["failures_rows"],
        "source_failures_without_cert": checked["failures_missing_cert_rows"],
        "quarters": len(QUARTERS), "panel_rows": total_clean,
        "panel_distinct_certs": sum(1 for v in status.values() if v["clean_count"]),
        "quarantined_rows": len(quarantine), "clean_panel_schema": str(output_schema),
        "lag_assumption_days": availability_lag_days,
        "source_policy": "Preserve observed numeric values; exclude only nonpositive/missing ASSET",
        "source_vintage_note": "Contemporary FDIC extract; not proven historical publication vintage",
        "exit_classification_is_retrospective_only": True,
        "git_commit": git_commit,
        "rule_counts": dict(rules), "exit_counts": dict(exit_counts),
        "reference_audit": reference_info,
        "partitions": partition_info, "audit_hashes": audit_hashes,
    }
    write_json(output / "part2_manifest.json", manifest_out)
    verdict = verify_panel(source, output, write_verdict=False)
    write_json(output / "part2_validation.json", verdict)
    return verdict


def verify_panel(source: Path, output: Path, write_verdict: bool = False) -> dict[str, Any]:
    """Independent read-only verification (never reruns data processing)."""
    import pyarrow.parquet as pq
    source = source.resolve()
    output = output.resolve()
    checked, _ = validate_source(source)
    manifest = load_json(output / "part2_manifest.json")
    if (manifest.get("source_manifest_sha256") != checked["manifest_sha256"] or
        manifest.get("source_verification_sha256") != checked["verification_report_sha256"] or
        manifest.get("source_rows") != checked["financial_rows"] or
        manifest.get("quarters") != len(QUARTERS)):
        raise PanelError("Part 2 is not derived from the current verified Part 1 dataset")
    partitions = manifest.get("partitions", [])
    if len(partitions) != len(QUARTERS):
        raise PanelError("Clean panel is missing quarter partitions")
    total = 0
    observed = set()
    expected_schema = panel_schema()
    for entry in partitions:
        q = entry["quarter"]
        if q not in QUARTERS or q in observed:
            raise PanelError(f"Invalid or repeated panel quarter: {q}")
        observed.add(q)
        path = output / entry["relative_path"]
        if not path.is_file() or sha256(path) != entry["sha256"]:
            raise PanelError(f"Output panel hash mismatch: {path}")
        pf = pq.ParquetFile(path)
        if pf.metadata.num_rows != entry["rows"] or pf.schema_arrow != expected_schema:
            raise PanelError(f"Output panel schema/rows mismatch: {path}")
        total += entry["rows"]
    if observed != set(QUARTERS):
        raise PanelError("Unexpected panel quarter coverage")
    if total != manifest["panel_rows"] or total + manifest["quarantined_rows"] != checked["financial_rows"]:
        raise PanelError("Part 2 row accounting does not reconcile with Part 1")
    for name in AUDIT_FILES:
        path = output / name
        if not path.is_file() or sha256(path) != manifest.get("audit_hashes", {}).get(name):
            raise PanelError(f"Audit evidence hash mismatch: {name}")
    if sum(manifest.get("exit_counts", {}).values()) != checked["distinct_financial_certs"]:
        raise PanelError("Terminal-exit audit does not cover every source CERT")
    if manifest.get("exit_classification_is_retrospective_only") is not True:
        raise PanelError("Unverified temporal separation of exit data")
    existing = output / "part2_validation.json"
    if existing.is_file():
        old = load_json(existing)
        if old.get("success") is not True or old.get("manifest_sha256") != sha256(output / "part2_manifest.json"):
            raise PanelError("Previous Part 2 validation is stale or inconsistent")
    verdict = {
        "success": True, "verified_at_utc": utc_now(),
        "quarters": len(QUARTERS), "source_rows": checked["financial_rows"],
        "clean_rows": total, "quarantined_rows": manifest["quarantined_rows"],
        "bank_certificates": checked["distinct_financial_certs"],
        "source_manifest_sha256": checked["manifest_sha256"],
        "manifest_sha256": sha256(output / "part2_manifest.json"),
        "exit_counts": manifest["exit_counts"],
        "warnings": [
            "Retrospective exit classes are separate from as-of panel predictors",
            "Assumed availability lag is not actual FDIC publication metadata",
            "Nonfailure closure is not evidence of a merger",
        ],
    }
    if write_verdict:
        # Verification is read-only by design; no files are overwritten.
        LOG.info("Verified output (no files modified): %s", output)
    return verdict


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    run_p = sub.add_parser("run", help="Produce quality audit and clean panel once")
    run_p.add_argument("--source-dir", type=Path, required=True)
    run_p.add_argument("--output-dir", type=Path, required=True)
    run_p.add_argument("--availability-lag-days", type=int, default=90)
    run_p.add_argument("--git-commit", default="not_recorded")
    verify_p = sub.add_parser("verify", help="Verify source lineage and all output checksums")
    verify_p.add_argument("--source-dir", type=Path, required=True)
    verify_p.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    try:
        if args.cmd == "run":
            result = build_panel(args.source_dir, args.output_dir,
                                 args.availability_lag_days, args.git_commit)
        else:
            result = verify_panel(args.source_dir, args.output_dir, write_verdict=True)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0
    except (PanelError, OSError, ValueError, KeyError, ImportError) as exc:
        LOG.error("Part 2 verification failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
