"""Part 2 / Data-quality rules and historically defensible exit classification.

This module contains the deterministic, dependency-free policy functions. The
large-table scan and Parquet I/O are implemented separately in panel.py.
"""
from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from typing import Any, Iterable, Mapping

# 1. Scope and documented screening rules
# Financial amounts are USD thousands; ratio values are percentages.
# These thresholds are screening flags, not automatic financial corrections.
FLAG_NAMES = (
    "DQ_NEGATIVE_DEPOSITS", "DQ_DEPOSITS_GT_ASSETS",
    "DQ_NEGATIVE_GROSS_LOANS", "DQ_GROSS_LOANS_GT_ASSETS",
    "DQ_NONCURRENT_GT_GROSS_LOANS", "DQ_EXTREME_ROA", "DQ_EXTREME_ROE",
)
EXIT_TYPES = (
    "confirmed_failure", "documented_other_exit", "right_censored",
    "unresolved_discontinuation", "inconsistent_event_timing",
    "ambiguous_failure_events", "inconsistent_institution_end_date",
)


def valid_number(value: Any) -> bool:
    """True for finite numeric observations; zero is a valid number here."""
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(float(value)))


def asset_quarantine_reason(asset: Any) -> str | None:
    """Only invalid core exposure values prevent panel inclusion."""
    if not valid_number(asset):
        return "missing_or_nonfinite_assets"
    if float(asset) <= 0.0:
        return "nonpositive_assets"
    return None


def quality_flags(record: Mapping[str, Any]) -> dict[str, bool]:
    """Current-quarter rules: no full-sample means or future records are used."""
    asset = record.get("ASSET")
    dep = record.get("DEP")
    loans = record.get("LNLSGR")
    ncl = record.get("NCLNLS")
    roa = record.get("ROA")
    roe = record.get("ROE")
    return {
        "DQ_NEGATIVE_DEPOSITS": valid_number(dep) and dep < 0,
        "DQ_DEPOSITS_GT_ASSETS": (valid_number(dep) and valid_number(asset)
                                   and asset > 0 and dep > asset),
        "DQ_NEGATIVE_GROSS_LOANS": valid_number(loans) and loans < 0,
        "DQ_GROSS_LOANS_GT_ASSETS": (valid_number(loans) and valid_number(asset)
                                     and asset > 0 and loans > asset),
        "DQ_NONCURRENT_GT_GROSS_LOANS": (valid_number(ncl) and valid_number(loans)
                                          and loans >= 0 and ncl > loans),
        "DQ_EXTREME_ROA": valid_number(roa) and abs(roa) > 100,
        "DQ_EXTREME_ROE": valid_number(roe) and abs(roe) > 100,
    }


def parse_source_date(value: Any) -> date | None:
    """Interpret common FDIC date encodings; reject nonempty unknown encodings.

    Date parse failures are audited rather than replaced with an invented date.
    """
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, bool):
        raise ValueError(f"Invalid calendar date: {value!r}")
    text = str(value).strip()
    if text in {"0", "00000000", "99999999", "9999-12-31"}:
        return None
    if "T" in text or (" " in text and text[0:4].isdigit()):
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
        except ValueError:
            pass
    patterns = (
        "%Y%m%d", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S", "%m/%d/%Y", "%m-%d-%Y", "%d-%b-%Y",
    )
    for pattern in patterns:
        try:
            return datetime.strptime(text, pattern).date()
        except ValueError:
            pass
    raise ValueError(f"Unrecognized FDIC date: {value!r}")


def quarter_ordinal(d: date) -> int:
    return d.year * 4 + (d.month - 1) // 3


def preceding_quarter_gap(previous: date | None, current: date) -> int | None:
    if previous is None:
        return None
    delta = quarter_ordinal(current) - quarter_ordinal(previous)
    if delta <= 0:
        raise ValueError("Bank reporting dates must be strictly increasing")
    return delta - 1


def assumed_available_from(report_date: date, lag_days: int = 90) -> date:
    """Conservative *assumption*, not an observed FDIC publication timestamp."""
    if lag_days < 0 or lag_days > 366:
        raise ValueError("Publication-lag assumption must be within [0, 366] days")
    return report_date + timedelta(days=lag_days)


def classify_terminal_record(
    last_report: date,
    study_end: date,
    failure_dates: Iterable[date],
    institution_end: date | None,
) -> tuple[str, date | None, str]:
    """Retrospective terminal classification, NEVER joined to predictor rows.

    A non-failure termination is not assumed to be a merger. Missing outcome
    evidence remains explicitly unresolved. Events after study_end do not
    establish an in-sample failure.
    """
    events = sorted(set(d for d in failure_dates if d is not None and d <= study_end))
    if len(events) > 1:
        return ("ambiguous_failure_events", None,
                "Multiple distinct failure dates for one CERT")
    if events and events[0] < last_report:
        return ("inconsistent_event_timing", events[0],
                "Failure precedes the last financial report")
    if events and events[0] >= last_report:
        return ("confirmed_failure", events[0],
                "CERT-linked failure date within the study window")
    if institution_end is not None and institution_end < last_report:
        return ("inconsistent_institution_end_date", institution_end,
                "Institution end date precedes final financial report")
    if institution_end is not None and last_report <= institution_end <= study_end:
        return ("documented_other_exit", institution_end,
                "Institution end date; no CERT-linked in-window failure")
    if last_report >= study_end:
        return ("right_censored", None, "Observed at the final quarter")
    return ("unresolved_discontinuation", None,
            "Reporting stops without dated in-window exit evidence")


def summarize_missing(record: Mapping[str, Any], financial_numeric_fields: Iterable[str]) -> int:
    return sum(record.get(name) is None for name in financial_numeric_fields)
