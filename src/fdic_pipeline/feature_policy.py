"""Part 4 / Feature definitions, four-quarter outcomes, and temporal split policy.

All features are functions of the current/earlier Financials records. Outcomes
are constructed separately from retrospective Part 2 terminal-event evidence.
"""
from __future__ import annotations

import calendar
import math
from datetime import date, timedelta
from typing import Any, Mapping

LABEL_LAST_QUARTER = date(2025, 6, 30)
STUDY_END = date(2026, 6, 30)
DEV_END = date(2006, 12, 31)
TEST_START = date(2008, 3, 31)
TEST_END = date(2014, 12, 31)
AVAILABILITY_LAG_DAYS = 90
LABEL_NAME = "Y_FAIL_4Q"

# Certificate and report date are identifiers, never predictive columns.
# Reports about future failure/termination are not predictive columns.
FEATURE_NAMES = (
    "LOG_ASSET", "EQUITY_ASSETS_PCT", "DEPOSITS_ASSETS_PCT",
    "CASH_ASSETS_PCT", "SECURITIES_ASSETS_PCT", "LOANS_ASSETS_PCT",
    "BROKERED_DEPOSITS_PCT", "NONCURRENT_LOANS_PCT",
    "ROA_PCT", "ROE_PCT", "NIM_PCT", "TIER1_LEVERAGE_PCT",
    "TOTAL_RISK_CAPITAL_PCT", "ASSET_GROWTH_QOQ_PCT",
    "DEPOSIT_GROWTH_QOQ_PCT", "ASSET_GROWTH_YOY_PCT",
    "ROA_CHANGE_YOY_PPT", "PRIOR_REPORT_COUNT",
    "REPORT_GAP_QUARTERS", "DQ_EXTREME_ROE", "MISSING_FINANCIAL_COUNT",
)

# Dictionary metadata is part of the reproducible feature specification.
# Accounting magnitudes share USD-thousands units; ratios are percentage points.
FEATURE_DETAILS = {
    "LOG_ASSET": ("size", "log(1 + ASSET)", "Bank size and scale effects", "current"),
    "EQUITY_ASSETS_PCT": ("capital", "100 * EQ / ASSET", "Loss-absorbing equity relative to assets", "current"),
    "DEPOSITS_ASSETS_PCT": ("funding", "100 * DEP / ASSET", "Deposit funding concentration", "current"),
    "CASH_ASSETS_PCT": ("liquidity", "100 * CHBAL / ASSET", "Cash liquidity buffer", "current"),
    "SECURITIES_ASSETS_PCT": ("liquidity", "100 * SC / ASSET", "Securities asset allocation", "current"),
    "LOANS_ASSETS_PCT": ("credit_exposure", "100 * LNLSGR / ASSET", "Loan concentration", "current"),
    "BROKERED_DEPOSITS_PCT": ("funding", "100 * BRO / DEP", "Dependence on brokered deposits", "current"),
    "NONCURRENT_LOANS_PCT": ("asset_quality", "100 * NCLNLS / LNLSGR", "Noncurrent credit exposure relative to loans", "current"),
    "ROA_PCT": ("earnings", "ROA", "Reported return on assets", "current"),
    "ROE_PCT": ("earnings", "ROE", "Reported return on equity", "current"),
    "NIM_PCT": ("earnings", "NIMY", "Reported net interest margin", "current"),
    "TIER1_LEVERAGE_PCT": ("capital", "RBC1AAJ", "Reported Tier 1 leverage ratio", "current"),
    "TOTAL_RISK_CAPITAL_PCT": ("capital", "RBCRWAJ", "Reported total risk-based capital ratio", "current"),
    "ASSET_GROWTH_QOQ_PCT": ("growth", "100 * (ASSET(q)/ASSET(q-1)-1)", "Short-horizon asset expansion/contraction", "previous_exact_quarter"),
    "DEPOSIT_GROWTH_QOQ_PCT": ("growth", "100 * (DEP(q)/DEP(q-1)-1)", "Short-horizon deposit funding changes", "previous_exact_quarter"),
    "ASSET_GROWTH_YOY_PCT": ("growth", "100 * (ASSET(q)/ASSET(q-4)-1)", "Annual asset growth", "previous_exact_year"),
    "ROA_CHANGE_YOY_PPT": ("earnings_trend", "ROA(q) - ROA(q-4)", "Annual change in profitability", "previous_exact_year"),
    "PRIOR_REPORT_COUNT": ("report_history", "Part 2 PRIOR_REPORT_COUNT", "Observed reporting history and vintage", "past_only"),
    "REPORT_GAP_QUARTERS": ("report_history", "Part 2 REPORT_GAP_QUARTERS", "Recent discontinuity in financial reporting", "past_only"),
    "DQ_EXTREME_ROE": ("data_quality", "float(Part 2 DQ_EXTREME_ROE)", "Current-report ROE anomaly indicator", "current"),
    "MISSING_FINANCIAL_COUNT": ("data_quality", "Part 2 MISSING_FINANCIAL_COUNT", "Current-report financial information completeness", "current"),
}
if tuple(FEATURE_DETAILS) != FEATURE_NAMES:
    raise AssertionError("Feature dictionary ordering must match model column ordering")


BAD_TERMINAL_CLASSES = {
    "ambiguous_failure_events", "inconsistent_event_timing",
    "inconsistent_institution_end_date",
}
VALID_TERMINAL_CLASSES = BAD_TERMINAL_CLASSES | {
    "confirmed_failure", "documented_other_exit",
    "right_censored", "unresolved_discontinuation",
}


def quarter_ordinal(day: date) -> int:
    if day.month not in (3, 6, 9, 12) or day.day != calendar.monthrange(day.year, day.month)[1]:
        raise ValueError(f"Not a calendar quarter end: {day}")
    return day.year * 4 + (day.month // 3 - 1)


def add_quarters(day: date, count: int = 4) -> date:
    ord_ = quarter_ordinal(day) + count
    year, q = divmod(ord_, 4)
    month = (q + 1) * 3
    return date(year, month, calendar.monthrange(year, month)[1])


def split_for_quarter(day: date) -> str:
    quarter_ordinal(day)
    if day > LABEL_LAST_QUARTER:
        return "label_immature"
    if day <= DEV_END:
        return "development"
    if day < TEST_START:
        return "purge_gap"
    if day <= TEST_END:
        return "test_holdout"
    return "unused_post_test"


def safe_number(v: Any) -> float | None:
    if isinstance(v, bool) or v is None:
        return None
    try:
        n = float(v)
    except (TypeError, ValueError, OverflowError):
        return None
    return n if math.isfinite(n) else None


def safe_ratio(numer: Any, denom: Any, multiplier: float = 100.0) -> float | None:
    n, d = safe_number(numer), safe_number(denom)
    if n is None or d is None or d <= 0:
        return None
    result = multiplier * n / d
    return result if math.isfinite(result) else None


def safe_growth(current: Any, previous: Any) -> float | None:
    now, prior = safe_number(current), safe_number(previous)
    if now is None or prior is None or prior <= 0:
        return None
    val = 100 * (now / prior - 1)
    return val if math.isfinite(val) else None


def optional_delta(current: Any, previous: Any) -> float | None:
    now, prior = safe_number(current), safe_number(previous)
    return (now - prior) if now is not None and prior is not None else None


def build_features(row: Mapping[str, Any], preceding: Mapping[int, Mapping[str, Any]]) -> dict[str, float | None]:
    """Use only the current row and earlier quarters keyed by calendar ordinal."""
    day = row["REPDTE"]
    qidx = quarter_ordinal(day)
    prev1 = preceding.get(qidx - 1, {})
    prev4 = preceding.get(qidx - 4, {})
    asset = safe_number(row.get("ASSET"))
    out = {
        "LOG_ASSET": math.log1p(asset) if asset is not None and asset > 0 else None,
        "EQUITY_ASSETS_PCT": safe_ratio(row.get("EQ"), asset),
        "DEPOSITS_ASSETS_PCT": safe_ratio(row.get("DEP"), asset),
        "CASH_ASSETS_PCT": safe_ratio(row.get("CHBAL"), asset),
        "SECURITIES_ASSETS_PCT": safe_ratio(row.get("SC"), asset),
        "LOANS_ASSETS_PCT": safe_ratio(row.get("LNLSGR"), asset),
        "BROKERED_DEPOSITS_PCT": safe_ratio(row.get("BRO"), row.get("DEP")),
        "NONCURRENT_LOANS_PCT": safe_ratio(row.get("NCLNLS"), row.get("LNLSGR")),
        "ROA_PCT": safe_number(row.get("ROA")),
        "ROE_PCT": safe_number(row.get("ROE")),
        "NIM_PCT": safe_number(row.get("NIMY")),
        "TIER1_LEVERAGE_PCT": safe_number(row.get("RBC1AAJ")),
        "TOTAL_RISK_CAPITAL_PCT": safe_number(row.get("RBCRWAJ")),
        "ASSET_GROWTH_QOQ_PCT": safe_growth(asset, prev1.get("ASSET")),
        "DEPOSIT_GROWTH_QOQ_PCT": safe_growth(row.get("DEP"), prev1.get("DEP")),
        "ASSET_GROWTH_YOY_PCT": safe_growth(asset, prev4.get("ASSET")),
        "ROA_CHANGE_YOY_PPT": optional_delta(row.get("ROA"), prev4.get("ROA")),
        "PRIOR_REPORT_COUNT": safe_number(row.get("PRIOR_REPORT_COUNT")),
        "REPORT_GAP_QUARTERS": safe_number(row.get("REPORT_GAP_QUARTERS")),
        "DQ_EXTREME_ROE": float(bool(row.get("DQ_EXTREME_ROE"))),
        "MISSING_FINANCIAL_COUNT": safe_number(row.get("MISSING_FINANCIAL_COUNT")),
    }
    if tuple(out) != FEATURE_NAMES:
        raise AssertionError("Feature selection is inconsistent with the documented schema")
    return out


def label_for_report(day: date, event: Mapping[str, Any], lag_days: int = AVAILABILITY_LAG_DAYS) -> tuple[int | None, str]:
    """Four-quarter event label with conservative censoring and availability gate.

    Future events are admissible ONLY for constructing outcome/eligibility, never
    as predictor features. End date belongs to (q, q + 4 quarters], inclusive.
    """
    if day > LABEL_LAST_QUARTER:
        return None, "label_immature"
    horizon = add_quarters(day, 4)
    if horizon > STUDY_END:
        return None, "label_immature"
    cls = event.get("TERMINAL_CLASS")
    if cls not in VALID_TERMINAL_CLASSES:
        raise ValueError(f"Unknown terminal status: {cls!r}")
    if cls in BAD_TERMINAL_CLASSES:
        return None, "ambiguous_terminal_evidence"
    last = date.fromisoformat(event["LAST_REPDTE"])
    event_day = date.fromisoformat(event["EVENT_DATE"]) if event.get("EVENT_DATE") else None
    available = day + timedelta(days=lag_days)
    if cls == "confirmed_failure":
        if event_day is None or event_day > STUDY_END:
            raise ValueError("Confirmed failure has no eligible event date")
        if event_day <= day:
            return None, "post_failure_report"
        if event_day <= available:
            # Financial statement cannot predict an event before its assumed availability.
            return None, "failure_before_assumed_publication"
        return (1, "failure_within_4q") if event_day <= horizon else (0, "observed_no_failure_4q")
    if cls == "documented_other_exit":
        if event_day is None:
            raise ValueError("Documented exit lacks an end date")
        if event_day <= day:
            return None, "post_exit_report"
        if event_day <= available:
            return None, "exit_before_assumed_publication"
        if event_day <= horizon:
            # Mergers/other exits are NOT failure labels; conservatively censor.
            return None, "competing_nonfailure_exit"
        return 0, "observed_no_failure_4q"
    if cls == "right_censored":
        if last < horizon:
            return None, "horizon_not_observed"
        return 0, "observed_no_failure_4q"
    if cls == "unresolved_discontinuation":
        if last < horizon:
            return None, "unresolved_before_horizon"
        return 0, "observed_no_failure_4q"
    raise AssertionError("Unhandled terminal class")
