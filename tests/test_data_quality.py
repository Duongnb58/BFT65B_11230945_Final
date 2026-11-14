"""Part 2 deterministic policy tests; no network and no PyArrow required."""
from datetime import date, datetime, timedelta
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from fdic_pipeline.data_quality import (
    EXIT_TYPES, FLAG_NAMES, asset_quarantine_reason, assumed_available_from,
    classify_terminal_record, parse_source_date, preceding_quarter_gap,
    quality_flags, quarter_ordinal, summarize_missing, valid_number,
)


@pytest.mark.parametrize("value,expected", [
    (1, True), (0.0, True), (-2.1, True), (float("nan"), False),
    (float("inf"), False), (None, False), (True, False), ("3", False),
])
def test_finite_numeric(value, expected):
    assert valid_number(value) is expected


@pytest.mark.parametrize("value,expected", [
    (None, "missing_or_nonfinite_assets"),
    (float("nan"), "missing_or_nonfinite_assets"),
    (0, "nonpositive_assets"), (-0.1, "nonpositive_assets"),
    (1, None), (1_000_000, None),
])
def test_asset_gate(value, expected):
    assert asset_quarantine_reason(value) == expected


@pytest.mark.parametrize("raw,expected", [
    ("20260630", date(2026, 6, 30)),
    ("2026-06-30", date(2026, 6, 30)),
    ("2026-06-30T00:00:00", date(2026, 6, 30)),
    ("2026-06-30 14:00:00", date(2026, 6, 30)),
    ("06/30/2026", date(2026, 6, 30)),
    ("30-Jun-2026", date(2026, 6, 30)),
    (date(2026, 6, 30), date(2026, 6, 30)),
    (datetime(2026, 6, 30, 9, 0), date(2026, 6, 30)),
    (None, None), ("", None), ("99999999", None),
])
def test_source_date_formats(raw, expected):
    assert parse_source_date(raw) == expected


@pytest.mark.parametrize("raw", ["monday", "20260230", True, "2026/99/99"])
def test_reject_unknown_date(raw):
    with pytest.raises(ValueError):
        parse_source_date(raw)


def test_quality_current_only():
    r = {"ASSET": 100.0, "DEP": 120.0, "LNLSGR": 105.0,
         "NCLNLS": 110.0, "ROA": -111.0, "ROE": 101.0}
    f = quality_flags(r)
    assert set(f) == set(FLAG_NAMES)
    assert f["DQ_DEPOSITS_GT_ASSETS"]
    assert f["DQ_GROSS_LOANS_GT_ASSETS"]
    assert f["DQ_NONCURRENT_GT_GROSS_LOANS"]
    assert f["DQ_EXTREME_ROA"] and f["DQ_EXTREME_ROE"]
    assert not f["DQ_NEGATIVE_DEPOSITS"]


def test_negative_financial_flags():
    f = quality_flags({"DEP": -1, "LNLSGR": -2, "NCLNLS": None,
                       "ASSET": 3, "ROA": None})
    assert f["DQ_NEGATIVE_DEPOSITS"] and f["DQ_NEGATIVE_GROSS_LOANS"]
    assert not f["DQ_NONCURRENT_GT_GROSS_LOANS"]


def test_nulls_not_imputed():
    assert not any(quality_flags({"ASSET": None, "DEP": None}).values())
    assert summarize_missing({"A": None, "B": 0, "C": -1}, ["A", "B", "C"]) == 1


@pytest.mark.parametrize("previous,current,expected", [
    (None, date(1984, 3, 31), None),
    (date(1984, 3, 31), date(1984, 6, 30), 0),
    (date(1984, 3, 31), date(1984, 12, 31), 2),
    (date(2025, 12, 31), date(2026, 6, 30), 1),
])
def test_historical_gaps(previous, current, expected):
    assert preceding_quarter_gap(previous, current) == expected


@pytest.mark.parametrize("previous,current", [
    (date(1984, 3, 31), date(1984, 3, 31)),
    (date(1984, 6, 30), date(1984, 3, 31)),
])
def test_reject_nonchronological_report(previous, current):
    with pytest.raises(ValueError):
        preceding_quarter_gap(previous, current)


def test_fixed_publication_assumption_not_actual_timestamp():
    q = date(2026, 6, 30)
    assert assumed_available_from(q, 90) == q + timedelta(days=90)
    with pytest.raises(ValueError):
        assumed_available_from(q, -1)
    with pytest.raises(ValueError):
        assumed_available_from(q, 367)


@pytest.mark.parametrize("last,failures,end,expected", [
    (date(2011, 6, 30), [date(2011, 7, 15)], None, "confirmed_failure"),
    (date(2010, 6, 30), [], date(2010, 7, 1), "documented_other_exit"),
    (date(2010, 6, 30), [], date(2010, 6, 30), "documented_other_exit"),
    (date(2026, 6, 30), [], date(2026, 6, 30), "documented_other_exit"),
    (date(2026, 6, 30), [], None, "right_censored"),
    (date(2010, 6, 30), [], None, "unresolved_discontinuation"),
    (date(2010, 6, 30), [date(2010, 4, 1)], None, "inconsistent_event_timing"),
    (date(2010, 6, 30), [date(2010, 7, 1), date(2010, 8, 1)], None,
     "ambiguous_failure_events"),
    (date(2010, 6, 30), [], date(2009, 6, 30), "inconsistent_institution_end_date"),
    (date(2010, 6, 30), [date(2026, 7, 1)], None, "unresolved_discontinuation"),
    (date(2026, 6, 30), [date(2026, 7, 1)], None, "right_censored"),
])
def test_retrospective_exit_classification(last, failures, end, expected):
    label, _, _ = classify_terminal_record(last, date(2026, 6, 30), failures, end)
    assert label == expected
    assert label in EXIT_TYPES


def test_nonfailure_is_not_merger_inference():
    label, _, rationale = classify_terminal_record(
        date(2008, 3, 31), date(2026, 6, 30), [], date(2008, 4, 1))
    assert label == "documented_other_exit"
    assert "merger" not in rationale.lower()


def test_source_future_event_not_asof_flag():
    """Terminal labels use future evidence, but financial flags never receive it."""
    current = {"ASSET": 200, "DEP": 100, "ROA": 0.8}
    before = quality_flags(current)
    classify_terminal_record(date(2008, 3, 31), date(2026, 6, 30),
                             [date(2008, 7, 1)], None)
    assert before == quality_flags(current)
    assert not any("FAIL" in k or "EXIT" in k for k in before)


def test_generated_methodology_is_specific(tmp_path):
    from fdic_pipeline.panel import write_methodology
    context = {
        "quarters": 170, "source_rows": 172, "panel_rows": 170,
        "quarantined_rows": 2, "manifest_sha256": "a" * 64,
        "verification_sha256": "b" * 64, "availability_lag_days": 120,
        "rule_counts": {"invalid_assets": 2, "reporting_gaps": 1},
        "exit_counts": {"confirmed_failure": 4, "right_censored": 10},
    }
    write_methodology(tmp_path, context)
    content = (tmp_path / "quality_report.md").read_text()
    assert "120 days" in content
    assert "120-day availability proxy" in content
    assert "170" in content
    assert "Retrospective" in content or "retrospective" in content
    assert "not a merger" in content.lower() or "not automatically" in content.lower()
