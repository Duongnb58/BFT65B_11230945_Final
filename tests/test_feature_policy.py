"""Part 4 pure rules: the temporal leakage gate is enforced here."""
from datetime import date
import copy
import math

import pytest
from fdic_pipeline.feature_policy import (
    add_quarters, quarter_ordinal, split_for_quarter, safe_ratio, safe_growth,
    build_features, label_for_report, FEATURE_NAMES, FEATURE_DETAILS, LABEL_LAST_QUARTER,
)


def terminal(klass="confirmed_failure", last="2009-12-31", event="2008-12-31"):
    return {"TERMINAL_CLASS": klass, "LAST_REPDTE": last, "EVENT_DATE": event}


def test_four_quarter_horizon_boundary():
    assert add_quarters(date(2025, 6, 30)) == date(2026, 6, 30)
    assert add_quarters(date(2008, 3, 31)) == date(2009, 3, 31)
    assert LABEL_LAST_QUARTER == date(2025, 6, 30)


def test_reject_wrong_calendar_date():
    with pytest.raises(ValueError, match="quarter end"):
        quarter_ordinal(date(2025, 6, 29))


@pytest.mark.parametrize("date_,expected", [
    (date(1984, 3, 31), "development"), (date(2006, 12, 31), "development"),
    (date(2007, 3, 31), "purge_gap"), (date(2007, 12, 31), "purge_gap"),
    (date(2008, 3, 31), "test_holdout"), (date(2014, 12, 31), "test_holdout"),
    (date(2015, 3, 31), "unused_post_test"),
    (date(2025, 6, 30), "unused_post_test"),
    (date(2025, 9, 30), "label_immature"),
])
def test_fixed_temporal_split(date_, expected):
    assert split_for_quarter(date_) == expected


def test_failure_in_future_horizon_is_positive():
    y, status = label_for_report(date(2008, 3, 31), terminal())
    assert (y, status) == (1, "failure_within_4q")


def test_failure_on_horizon_boundary_is_positive():
    assert label_for_report(date(2008, 3, 31), terminal(event="2009-03-31"))[0] == 1


def test_failure_beyond_horizon_is_negative():
    assert label_for_report(date(2008, 3, 31), terminal(event="2009-06-30"))[0] == 0


def test_failure_on_report_date_is_not_future_failure():
    assert label_for_report(date(2008, 3, 31), terminal(event="2008-03-31"))[0] is None


def test_failure_before_assumed_publication_is_censored():
    value, status = label_for_report(date(2008, 3, 31), terminal(event="2008-04-15"))
    assert value is None and status == "failure_before_assumed_publication"


def test_failure_after_90day_assumption_may_be_positive():
    # 2008-03-31 + 90 days = 2008-06-29.
    assert label_for_report(date(2008, 3, 31), terminal(event="2008-06-30"))[0] == 1


def test_documented_other_exit_is_not_a_failure():
    val, reason = label_for_report(date(2008, 3, 31), terminal("documented_other_exit", event="2008-12-31"))
    assert val is None and reason == "competing_nonfailure_exit"


def test_documented_other_exit_later_than_horizon_is_zero():
    assert label_for_report(date(2008, 3, 31), terminal("documented_other_exit", event="2010-03-31"))[0] == 0


@pytest.mark.parametrize("kind", ["ambiguous_failure_events", "inconsistent_event_timing",
                                     "inconsistent_institution_end_date"])
def test_ambiguous_status_never_negative(kind):
    val, status = label_for_report(date(2008, 3, 31), terminal(kind))
    assert val is None and status == "ambiguous_terminal_evidence"


def test_right_censored_horizon_eligibility():
    assert label_for_report(date(2025, 6, 30),
                            terminal("right_censored", last="2026-06-30", event=""))[0] == 0
    assert label_for_report(date(2025, 9, 30),
                            terminal("right_censored", last="2026-06-30", event=""))[0] is None


def test_unresolved_exit_after_horizon_censored():
    value, status = label_for_report(date(2008, 3, 31),
                                    terminal("unresolved_discontinuation", last="2008-12-31", event=""))
    assert value is None and status == "unresolved_before_horizon"


def test_unresolved_exit_at_horizon_can_be_zero():
    assert label_for_report(date(2008, 3, 31),
                            terminal("unresolved_discontinuation", last="2009-03-31", event=""))[0] == 0


def test_ratio_zero_denominator_and_nonfinite_values():
    assert safe_ratio(3, 0) is None
    assert safe_ratio(None, 2) is None
    assert safe_ratio(math.inf, 2) is None
    assert safe_growth(10, 0) is None
    assert safe_growth(110, 100) == pytest.approx(10.0)


def test_features_read_only_current_prior_quarters():
    q = date(2000, 6, 30)
    current = {"REPDTE": q, "ASSET": 110.0, "DEP": 90.0,
               "EQ": 10.0, "CHBAL": 5.0, "LNLSGR": 80.0,
               "BRO": 9.0, "NCLNLS": 2.0,
               "ROA": 1.2, "PRIOR_REPORT_COUNT": 2,
               "DQ_EXTREME_ROE": False, "MISSING_FINANCIAL_COUNT": 4}
    ix = quarter_ordinal(q)
    history = {ix-1: {"ASSET": 100.0, "DEP": 75.0},
               ix-4: {"ASSET": 90.0, "ROA": 0.9},
               ix+1: {"ASSET": 10**30, "DEP": 1}}
    before = copy.deepcopy((current, history))
    out = build_features(current, history)
    assert tuple(out) == FEATURE_NAMES
    assert out["ASSET_GROWTH_QOQ_PCT"] == pytest.approx(10.0)
    assert out["DEPOSIT_GROWTH_QOQ_PCT"] == pytest.approx(20.0)
    assert out["ASSET_GROWTH_YOY_PCT"] == pytest.approx(100*(110/90-1))
    assert out["ROA_CHANGE_YOY_PPT"] == pytest.approx(.3)
    assert out["BROKERED_DEPOSITS_PCT"] == pytest.approx(10)
    assert (current, history) == before
    del history[ix+1]
    assert build_features(current, history) == out


def test_no_growth_from_nonconsecutive_report():
    q = date(2000, 6, 30)
    old = {quarter_ordinal(q)-2: {"ASSET": 100.0, "DEP": 50.0}}
    out = build_features({"REPDTE": q, "ASSET": 120.0, "DEP": 60.0}, old)
    assert out["ASSET_GROWTH_QOQ_PCT"] is None
    assert out["DEPOSIT_GROWTH_QOQ_PCT"] is None


def test_only_approved_model_features():
    assert "CERT" not in FEATURE_NAMES
    assert "FAILDATE" not in FEATURE_NAMES
    assert "ACTIVE" not in FEATURE_NAMES
    assert "EVENT_DATE" not in FEATURE_NAMES
    assert "TERMINAL_CLASS" not in FEATURE_NAMES
    assert len(set(FEATURE_NAMES)) == len(FEATURE_NAMES)


def test_every_feature_has_a_documented_formula_and_timing():
    assert tuple(FEATURE_DETAILS) == FEATURE_NAMES
    assert all(len(FEATURE_DETAILS[name]) == 4
               and all(isinstance(part, str) and part.strip()
                       for part in FEATURE_DETAILS[name]) for name in FEATURE_NAMES)
