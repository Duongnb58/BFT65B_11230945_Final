"""Part 5 pure policy tests: chronological purge, metrics, cost decisions."""
from datetime import date
import math
import numpy as np
import pandas as pd
import pytest

from fdic_pipeline.feature_policy import FEATURE_NAMES
from fdic_pipeline.model_policy import (FOLDS, MODEL_NAMES, make_candidate,
                                        select_by_average_precision, validate_folds)
from fdic_pipeline.modelling_validation import (
    ModelError, metrics_for_ranking, confusion_cost,
    cost_minimizing_threshold, estimate_development_costs,
)


def test_frozen_purged_folds():
    validate_folds()
    assert len(FOLDS) == 3
    assert all(x.training_end < x.validation_start for x in FOLDS)
    assert FOLDS[-1].validation_end < date(2007, 1, 1)


@pytest.mark.parametrize("name", MODEL_NAMES)
def test_independent_pipeline_created_each_time(name):
    a, b = make_candidate(name), make_candidate(name)
    assert a is not b and a.named_steps["preprocessing"] is not b.named_steps["preprocessing"]
    assert "classifier" in a.named_steps


def test_ranking_metrics_imbalance_appropriate():
    result = metrics_for_ranking([0, 0, 0, 1], [0.1, 0.2, 0.3, 0.9])
    assert result["average_precision"] == pytest.approx(1)
    assert result["roc_auc"] == pytest.approx(1)
    assert result["top_5pct_recall"] == pytest.approx(1)
    assert result["prevalence"] == 0.25


def test_ranking_rejects_one_class():
    with pytest.raises(ModelError):
        metrics_for_ranking([0, 0], [0.2, 0.3])


def test_threshold_minimum_and_ties():
    y = np.array([0, 0, 1, 1])
    p = np.array([0.5, 0.5, 0.5, 0.7])
    result = cost_minimizing_threshold(y, p, fp_cost=2, fn_cost=10)
    assert result["threshold"] == 0.5
    assert result["tp"] == 2 and result["fp"] == 2
    assert result["estimated_cost_thousands_usd"] == 4


def test_threshold_chooses_no_alarm_if_all_fp_expensive():
    result = cost_minimizing_threshold([0, 0, 1], [0.9, 0.8, 0.1], 100, 1)
    assert result["threshold"] > 1
    assert result["alerts"] == 0


def test_threshold_groups_identical_scores():
    result = cost_minimizing_threshold([0, 1], [0.5, 0.5], 1, 10)
    assert result["threshold"] == 0.5
    assert result["tp"] == 1 and result["fp"] == 1


def test_threshold_rejects_invalid_costs():
    with pytest.raises(ModelError):
        cost_minimizing_threshold([0,1], [0.1,0.9], 0, 10)


def test_costs_use_only_development_failures_and_declared_proxy():
    dev = pd.DataFrame({"LOG_ASSET":np.log1p([1000, 2000, 3000]), "CERT":[1, 2, 3]})
    failures = [{"CERT":i, "FAILDATE":"1990-01-01", "COST":100+i} for i in range(30)]
    dev["CERT"] = [0, 1, 2]
    # Expand the valid bank population for the synthetic example.
    dev = pd.concat([dev, pd.DataFrame({"CERT":list(range(3,30)),
                                       "LOG_ASSET":[np.log1p(2000)]*27})], ignore_index=True)
    failures += [{"CERT":100, "FAILDATE":"2010-01-01", "COST":1e9},
                 {"CERT":101, "FAILDATE":"1989-01-01", "COST":None}]
    c = estimate_development_costs(dev, failures)
    assert c["missed_failure_cost_thousands_usd"] == pytest.approx(114.5)
    assert c["false_alarm_cost_thousands_usd"] == pytest.approx(2.0)
    assert c["usable_development_failure_cost_records"] == 30
    assert "assumption" in c["false_alarm_cost_kind"]


def test_select_model_mean_fold_ap_only():
    rows=[]
    for i, f in enumerate(FOLDS):
        rows.extend([{"fold":f.name,"model":"prior_baseline","average_precision":0.9},
                     {"fold":f.name,"model":"logistic_risk","average_precision":0.2+i*0.01},
                     {"fold":f.name,"model":"hist_gradient_boosting","average_precision":0.3}])
    assert select_by_average_precision(rows) == "hist_gradient_boosting"


def test_false_alert_count_from_bank_quarter_units():
    r = confusion_cost([0,1,0,1], [0.8,0.9,0.1,0.3], .5, 2., 10.)
    assert (r["tp"],r["fp"],r["tn"],r["fn"]) == (1,1,1,1)
    assert r["estimated_cost_thousands_usd"] == 12
    assert r["false_alarms_per_1000"] == 250
