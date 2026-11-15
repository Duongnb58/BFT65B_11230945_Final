"""Part 5 / Frozen chronological folds, model specifications and decision policy.

No test-period data, test labels, or after-development FDIC events enter model
selection, preprocessing, calibration, or the alarm-cost assumptions.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from .feature_policy import DEV_END, TEST_START, TEST_END, LABEL_NAME, FEATURE_NAMES, add_quarters

SEED = 11230945
FALSE_ALARM_ASSET_RATE = 0.001  # Scenario: supervisory review costs 0.1% of median development assets.
MODEL_NAMES = ("prior_baseline", "logistic_risk", "hist_gradient_boosting")
CANDIDATE_MODELS = ("logistic_risk", "hist_gradient_boosting")
TOP_FRACTIONS = (0.01, 0.05)


@dataclass(frozen=True)
class Fold:
    name: str
    training_end: date
    validation_start: date
    validation_end: date


FOLDS = (
    Fold("fold_1", date(1989, 12, 31), date(1991, 3, 31), date(1993, 12, 31)),
    Fold("fold_2", date(1993, 12, 31), date(1995, 3, 31), date(1998, 12, 31)),
    Fold("fold_3", date(1998, 12, 31), date(2000, 3, 31), date(2003, 12, 31)),
)


def validate_folds() -> None:
    last_validation_end = None
    for fold in FOLDS:
        if fold.training_end >= fold.validation_start:
            raise ValueError("Chronological order violated")
        if add_quarters(fold.training_end, 4) >= fold.validation_start:
            raise ValueError("Training outcomes overlap validation reporting window")
        if fold.validation_end > DEV_END or fold.validation_start > fold.validation_end:
            raise ValueError("Validation leaves development")
        if last_validation_end is not None and fold.validation_start <= last_validation_end:
            raise ValueError("Overlapping validation windows")
        last_validation_end = fold.validation_end
    if add_quarters(DEV_END, 4) >= TEST_START or TEST_END < TEST_START:
        raise ValueError("Invalid final test purge")


def make_candidate(name: str) -> Any:
    """Fresh sklearn estimator. Each fitted pipeline owns its preprocessing."""
    from sklearn.dummy import DummyClassifier
    from sklearn.ensemble import HistGradientBoostingClassifier
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import Pipeline
    from .feature_pipeline import make_unfitted_preprocessor

    if name == "prior_baseline":
        return Pipeline([
            ("preprocessing", make_unfitted_preprocessor()),
            ("classifier", DummyClassifier(strategy="prior")),
        ])
    if name == "logistic_risk":
        return Pipeline([
            ("preprocessing", make_unfitted_preprocessor()),
            ("classifier", LogisticRegression(C=0.5, max_iter=250, solver="lbfgs", random_state=SEED)),
        ])
    if name == "hist_gradient_boosting":
        return Pipeline([
            ("preprocessing", make_unfitted_preprocessor()),
            ("classifier", HistGradientBoostingClassifier(
                learning_rate=0.07, max_iter=80, max_leaf_nodes=15,
                min_samples_leaf=120, l2_regularization=1.0,
                early_stopping=False, random_state=SEED)),
        ])
    raise ValueError(f"Unregistered model: {name}")


def select_by_average_precision(rows: list[dict]) -> str:
    """Equal-weight average of fold AP; baseline is not eligible for selection."""
    import math
    means = {}
    for name in CANDIDATE_MODELS:
        observed = [float(r["average_precision"]) for r in rows if r["model"] == name]
        if len(observed) != len(FOLDS) or any(not math.isfinite(x) for x in observed):
            raise ValueError(f"Incomplete validation for {name}")
        means[name] = sum(observed) / len(observed)
    return sorted(CANDIDATE_MODELS, key=lambda name: (-means[name], CANDIDATE_MODELS.index(name)))[0]


validate_folds()
