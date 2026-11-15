"""Mandatory Colab tests: real scikit-learn and Spark MLlib on a synthetic panel.

The FDIC test holdout is never accessed. Independent fixture uses synthetic values.
"""
from datetime import date
import numpy as np
import pandas as pd
import pytest

pa = pytest.importorskip("pyarrow")
sklearn = pytest.importorskip("sklearn")
from fdic_pipeline.feature_policy import FEATURE_NAMES, LABEL_NAME
from fdic_pipeline.model_policy import FOLDS, make_candidate
from fdic_pipeline.modelling_validation import metrics_for_ranking, spark_mllib_development_demo


def toy_development():
    quarters = pd.date_range("1984-03-31", "2006-12-31", freq="QE-DEC")
    rng = np.random.default_rng(11230945)
    rows = []
    for day in quarters:
        for cert in range(100, 125):
            x = {name:float(rng.normal(0,1)) for name in FEATURE_NAMES}
            x["LOG_ASSET"] = float(np.log1p(1000+cert*10))
            x["EQUITY_ASSETS_PCT"] = float(5 + cert%7)
            x["DEPOSITS_ASSETS_PCT"] = 70.
            x["LOANS_ASSETS_PCT"] = 55.
            x["ROA_PCT"] = float((cert%8)-3)
            label = int((cert + day.quarter) % 5 == 0)
            rows.append({"CERT":cert,"REPDTE":day.date(),**x,LABEL_NAME:label})
    return pd.DataFrame(rows)


def test_sk_run_with_fresh_fold_preprocessors():
    dev = toy_development()
    for f in FOLDS:
        train = dev.loc[dev["REPDTE"] <= f.training_end]
        val = dev.loc[(dev["REPDTE"] >= f.validation_start) & (dev["REPDTE"] <= f.validation_end)]
        model = make_candidate("logistic_risk")
        model.fit(train[list(FEATURE_NAMES)], train[LABEL_NAME])
        p = model.predict_proba(val[list(FEATURE_NAMES)])[:,1]
        assert len(p) == len(val)
        assert metrics_for_ranking(val[LABEL_NAME],p)["average_precision"] > 0


def test_spark_demo_real_jvm(tmp_path):
    pytest.importorskip("pyspark")
    result = spark_mllib_development_demo(toy_development(), tmp_path)
    assert result["engine"] == "Spark MLlib, local[2]"
    assert result["validation_metrics"]["observations"] > 0
    assert result["validation_metrics"]["average_precision"] > 0
    assert (tmp_path / "spark_mllib_demo.json").is_file()
