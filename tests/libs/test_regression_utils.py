import pandas as pd
import numpy as np

from unittest.mock import MagicMock, patch

from numpy import zeros, shape

from libs.regression_utils import (
    data_prepare,
    post_processing,
    reg_adjust_parallel,
    reg_adjust_parallel_rm,
    reg_adjust_parallel_pyspark,
    reg_adjust_parallel_rm_pyspark,
    cpt_adj_r2,
    f_test,
    significance_pvalue,
    cor_test,
    parallel_cor_test,
)


def test_data_prepare():
    dt = pd.DataFrame()
    df = data_prepare(dt, [])
    assert df[0].columns.tolist() == [
        "regression_id",
        "adjusted_prediction",
        "adj_model",
        "adj_outlier",
        "p_value",
        "slope1",
        "slope2",
        "intercept",
    ]


def test_post_processing():
    results = pd.DataFrame(
        {
            "p_value": [0],
            "prediction": [0],
            "adj_outlier": [0],
            "adjusted_prediction": [0],
            "residual": [0],
            "adjusted": [0],
            "slope1": [0],
            "slope2": [0],
        }
    )
    alpha = 0
    df = post_processing(results, alpha)
    assert df.columns.tolist() == [
        "p_value",
        "prediction",
        "adj_outlier",
        "adjusted_prediction",
        "residual",
        "adjusted",
        "slope1",
        "slope2",
    ]


def test_reg_adjust_parallel():
    dt = pd.DataFrame(
        {
            "regression_id": [0],
            "prediction_x": [0],
            "prediction": [0],
            "adjusted_prediction": [0],
            "adj_model": [0],
            "adj_outlier": [0],
            "p_value": [0],
            "slope1": [0],
            "slope2": [0],
            "intercept": [0],
        }
    )
    df = reg_adjust_parallel(0, dt)
    assert df.columns.tolist() == [
        "regression_id",
        "prediction_x",
        "prediction",
        "adjusted_prediction",
        "adj_model",
        "adj_outlier",
        "p_value",
        "slope1",
        "slope2",
        "intercept",
    ]


def test_reg_adjust_parallel_rm():
    dt = pd.DataFrame(
        {
            "regression_id": [0],
            "prediction_x": [0],
            "prediction": [0],
            "adjusted_prediction": [0],
            "adj_model": [0],
            "adj_outlier": [0],
            "p_value": [0],
            "slope1": [0],
            "slope2": [0],
            "intercept": [0],
            "analysis_target_y": [0],
        }
    )
    df = reg_adjust_parallel_rm(0, dt)
    assert df.columns.tolist() == [
        "regression_id",
        "prediction_x",
        "prediction",
        "adjusted_prediction",
        "adj_model",
        "adj_outlier",
        "p_value",
        "slope1",
        "slope2",
        "intercept",
        "analysis_target_y",
    ]


def test_reg_adjust_parallel_pyspark():
    dt = pd.DataFrame(
        {
            "regression_id": [0],
            "prediction_x": [0],
            "prediction": [0],
            "adjusted_prediction": [0],
            "adj_model": [0],
            "adj_outlier": [0],
            "p_value": [0],
            "slope1": [0],
            "slope2": [0],
            "intercept": [0],
            "analysis_target_y": [0],
        }
    )
    df = reg_adjust_parallel_pyspark(dt)
    assert df.columns.tolist() == [
        "regression_id",
        "prediction_x",
        "prediction",
        "adjusted_prediction",
        "adj_model",
        "adj_outlier",
        "p_value",
        "slope1",
        "slope2",
        "intercept",
        "analysis_target_y",
        "residual",
        "adjusted",
    ]


def test_reg_adjust_parallel_rm_pyspark():
    dt = pd.DataFrame(
        {
            "regression_id": [0],
            "prediction_x": [0],
            "prediction": [0],
            "adjusted_prediction": [0],
            "adj_model": [0],
            "adj_outlier": [0],
            "p_value": [0],
            "slope1": [0],
            "slope2": [0],
            "intercept": [0],
            "analysis_target_y": [0],
        }
    )
    df = reg_adjust_parallel_rm_pyspark(dt)
    assert df.columns.tolist() == [
        "regression_id",
        "prediction_x",
        "prediction",
        "adjusted_prediction",
        "adj_model",
        "adj_outlier",
        "p_value",
        "slope1",
        "slope2",
        "intercept",
        "analysis_target_y",
        "residual",
        "adjusted",
    ]


def test_cpt_adj_r2():
    x = pd.DataFrame(
        {
            "regression_id": [0],
            "prediction_x": [0],
            "prediction": [0],
            "adjusted_prediction": [0],
            "adj_model": [0],
            "adj_outlier": [0],
            "p_value": [0],
            "slope1": [0],
            "slope2": [0],
            "intercept": [0],
            "analysis_target_y": [0],
        }
    )
    adj_r = cpt_adj_r2(0, x)
    assert adj_r == 1


def test_f_test():
    x = pd.DataFrame(
        {
            "regression_id": [0],
            "prediction_x": [0],
            "prediction": [0],
            "adjusted_prediction": [0],
            "adj_model": [0],
            "adj_outlier": [0],
            "p_value": [0],
            "slope1": [0],
            "slope2": [0],
            "intercept": [0],
            "analysis_target_y": [0],
        }
    )
    y = pd.DataFrame(
        {
            "regression_id": [0],
            "prediction_x": [0],
            "prediction": [0],
            "adjusted_prediction": [0],
            "adj_model": [0],
            "adj_outlier": [0],
            "p_value": [0],
            "slope1": [0],
            "slope2": [0],
            "intercept": [0],
            "analysis_target_y": [0],
        }
    )
    ft = MagicMock()
    f_test(x, y, ft)


@patch("libs.regression_utils.np.linalg.inv")
def test_significance_pvalue(mock_np):
    lm = MagicMock()
    p_values = significance_pvalue(np.ndarray([0]), np.ndarray([0]), lm)
    assert p_values == []


def test_cor_test():
    dt = pd.DataFrame(
        {
            "regression_id": [0, 0],
            "prediction_x": [0, 0],
            "prediction": [0, 0],
            "cor": [0, 0],
            "p_value": [0, 0],
        }
    )
    re = cor_test(0, dt)
    assert re.columns.tolist() == ["regression_id", "cor", "p_value"]
