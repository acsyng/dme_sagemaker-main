import pandas as pd
import numpy as np
import polars as pl
import pyspark.pandas as ps

from unittest.mock import MagicMock, patch

from libs.metric_utils import (
    create_empty_out,
    prepare_rating_metric_input,
    create_check_df,
    run_metrics,
)


@patch("libs.metric_utils.F")
def test_create_empty_out(mock_spark_functions):
    pvs_df = MagicMock()
    create_empty_out(pvs_df)


@patch("libs.metric_utils.F")
@patch("libs.metric_utils.pd")
@patch("libs.metric_utils.ps")
def test_prepare_rating_metric_input(mock_ps, mock_pd, mock_f):
    input_df = MagicMock()
    metric_input_cols = [
        "ap_data_sector",
        "analysis_year",
        "analysis_type",
        "source_id",
        "entry_id",
        "stage",
        "decision_group_rm",
        "material_type",
        "breakout_level",
        "breakout_level_value",
        "trial_id",
        "trait",
        "result_numeric_value",
        "result_diff",
        "metric",
        "cpifl",
        "chkfl",
        "distribution_type",
        "direction",
        "threshold_factor",
        "spread_factor",
        "mn_weight",
        "var_weight",
        "adv_mn_weight",
        "adv_var_weight",
    ]
    gr_cols2 = [
        "ap_data_sector",
        "analysis_year",
        "analysis_type",
        "source_id",
        "decision_group_rm",
        "stage",
        "material_type",
        "breakout_level",
        "breakout_level_value",
        "trait",
    ]
    prepare_rating_metric_input(input_df, metric_input_cols, gr_cols2)


@patch("libs.metric_utils.np")
@patch("libs.metric_utils.pd")
@patch("libs.metric_utils.ps")
def test_create_check_df(mock_ps, mock_pd, mock_np):
    checks_df = MagicMock()
    checks_df.shape = [1]
    create_check_df("late_phenohybrid1yr", checks_df)


@patch("libs.metric_utils.dme")
def test_run_metrics(mock_dme):
    df = MagicMock()
    mock_dme.trial_mean_tests.return_value = {
        "pctchk": 0,
        "statistic": 0,
        "sem": 0,
        "metric_value": 0,
        "metric_method": 0,
    }
    run_metrics(df)


class MockDataFrame:
    def __init__(self, df):
        self.df = df

    def __getitem__(self, *args, **kwargs):
        return self.df
