import argparse
import json
import numpy as np
import polars as pl
import s3fs

from libs.config.config_vars import CONFIG
from libs.dme_sql_queries import get_analysis_types
from libs.event_bridge.event import error_event, create_event
from libs.logger.cloudwatch_logger import CloudWatchLogger
from libs.postgres.postgres_connection import PostgresConnection


DME_OUTPUT_INSERT = """INSERT INTO dme.dme_output
(
    ap_data_sector,
    analysis_year,
    analysis_type,
    pipeline_runid,
    source_id,
    stage,
    decision_group_rm,
    material_type,
    breakout_level_1,
    breakout_level_1_value,
    breakout_level_2,
    breakout_level_2_value,
    breakout_level_3,
    breakout_level_3_value,
    breakout_level_4,
    breakout_level_4_value,
    entry_id,
    check_entry_id,
    metric,
    trait,
    metric_type,
    metric_method,
    cpifl,
    chkfl,
    check_chkfl,
    count,
    chk_count,
    dme_text,
    prediction,
    stddev,
    diff_prediction,
    diff_stddev,
    check_prediction,
    check_stddev,
    check_diff_prediction,
    check_diff_stddev,
    abs_mean_pctchk,
    abs_mean_prob,
    abs_var_prob,
    rel_mean_zscore,
    rel_mean_prob,
    rel_var_prob
)
SELECT
    ap_data_sector,
    CAST(analysis_year AS FLOAT) AS analysis_year,
    analysis_type,
    '{1}' AS pipeline_runid,
    decision_group AS source_id,
    0 AS stage,
    0 AS decision_group_rm,
    material_type,
    breakout_level AS breakout_level_1,
    breakout_level_value AS breakout_level_1_value,
    'na' AS breakout_level_2,
    'all' AS breakout_level_2_value,
    'na' AS breakout_level_3,
    'all' AS breakout_level_3_value,
    'na' AS breakout_level_4,
    'all' AS breakout_level_4_value,
    be_bid AS entry_id,
    check_be_bid AS check_entry_id,
    metric_name AS metric,
    trait,
    'head-to-head' AS metric_type,
    MAX(metric_method) AS metric_method,
    MAX(CAST( cpifl AS INT)) AS cpifl,
    MAX(CAST( chkfl AS INT)) AS chkfl,
    MAX(CAST( check_chkfl AS INT)) AS check_chkfl,
    CAST( GREATEST(0, LEAST(9999, MAX(count))) AS INT) AS count,
    CAST( GREATEST(0, LEAST(9999, MAX(check_count))) AS INT) AS chk_count,
    NULL AS dme_text,
    MAX(prediction) AS prediction,
    MAX(stddev) AS stddev,
    0 AS diff_prediction,
    0 AS diff_stddev,
    MAX(check_prediction) AS check_prediction,
    MAX(check_stddev) AS check_stddev,
    0 AS check_diff_prediction,
    0 AS check_diff_stddev,
    MAX(pctchk) AS abs_mean_pctchk,
    MAX(metric_value) AS abs_mean_prob,
    0 AS abs_var_prob,
    MAX(statistic) AS rel_mean_zscore,
    0 AS rel_mean_prob,
    0 AS rel_var_prob
FROM public.{0}
GROUP BY
    ap_data_sector,
    analysis_year,
    analysis_type,
    decision_group,
    material_type,
    breakout_level,
    breakout_level_value,
    be_bid,
    check_be_bid,
    metric_name,
    trait
"""

DME_OUTPUT_METRICS_INSERT = """INSERT INTO dme.dme_output_metrics
(
    ap_data_sector,
    source_id,
    decision_group_rm,
    market_seg,
    entry_id,
    pipeline_runid,
    performance_pct_chk,
    performance,
    stability,
    risk,
    advancement,
    analysis_type,
    source_year,
    analysis_year,
    model,
    cpifl,
    material_type
)
SELECT
    ap_data_sector,
    decision_group AS source_id,
    0 AS decision_group_rm,
    breakout_level_value AS market_seg,
    be_bid AS entry_id,
    '{1}',
    MAX(pctchk) as performance_pct_chk,
    MAX(performance),
    MAX(stability),
    MAX(risk),
    MAX(advancement),
    analysis_type,
    MAX(CAST(source_year AS FLOAT)) AS source_year,
    CAST(analysis_year AS FLOAT),
    '' AS model,
    MAX(CAST( cpifl AS INT)) AS cpifl,
    material_type
FROM public.{0}
GROUP BY
    ap_data_sector,
    decision_group,
    breakout_level_value,
    be_bid,
    analysis_type,
    analysis_year,
    material_type"""

TABLE_DELETE = """ DELETE FROM dme.{0}
WHERE ap_data_sector = '{1}'
AND analysis_year = {2}
AND analysis_type IN {3}
AND source_id IN {4}
"""


def delete_and_insert(
    delete_sql,
    insert_sql,
    table_name,
    df,
    ap_data_sector,
    analysis_year,
    analysis_type,
    current_source_ids,
    pipeline_runid,
    logger,
):
    """Drops existing data and then publishes new data to PGSQL table

    Deletes all data corresponding to an ap_data_sector - analysis_year - analysis_type,
    creates and populates a temporary table, inserts the contents of the temporary table
    into the permanent table, and then deletes the temporary table.

    Parameters:
    - upsert_sql: string
    - update_sql: string
    - table_name: string
    - df: spark df
    - ap_data_sector: string
    - analysis_year: string
    - analysis_type: string
    """
    pc = PostgresConnection()
    temp_table_name = pc.get_temp_table(table_name)
    logger.info(f"compute_metric_output2 populating table {temp_table_name}")
    pc.write_polars_df(df, temp_table_name)
    logger.info(f"compute_metric_output2 delete rows in table {table_name}")
    pc.run_sql(
        delete_sql.format(
            table_name, ap_data_sector, analysis_year, analysis_type, current_source_ids
        )
    )
    logger.info(f"compute_metric_output2 insert rows in table {table_name}")
    pc.run_sql(insert_sql.format(temp_table_name, pipeline_runid))
    logger.info(f"compute_metric_output2 drop table {temp_table_name}")
    pc.run_sql(f"DROP TABLE {temp_table_name}")


def compute_mti(df, group_cols, weight_col="weight", h2h_mode=False):
    base_group_cols = [
        "ap_data_sector",
        "analysis_year",
        "analysis_type",
        "decision_group",
        "material_type",
        "breakout_level",
        "breakout_level_value",
        "be_bid",
    ]

    sum_cols = ["count"]
    mean_cols = ["prediction", "stddev"]
    max_cols = ["cpifl", "chkfl"]

    if h2h_mode:
        sum_cols = sum_cols + ["check_count"]
        mean_cols = mean_cols + ["check_prediction", "check_stddev"]
        max_cols = max_cols + ["check_chkfl"]

    if weight_col == None:
        mean_cols = mean_cols + ["weight", "adv_weight", "pctchk", "statistic"]
        max_cols = max_cols + ["incl_pctchk", "adv_incl_pctchk"]
    elif weight_col == "weight":
        mean_cols = mean_cols + ["adv_weight"]
        max_cols = max_cols + ["adv_incl_pctchk"]
        incl_pctchk_col = "incl_pctchk"
    elif weight_col == "adv_weight":
        incl_pctchk_col = "adv_incl_pctchk"

    if weight_col == None:
        df = df.group_by(base_group_cols + group_cols).agg(
            [pl.col(f"{c}").sum() for c in sum_cols]
            + [pl.col(f"{c}").mean() for c in mean_cols]
            + [pl.col(f"{c}").max() for c in max_cols]
            + [np.exp(np.log(pl.col("metric_value")).mean())]
        )
    else:
        df = df.group_by(base_group_cols + group_cols).agg(
            [pl.col(f"{c}").sum() for c in sum_cols]
            + [pl.col(f"{c}").mean() for c in mean_cols]
            + [pl.col(f"{c}").max() for c in max_cols]
            + [
                pl.lit("aggregate").alias("metric_method"),
                (
                    (
                        pl.col("pctchk") * pl.col(weight_col) * pl.col(incl_pctchk_col)
                    ).sum()
                )
                / ((pl.col(weight_col) * pl.col(incl_pctchk_col)).sum()).alias(
                    "pctchk"
                ),
                ((pl.col("statistic") * pl.col(weight_col)).sum())
                / (pl.col(weight_col).sum()).alias("statistic"),
                (
                    np.exp(
                        ((np.log(pl.col("metric_value")) * pl.col(weight_col)).sum())
                        / (pl.col(weight_col).sum())
                    )
                ),
            ]
        )

    if h2h_mode:
        df = df.with_columns(pl.lit("aggregate").alias("trait"))

    if weight_col == "adv_weight":
        df = df.with_columns(pl.lit("advancement").alias("metric_name"))

    return df


def set_reduce_mem(pnum, tnum):
    """Determines if dataframes should be converted via reduce_memory_usage_pl()
    based on the size of the input dfs pvs_metric_df and trial_num_output_df

    Parameters:
    - pnum: int number of rows in pvs_metric_df
    - tnum: int number of rows in trial_num_output_df

    Returns:
    - reduce_mem: bool

    """

    if max(pnum, tnum) > 25000000:
        reduce_mem = True
    else:
        reduce_mem = False

    return reduce_mem


def get_s3_file_paths(s3, folder_name):
    """Returns list of files in an s3 folder"""
    file_list = [path for path in s3.ls(folder_name) if path.endswith(".parquet")]

    file_list = ["s3://" + s for s in file_list]

    return file_list


def test_cast_col(df, col):
    """Tests if a column would fit into a smaller data type, and if so,
    performs the conversion on that column

    Parameters:
    - df: polars df
    - col: column name in df

    Returns:
    - df: polars df with col cast into a smaller type

    """
    numeric_int_types = [pl.Int8, pl.Int16, pl.Int32, pl.Int64]
    numpy_int_types = [np.int8, np.int16, np.int32, np.int64]
    numeric_float_types = [pl.Float32, pl.Float64]

    col_type = df[col].dtype
    c_min = df[col].min()
    c_max = df[col].max()
    if (c_min is None) | (c_max is None):
        return df
    elif col_type in numeric_int_types:
        for pl_type, np_type in zip(numeric_int_types, numpy_int_types):
            if c_min > np.iinfo(np_type).min and c_max < np.iinfo(np_type).max:
                df = df.with_columns(df[col].cast(pl_type))
    elif col_type in numeric_float_types:
        if c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
            df = df.with_columns(df[col].cast(pl.Float32))
    elif col_type == pl.Utf8:
        df = df.with_columns(df[col].cast(pl.Categorical))

    return df


def reduce_memory_usage_pl(df, reduce_mem, logger):
    """Reduce memory usage by polars dataframe df by changing its data types
    if reduce_mem is true.
    """
    if reduce_mem & (df.shape[0] > 0):
        logger.info(
            f"Memory usage of dataframe is {round(df.estimated_size('mb'), 2)} MB"
        )

        for col in df.columns:
            df = test_cast_col(df, col)

        logger.info(
            f"Memory usage of dataframe became {round(df.estimated_size('mb'), 2)} MB"
        )

    return df


def compute_h2h_metrics(
    stacked_df,
    reduce_mem,
    h2h_metric_cols,
    ap_data_sector,
    analysis_year,
    analysis_type,
    current_source_ids,
    pipeline_runid,
    args,
    s3,
    logger,
    force_refresh,
):

    h2h_metric_df = compute_mti(
        stacked_df.filter(
            (pl.col("metric_name") != "h2h")
            & (pl.col("weight") > 0)
            & (pl.col("check_be_bid").is_not_null())
        ),
        group_cols=["check_be_bid", "metric_name"],
        weight_col="weight",
        h2h_mode=True,
    )

    logger.info(
        f"compute_metric_output2: h2h_metric_df1 count={h2h_metric_df.shape[0]}"
    )

    h2h_metric_df = reduce_memory_usage_pl(h2h_metric_df, reduce_mem, logger)

    h2h_adv_df = compute_mti(
        h2h_metric_df.filter(pl.col("adv_weight") > 0),
        group_cols=["check_be_bid"],
        weight_col="adv_weight",
        h2h_mode=True,
    )

    h2h_adv_df = reduce_memory_usage_pl(h2h_adv_df, reduce_mem, logger)

    if h2h_metric_df.shape[0] > 0:
        h2h_metric_df = pl.concat(
            [
                stacked_df.filter(
                    (pl.col("check_be_bid").is_not_null())
                    & (pl.col("check_be_bid") != "")
                    & (pl.col("check_prediction").is_not_null())
                ).select(h2h_metric_cols),
                h2h_metric_df.filter(
                    (pl.col("check_be_bid").is_not_null())
                    & (pl.col("check_be_bid") != "")
                    & (pl.col("check_prediction").is_not_null())
                ).select(h2h_metric_cols),
            ],
            how="diagonal_relaxed",
        )
    else:
        h2h_metric_df = stacked_df.filter(
            (pl.col("check_be_bid").is_not_null())
            & (pl.col("check_be_bid") != "")
            & (pl.col("check_prediction").is_not_null())
        ).select(h2h_metric_cols)

    if h2h_adv_df.shape[0] > 0:
        h2h_metric_df = pl.concat(
            [
                h2h_metric_df,
                h2h_adv_df.filter(
                    (pl.col("check_be_bid").is_not_null())
                    & (pl.col("check_be_bid") != "")
                    & (pl.col("check_prediction").is_not_null())
                ).select(h2h_metric_cols),
            ],
            how="diagonal_relaxed",
        )

    logger.info(f"compute_metric_output2: h2h_metric_df count={h2h_metric_df.shape[0]}")

    if not h2h_metric_df.is_empty():

        logger.info(f"metric_output2 save to S3: {args.s3_output_h2h_metric_output}")

        write_parquet(s3, h2h_metric_df, args.s3_output_h2h_metric_output)
        write_parquet(
            s3, h2h_metric_df, args.s3_dme_output_h2h, pipeline_runid, not force_refresh
        )

        logger.info("compute_metric_output2 delete and insert to db dme_output")
        delete_and_insert(
            TABLE_DELETE,
            DME_OUTPUT_INSERT,
            "dme_output",
            h2h_metric_df,
            ap_data_sector,
            analysis_year,
            analysis_type,
            current_source_ids,
            pipeline_runid,
            logger,
        )

        h2h_metric_df = h2h_metric_df.clear()
        h2h_adv_df = h2h_adv_df.clear()


def compute_old_agg_df(
    agg_metric_df,
    base_group_cols,
    ap_data_sector,
    analysis_year,
    analysis_type,
    current_source_ids,
    pipeline_runid,
    args,
    s3,
    logger,
):
    old_agg_df = agg_metric_df.select(
        base_group_cols + ["cpifl", "metric_name", "pctchk", "metric_value"]
    ).pivot(
        on="metric_name",
        index=base_group_cols + ["cpifl"],
        values=["pctchk", "metric_value"],
    )

    old_agg_df = old_agg_df.select(
        pl.selectors.all() - pl.selectors.numeric(),
        pl.col("analysis_year"),
        pl.col("analysis_year").alias("source_year"),
        pl.selectors.starts_with("pctchk_perf").alias("pctchk"),
        pl.selectors.starts_with("metric_value").name.map(
            lambda c: c.replace("metric_value_", "")
        ),
    )

    for c in ["pctchk", "performance", "stability", "risk", "advancement"]:
        if c not in old_agg_df.columns:
            old_agg_df = old_agg_df.with_columns(pl.lit(None).cast(pl.Float32).alias(c))

    logger.info(f"compute_metric_output2:old_agg_df count={old_agg_df.shape[0]}")
    logger.info(
        f"compute_metric_output2: old_agg_df decision_group_count:"
        f'{old_agg_df.select("decision_group").n_unique()}'
    )

    if not old_agg_df.is_empty():
        logger.info(f"metric_output2 save to S3: {args.s3_output_old_metric_output}")

        write_parquet(s3, old_agg_df, args.s3_output_old_metric_output)

        logger.info("compute_metric_output2 delete and insert to db dme_output_metrics")
        delete_and_insert(
            TABLE_DELETE,
            DME_OUTPUT_METRICS_INSERT,
            "dme_output_metrics",
            old_agg_df,
            ap_data_sector,
            analysis_year,
            analysis_type,
            current_source_ids,
            pipeline_runid,
            logger,
        )


def main():
    parser = argparse.ArgumentParser(description="app inputs and outputs")
    parser.add_argument(
        "--s3_input_trial_numeric_output",
        type=str,
        help="s3 input trial_numeric_output bucket",
        required=True,
    )
    parser.add_argument(
        "--s3_input_trial_text_output",
        type=str,
        help="s3 input trial_text_output bucket",
        required=True,
    )
    parser.add_argument(
        "--s3_input_pvs_output",
        type=str,
        help="s3 input pvs_output bucket",
        required=True,
    )
    parser.add_argument(
        "--s3_output_old_metric_output",
        type=str,
        help="s3 output old_metric_output bucket",
        required=True,
    )
    parser.add_argument(
        "--s3_output_metric_output",
        type=str,
        help="s3 output metric_output bucket",
        required=True,
    )
    parser.add_argument(
        "--s3_output_h2h_metric_output",
        type=str,
        help="s3 output h2h_metric_output bucket",
        required=True,
    )
    parser.add_argument(
        "--s3_output_rating_metric_output",
        type=str,
        help="s3 output rating_metric_output bucket",
        required=True,
    )
    parser.add_argument(
        "--s3_dme_output_agg",
        type=str,
        help="s3 output dme_output_agg",
        required=True,
    )
    parser.add_argument(
        "--s3_dme_output_h2h",
        type=str,
        help="s3 output dme_output_h2h",
        required=True,
    )

    args = parser.parse_args()
    with open(
        "/opt/ml/processing/input/input_variables/query_variables.json", "r"
    ) as f:
        data = json.load(f)
        ap_data_sector = data["ap_data_sector"]
        analysis_year = data["analysis_year"]
        analysis_run_group = data["analysis_run_group"]
        current_source_ids = data["source_ids"]
        pipeline_runid = data["target_pipeline_runid"]
        force_refresh = data["force_refresh"] == "True"
        logger = CloudWatchLogger.get_logger(pipeline_runid)
        try:
            logger.info(f"metric_output2 processing: {data}")

            ### Specify encryption for writing output to S3 using polars+pyarrow
            s3 = s3fs.S3FileSystem(
                s3_additional_kwargs={
                    "ServerSideEncryption": "aws:kms",
                    "SSEKMSKeyId": CONFIG["output_kms_key"],
                }
            )

            ### Read inputs
            pvs_metric_files = get_s3_file_paths(s3, args.s3_input_pvs_output)
            pvs_metric_df = pl.read_parquet(
                pvs_metric_files, glob=False, low_memory=True
            )

            logger.info(
                f"compute_metric_output2: pvs_metric_df count={pvs_metric_df.shape[0]}"
            )

            trial_num_files = get_s3_file_paths(s3, args.s3_input_trial_numeric_output)
            trial_num_output_df = pl.read_parquet(
                trial_num_files, glob=False, low_memory=True
            )

            logger.info(
                f"compute_metric_output2: trial_num_output_df count={trial_num_output_df.shape[0]}"
            )
            reduce_mem = set_reduce_mem(
                pvs_metric_df.shape[0], trial_num_output_df.shape[0]
            )

            pvs_metric_df = reduce_memory_usage_pl(pvs_metric_df, reduce_mem, logger)
            trial_num_output_df = reduce_memory_usage_pl(
                trial_num_output_df, reduce_mem, logger
            )

            trial_text_files = get_s3_file_paths(s3, args.s3_input_trial_text_output)
            trial_text_output_df = pl.read_parquet(trial_text_files, glob=False)

            logger.info(
                f"compute_metric_output2: text_metric_output_df count={trial_text_output_df.shape[0]}"
            )

            trial_text_output_df = reduce_memory_usage_pl(
                trial_text_output_df, reduce_mem, logger
            )

            # Compute outputs
            if (
                max(
                    trial_num_output_df.shape[0],
                    trial_text_output_df.shape[0],
                    pvs_metric_df.shape[0],
                )
                > 0
            ):
                analysis_type = get_analysis_types(analysis_run_group)
                base_group_cols = [
                    "ap_data_sector",
                    "analysis_year",
                    "analysis_type",
                    "decision_group",
                    "material_type",
                    "breakout_level",
                    "breakout_level_value",
                    "be_bid",
                ]

                bebid_data_cols = [
                    "count",
                    "prediction",
                    "stddev",
                    "cpifl",
                    "chkfl",
                ]

                check_bebid_data_cols = [
                    "check_be_bid",
                    "check_count",
                    "check_prediction",
                    "check_stddev",
                    "check_chkfl",
                ]

                base_metric_cols = [
                    "metric_name",
                    "pctchk",
                    "statistic",
                    "metric_value",
                    "metric_method",
                ]

                stacked_df_cols = (
                    base_group_cols
                    + [
                        "trait",
                        "weight",
                        "adv_weight",
                        "incl_pctchk",
                        "adv_incl_pctchk",
                    ]
                    + bebid_data_cols
                    + check_bebid_data_cols
                    + base_metric_cols
                )

                h2h_metric_cols = (
                    base_group_cols
                    + ["trait"]
                    + bebid_data_cols
                    + check_bebid_data_cols
                    + base_metric_cols
                )

                agg_metric_cols = base_group_cols + bebid_data_cols + base_metric_cols

                stacked_df = (
                    pl.concat(
                        [
                            pvs_metric_df,
                            trial_num_output_df,
                            trial_text_output_df,
                        ],
                        how="diagonal_relaxed",
                    )
                    .select(stacked_df_cols)
                    .shrink_to_fit()
                )

                logger.info(
                    f"compute_metric_output2: stacked_df count={stacked_df.shape[0]}"
                )

                stacked_df = reduce_memory_usage_pl(stacked_df, reduce_mem, logger)

                write_parquet(
                    s3,
                    stacked_df.filter(pl.col("check_be_bid").is_null()),
                    args.s3_output_rating_metric_output,
                )

                compute_h2h_metrics(
                    stacked_df,
                    reduce_mem,
                    h2h_metric_cols,
                    ap_data_sector,
                    analysis_year,
                    analysis_type,
                    current_source_ids,
                    pipeline_runid,
                    args,
                    s3,
                    logger,
                    force_refresh,
                )

                agg_trait_df = compute_mti(
                    stacked_df.filter(
                        (pl.col("metric_name") != "h2h")
                        & (pl.col("weight") > 0)
                        & (
                            (pl.col("check_chkfl") == 1)
                            | (pl.col("check_be_bid") == "")
                            | (pl.col("check_be_bid").is_null())
                        )
                    ),
                    group_cols=["trait", "metric_name"],
                    weight_col=None,
                    h2h_mode=False,
                )

                agg_metric_df = compute_mti(
                    agg_trait_df,
                    group_cols=["metric_name"],
                    weight_col="weight",
                    h2h_mode=False,
                )

                agg_adv_df = compute_mti(
                    agg_metric_df,
                    group_cols=[],
                    weight_col="adv_weight",
                    h2h_mode=False,
                )

                agg_metric_df = agg_metric_df.select(agg_metric_cols)

                agg_metric_df = reduce_memory_usage_pl(
                    agg_metric_df, reduce_mem, logger
                )

                if agg_adv_df.shape[0] > 0:
                    agg_adv_df = reduce_memory_usage_pl(agg_adv_df, reduce_mem, logger)

                    agg_metric_df = pl.concat(
                        [
                            agg_metric_df,
                            agg_adv_df.select(agg_metric_cols),
                        ],
                        how="diagonal_relaxed",
                    ).shrink_to_fit()

                logger.info(
                    f"compute_metric_output2: agg_metric_df count={agg_metric_df.shape[0]}"
                )

                agg_metric_df = agg_metric_df.with_columns(
                    pl.when(
                        (pl.col("pctchk").is_nan())
                        | (pl.col("pctchk").is_null())
                        | (pl.col("pctchk").is_infinite())
                    )
                    .then(pl.lit(None))
                    .when(pl.col("pctchk") < 0)
                    .then(pl.lit(0))
                    .when(pl.col("pctchk") >= 1000)
                    .then(pl.lit(1000))
                    .otherwise(pl.col("pctchk"))
                    .alias("pctchk"),
                    pl.when(
                        (pl.col("statistic").is_nan())
                        | (pl.col("statistic").is_null())
                        | (pl.col("statistic").is_infinite())
                    )
                    .then(pl.lit(None))
                    .when(pl.col("statistic") <= -99)
                    .then(pl.lit(-99))
                    .when(pl.col("statistic") >= 99)
                    .then(pl.lit(99))
                    .otherwise(pl.col("statistic"))
                    .alias("statistic"),
                    pl.when(
                        (pl.col("metric_value").is_nan())
                        | (pl.col("metric_value").is_null())
                        | (pl.col("metric_value").is_infinite())
                    )
                    .then(pl.lit(None))
                    .when(pl.col("metric_value") < 1)
                    .then(pl.lit(0))
                    .when(pl.col("metric_value") > 99)
                    .then(pl.lit(99))
                    .otherwise(pl.col("metric_value"))
                    .alias("metric_value"),
                )

                logger.info(
                    f"compute_metric_output2: agg_metric_df count={agg_metric_df.shape[0]}"
                )

                logger.info(
                    f"metric_output2 save to S3: {args.s3_output_metric_output}"
                )

                write_parquet(s3, agg_metric_df, args.s3_output_metric_output)
                write_parquet(
                    s3,
                    agg_metric_df,
                    args.s3_dme_output_agg,
                    pipeline_runid,
                    not force_refresh,
                )

                compute_old_agg_df(
                    agg_metric_df,
                    base_group_cols,
                    ap_data_sector,
                    analysis_year,
                    analysis_type,
                    current_source_ids,
                    pipeline_runid,
                    args,
                    s3,
                    logger,
                )

            else:
                logger.info("compute_metric_output2: no output generated")

            create_event(
                CONFIG.get("event_bus"),
                ap_data_sector,
                analysis_year,
                pipeline_runid,
                analysis_run_group,
                None,
                "END",
                f"Finish Sagemaker DME pipeline: {pipeline_runid}",
                data["breakout_level"],
            )

        except Exception as e:
            logger.error(e)
            error_event(
                ap_data_sector,
                analysis_year,
                pipeline_runid,
                str(e),
                analysis_run_group,
            )
            raise e


def write_parquet(s3, df, path, pipeline_runid=None, is_merge=False):
    pyarrow_options = {"filesystem": s3}
    if pipeline_runid is not None:
        df = df.with_columns([pl.lit(pipeline_runid).alias("pipeline_runid")])
        path += ".parquet"
    else:
        pyarrow_options["partition_cols"] = ["decision_group"]

    if is_merge and s3.exists(path):
        existing_df = pl.read_parquet(path)
        df = pl.concat(
            [
                existing_df.filter(
                    ~pl.col("decision_group").is_in(df["decision_group"])
                ),
                df,
            ],
            how="diagonal_relaxed",
        )

    df.write_parquet(
        path,
        compression="snappy",
        use_pyarrow=True,
        pyarrow_options=pyarrow_options,
    )


if __name__ == "__main__":
    main()
