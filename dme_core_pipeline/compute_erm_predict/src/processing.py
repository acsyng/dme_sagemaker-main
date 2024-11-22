import json
import os

import numpy as np
import pandas as pd
import s3fs
from sklearn.linear_model import LinearRegression

from libs.config.config_vars import S3_BUCKET, S3_DATA_PREFIX
from libs.postgres.postgres_connection import PostgresConnection
from libs.snowflake.snowflake_connection import SnowflakeConnection
from libs.event_bridge.event import error_event, create_event
from libs.logger.cloudwatch_logger import CloudWatchLogger
from libs.processing.dme_sql_recipe import DmeSqlRecipe

from libs.dme_sql_queries import get_analysis_types, query_pvs_input

"""
Material-level RM data query
"""
MATERIAL_DATA = """
        SELECT
          ete.be_bid,
           CASE 
              WHEN mat_trt.number_value < 15
                  THEN 80 + mat_trt.number_value*5
              WHEN '{1}' LIKE 'SOY%' AND mat_trt.code = 'MRTYN'
                  THEN mat_trt.number_value - 30
              ELSE mat_trt.number_value
          END AS original_rm,
          CASE 
              WHEN mat_trt.number_value < 15
                  THEN 1
              ELSE 0
          END AS rescaled_flag,
          mat_trt.last_chg_date,
          mat_trt.code
        FROM (
            SELECT DISTINCT experiment_id 
            FROM  rv_ap_sector_experiment_config
            WHERE ap_data_sector_name = '{1}'
              AND analysis_year IN ({2})
              AND decision_group IN {3}
        ) asec
        INNER JOIN (
          SELECT DISTINCT
              experiment_id,
              material_id,
              be_bid
            FROM rv_trial_pheno_analytic_dataset
            WHERE ap_data_sector = '{1}'
        ) ete
          ON ete.experiment_id = asec.experiment_id
        INNER JOIN (
          SELECT 
              mt.trait_code as code,
              m.material_id,
              mt.last_chg_date,
              mt.material_number_value as number_value
            FROM rv_bb_material_trait_daas mt
            INNER JOIN rv_material m
              ON mt.material_guid = m.material_guid
            WHERE mt.material_number_value < 200
              AND LOWER(mt.trait_descr) LIKE '%maturity%' 
              AND LOWER(mt.trait_descr) NOT LIKE '%gene%'
              AND LOWER(mt.trait_descr) NOT LIKE '%weight%'
              AND (LOWER(mt.trait_descr) NOT LIKE '%days%' OR '{1}' LIKE 'CORN%NA')

          UNION ALL
          SELECT 
              vht.trait_code as code,
              m.material_id,
              vht.last_chg_date,
              vht.number_value as number_value
            FROM rv_bb_variety_hybrid_trait_daas vht
            INNER JOIN rv_material m
              ON vht.material_guid = m.material_guid
            WHERE vht.number_value < 200
              AND LOWER(vht.trait_descr) LIKE '%maturity%' 
              AND LOWER(vht.trait_descr) NOT LIKE '%gene%'
              AND LOWER(vht.trait_descr) NOT LIKE '%weight%'
              AND (LOWER(vht.trait_descr) NOT LIKE '%days%' OR '{1}' LIKE 'CORN%NA')

          UNION ALL
          SELECT 
              t.code as code,
              m.material_id,
              vhts.last_chg_date,
              vhts.number_value
            FROM rv_variety_hybrid_trait_sp vhts
          INNER JOIN (
            SELECT *
              FROM rv_trait_sp 
            WHERE LOWER(descr) LIKE '%maturity%' 
              AND  LOWER(descr) NOT LIKE '%gene%'
              AND  LOWER(descr) NOT LIKE '%weight%'
              AND (LOWER(descr) NOT LIKE '%days%' OR 'CORN_NA_SUMMER' LIKE 'CORN%NA%')
          ) t
          ON vhts.trait_guid = t.trait_guid
          INNER JOIN rv_variety_hybrid vh
          ON vhts.variety_hybrid_guid = vh.variety_hybrid_guid
          INNER JOIN rv_material m
          on vh.genetic_affiliation_guid = m.genetic_affiliation_guid
            WHERE vhts.number_value < 200
        ) mat_trt
        ON mat_trt.material_id = ete.material_id
        {0}
        """


def compute_entry_data(ap_data_sector, analysis_year, source_ids):
    """
    compute_entry_data()

    Retrieves material-level and variety-level maturities as recorded in SPIRIT
    and then selects the most frequent RM observed at the most
    recent timestamp in the dataset as the "true" rm for that entry.
    """

    if ap_data_sector == "CORN_BRAZIL_SUMMER":
        bounds_str = "WHERE mat_trt.number_value >= 120"
    else:
        bounds_str = "WHERE (mat_trt.number_value <15 OR mat_trt.number_value > 65)"

    with SnowflakeConnection() as sc:
        query_str = MATERIAL_DATA.format(
            bounds_str, ap_data_sector, analysis_year, source_ids
        )
        df = sc.get_data(
            query_str, package="pandas", schema_overrides=None, do_lower=True
        )

    df["erm_count"] = df.groupby(["be_bid", "last_chg_date", "original_rm"])[
        "original_rm"
    ].transform("count")

    df = (
        df.sort_values(by=["last_chg_date", "erm_count"], axis=0, ascending=False)
        .groupby(["be_bid"])
        .first()
        .reset_index()
    )

    return df


"""
Insert statement to record results into PostgreSQL db
"""
INSERT_RM_ESTIMATES = """INSERT INTO dme.rm_estimates(
    ap_data_sector, 
    entry_id,
    rm_estimate,
    analysis_year,
    rm_model,
    decision_group_name
)
SELECT ap_data_sector, 
    entry_id,
    rm_estimate,
    analysis_year,
    rm_model,
    decision_group_name
FROM public.{}
"""

"""
Delete statement to remove old regression results from PostgreSQL db
"""
DELETE_RM_ESTIMATES = """
DELETE FROM dme.rm_estimates
WHERE ap_data_sector = '{0}'
AND analysis_year IN ({1})
AND (decision_group_name IN {2} OR decision_group_name IS NULL)
AND rm_model IN ('regression', 'regression-by-dg')
"""

INSERT_RM_REPORT = """ INSERT INTO dme.dme_rm_report(
    ap_data_sector,
    analysis_year,
    analysis_type,
    pipeline_runid,
    decision_group,
    cmatf_count,
    cpifl_count,
    mat_count,
    cmatf_defined,
    cpifl_defined,
    x_min,
    x_max,
    y_min,
    y_max,
    erm_count,
    r2
)
SELECT ap_data_sector,
    analysis_year,
    analysis_type,
    '{0}' AS pipeline_runid,
    decision_group,
    cmatf_count,
    cpifl_count,
    mat_count,
    cmatf_defined,
    cpifl_defined,
    x_min,
    x_max,
    y_min,
    y_max,
    erm_count,
    r2
FROM public.{1}
"""

"""
Delete statement to remove old regression reports from PostgreSQL db
"""
DELETE_RM_REPORT = """
DELETE FROM dme.dme_rm_report
WHERE ap_data_sector = '{0}'
AND analysis_year IN ({1})
AND (decision_group IN {2} OR decision_group IS NULL)
"""


def apply_linreg(df, check_col):
    """
    apply_linreg()
    Performs linear regression on a decision group using the specified check_col
    as the source for check flags
    """
    x = np.array(
        df["prediction"].loc[(df["original_rm"].notna()) & (df[check_col] == 1)]
    ).reshape(-1, 1)
    y = np.array(
        df["original_rm"].loc[(df["original_rm"].notna()) & (df[check_col] == 1)]
    ).reshape(-1, 1)

    r = LinearRegression().fit(x, y)

    if (r.coef_[0] > 0) & (r.coef_[0] < 5):
        erm = r.predict(np.array(df["prediction"]).reshape(-1, 1))
        r2 = r.score(x, y)
        a = r.coef_[0]
    else:
        erm = None
        a = 0
        r2 = None

    return erm, a, r2


def predict_erm(df):
    """
    predict_erm()

    Performs linear regression on a decision group to calculate ERM. The following
    priority is used for check flags:
    1. CMATF with maturity data
    2. Internal CPIFL with maturity data
    3. All CPIFL with maturity data
    4. Any entries with maturity data

    A regression is valid if there are at least 2 checks and the slope of the line
    is between 0 and 5
    """
    df["cmatf_defined"] = (df.cmatf > 0) & (df.original_rm.notna())
    df["cpifl_internal"] = (
        (df["original_rm"].notna())
        & (df["cpifl"] == 1)
        & (
            (df["par1_be_bid"].notna())
            | (df["par2_be_bid"].notna())
            | (df["material_type"] != "entry")
        )
    ).astype(int)
    df["cpifl_defined"] = (df.cpifl > 0) & (df.original_rm.notna())
    df["mat_defined"] = (df["original_rm"].notna()).astype(int)

    a = 0
    df["erm"] = np.nan
    check_column_names = [
        "cmatf_defined",
        "cpifl_internal",
        "cpifl_defined",
        "mat_defined",
    ]
    for check_col in check_column_names:
        if (a == 0) & (df[check_col].sum() > 1):
            df["erm"], a, df["r2"] = apply_linreg(df, check_col)
            return df
        else:
            df["erm"] = None
            df["r2"] = np.nan

    return df


if __name__ == "__main__":
    with open(
        "/opt/ml/processing/input/input_variables/query_variables.json", "r"
    ) as f:
        data = json.load(f)
        ap_data_sector = data["ap_data_sector"]
        analysis_year = data["analysis_year"]
        analysis_run_group = data["analysis_run_group"]
        current_source_ids = data["source_ids"]
        pipeline_runid = data["target_pipeline_runid"]
        breakout_level = data["breakout_level"]
        analysis_type = get_analysis_types(analysis_run_group)
        data_path = f"s3a://{S3_BUCKET}/{S3_DATA_PREFIX}/archive/{pipeline_runid}/{analysis_run_group}/"
        logger = CloudWatchLogger.get_logger(pipeline_runid)
        try:
            logger.info(f"{analysis_run_group} compute_erm_predict start")
            logger.info(f"{analysis_run_group} compute_erm_predict - entry data query")
            entry_df = compute_entry_data(
                ap_data_sector, analysis_year, current_source_ids
            )
            logger.info(f"{analysis_run_group} compute_erm_predict - pvs data query")
            pvs_df = query_pvs_input(
                ap_data_sector,
                analysis_year,
                analysis_run_group,
                current_source_ids,
                "",
                True,
            )
            logger.info(f"{analysis_run_group} compute_erm_predict - import check df")
            checks_df = pd.read_parquet(f"{data_path}trial_checks.parquet")

            logger.info(
                f"{analysis_run_group} compute_erm_predict - Creat input dataset"
            )
            input_df = pvs_df.merge(
                checks_df.drop(
                    columns=[
                        "ap_data_sector",
                        "analysis_year",
                        "cperf",
                        "cagrf",
                        "cregf",
                        "crtnf",
                    ]
                ),
                left_on=["decision_group", "be_bid", "material_type"],
                right_on=["decision_group", "be_bid", "material_type"],
                how="left",
            )
            input_df = input_df.merge(
                entry_df[["be_bid", "original_rm"]], on=["be_bid"], how="left"
            ).fillna(value={"cpifl": 0, "cmatf": 0})

            if input_df.shape[0] > 2:
                logger.info(f"{analysis_run_group} compute_erm_predict - Calculate ERM")
                output_df = input_df.groupby(
                    "decision_group", group_keys=False, as_index=False
                ).apply(predict_erm)

                logger.info(
                    f"{analysis_run_group} compute_erm_predict - Create report table"
                )
                report_df = output_df
                report_df["cmatf_defined"] = (report_df.cmatf > 0) & (
                    report_df.original_rm.notna()
                )
                report_df["cpifl_defined"] = (report_df.cpifl > 0) & (
                    report_df.original_rm.notna()
                )
                report_df = (
                    report_df.groupby(
                        [
                            "ap_data_sector",
                            "analysis_type",
                            "analysis_year",
                            "decision_group",
                        ]
                    )
                    .agg(
                        cmatf_count=pd.NamedAgg(column="cmatf", aggfunc="sum"),
                        cpifl_count=pd.NamedAgg(column="cpifl", aggfunc="sum"),
                        mat_count=pd.NamedAgg(column="original_rm", aggfunc="count"),
                        cmatf_defined=pd.NamedAgg(
                            column="cmatf_defined", aggfunc="sum"
                        ),
                        cpifl_defined=pd.NamedAgg(
                            column="cpifl_defined", aggfunc="sum"
                        ),
                        x_min=pd.NamedAgg(column="prediction", aggfunc="min"),
                        x_max=pd.NamedAgg(column="prediction", aggfunc="max"),
                        y_min=pd.NamedAgg(column="original_rm", aggfunc="min"),
                        y_max=pd.NamedAgg(column="original_rm", aggfunc="max"),
                        erm_count=pd.NamedAgg(column="erm", aggfunc="count"),
                        r2=pd.NamedAgg(column="r2", aggfunc="mean"),
                    )
                    .reset_index()
                    .astype(
                        {
                            "cmatf_count": "int64",
                            "cpifl_count": "int64",
                            "x_min": "float64",
                            "x_max": "float64",
                            "y_min": "float64",
                            "y_max": "float64",
                            "r2": "float64",
                        }
                    )
                )

                output_df = output_df.rename(
                    columns={
                        "decision_group": "decision_group_name",
                        "erm": "rm_estimate",
                        "be_bid": "entry_id",
                    }
                )
                output_df["technology"] = ""
                output_df["rm_model"] = "regression-by-dg"
                output_df = output_df[
                    [
                        "ap_data_sector",
                        "analysis_year",
                        "decision_group_name",
                        "entry_id",
                        "technology",
                        "material_type",
                        "original_rm",
                        "cmatf",
                        "decision_group_rm",
                        "rm_estimate",
                        "rm_model",
                    ]
                ]

                if "SOY" in ap_data_sector:
                    output_df["rm_estimate"] = np.round(
                        (output_df["rm_estimate"] - 80) / 5, 2
                    )
                else:
                    output_df["rm_estimate"] = np.round(output_df["rm_estimate"], 0)

                output_df = output_df.loc[output_df.rm_estimate.notna(), :]

                pc = PostgresConnection()

                temp_table = pc.get_temp_table("rm_estimates")
                logger.info(
                    f"{analysis_run_group} compute_erm_predict - Write to dme.rm_estimates table"
                )
                pc.write_pandas_df(output_df, temp_table)
                delete_rm_statement = DELETE_RM_ESTIMATES.format(
                    ap_data_sector,
                    analysis_year,
                    (
                        "('"
                        + "', '".join(
                            output_df["decision_group_name"].drop_duplicates()
                        )
                        + "')"
                    ),
                )
                pc.execute_sql_batch(
                    [
                        delete_rm_statement,
                        INSERT_RM_ESTIMATES.format(temp_table),
                        f"DROP TABLE {temp_table}",
                    ]
                )

                temp_table2 = pc.get_temp_table("rm_report")
                logger.info(
                    f"{analysis_run_group} compute_erm_predict - Write to dme.rm_report table"
                )

                pc.write_pandas_df(report_df, temp_table2)
                delete_rm_statement = DELETE_RM_REPORT.format(
                    ap_data_sector,
                    analysis_year,
                    (
                        "('"
                        + "', '".join(report_df["decision_group"].drop_duplicates())
                        + "')"
                    ),
                )
                insert_rm_statement = INSERT_RM_REPORT.format(
                    pipeline_runid, temp_table2
                )
                drop_temp_statement = f"DROP TABLE {temp_table2}"
                pc.execute_sql_batch(
                    [delete_rm_statement, insert_rm_statement, drop_temp_statement]
                )

                logger.info(f"ERM Report DF shape:{report_df.shape}")

                logger.info(f"{analysis_run_group} compute_erm_predict end")

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
