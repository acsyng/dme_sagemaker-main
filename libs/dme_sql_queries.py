import pandas as pd
import polars as pl
from libs.snowflake.snowflake_connection import SnowflakeConnection
from libs.postgres.postgres_connection import PostgresConnection
from libs.logger.cloudwatch_logger import CloudWatchLogger


def get_analysis_types(analysis_run_group: str) -> str:
    """Translates analysis_run_group into analysis_types.

    Parameters:
    - analysis_run_group: str that is one of ['late_phenohybrid1yr',
        'late_phenogca', 'late_mynoet', 'genomic_prediction', 'mynoet',
        'phenogca', 'phenohybrid1yr']

    Returns:
    - analysis_type: str that is one or more of ['SingleExp', 'MultiExp',
    'MynoET', 'GenoPred', 'PhenoGCA']
    """

    if "mynoet" in analysis_run_group.lower():
        analysis_type = "('MynoET')"
    elif "phenogca" in analysis_run_group.lower():
        analysis_type = "('PhenoGCA')"
    elif analysis_run_group.lower() == "genomic_prediction":
        analysis_type = "('GenoPred')"
    else:
        analysis_type = "('SingleExp', 'MultiExp')"

    return analysis_type


def get_breakout_str(breakout_level: str) -> str:
    """Formats breakout level parameter

    Transforms the breakout level parameter into a string to be used in SQL

    Parameters:
    - breakout_level: breakout level pipeline parameter

    Returns
    - breakout_str: A SQL-compliant list
    """
    if (
        (breakout_level is None)
        | (breakout_level == "")
        | (breakout_level.lower() == "null")
        | (breakout_level == "''")
    ):
        breakout_str = "('')"
    else:
        breakout_str = "('" + breakout_level.replace(",", "', '") + "')"

    return breakout_str


def get_source_ids(
    ap_data_sector: str,
    analysis_year,
    analysis_run_group: str,
    breakout_level: str,
    force_refresh: str,
) -> str:
    """Determines which decision groups to run in DME core

    Using the input arguments, determines which decision groups (source_id's)
    need to be run through DME. During normal runs, updates to AAE output,
    feature_export, or check flags will cause a decision group to be flagged
    for an update.

    Parameters:
    - ap_data_sector: str
    - analysis_year: int or str that is a comma-separated sequence of years
        e.g. 2022,2023
    - analysis_run_group: str that contains ['phenohybrid1yr',
        'phenogca', 'mynoet', 'genomic_prediction']
    - breakout_level: str of one or more breakout levels. If multiple, format
        is "breakout_a,breakout_b"
    - force_refresh: str/bool indicating if all decision groups should be run

    Returns:
    - source_id_str: String in the format of a SQL list
    """

    analysis_type = get_analysis_types(analysis_run_group)

    breakout_level = get_breakout_str(breakout_level)

    with SnowflakeConnection() as sc:
        df = sc.get_data(
            """
            select DISTINCT pvs.source_id, pvs.pipeline_runid
                from(
                    select source_id, trait, MAX(pipeline_runid) AS pipeline_runid
                    from rv_ap_all_pvs
                    where ap_data_sector = '{0}'
                    AND CAST(source_year as integer) IN ({1})
                    AND analysis_type IN {2}
                    AND LOWER(loc) = 'all'
                    GROUP BY source_id,trait
                ) pvs
            INNER JOIN(
                select decision_group
                    from rv_ap_sector_experiment_config
                where ap_data_sector_name = '{0}' 
                    AND CAST(analysis_year as integer) IN ({1})
                    AND adapt_display = 1
            ) asec
            ON asec.decision_group = pvs.source_id
            INNER JOIN(
                select trait
                    from rv_ap_sector_trait_config
                where ap_data_sector_name = '{0}'
                    AND CAST(analysis_year as integer) IN ({1})
                    AND LOWER(distribution_type) IN ('normal', 'norm', 'rating', 'zinb', 'text')
                    AND LOWER(direction) IN ('positive', 'negative', 'equal', 'not equal', 'contain', 'contains', 'does not contain', 'equals')
                    AND LOWER(dme_metric) != 'na'
            ) astc 
            ON pvs.trait = astc.trait
            """.format(
                ap_data_sector, analysis_year, analysis_type
            )
        )

    if force_refresh == "False":
        with PostgresConnection() as pc:
            # Find decision groups where PVS has more recent data than DME
            df_dme = pc.get_data(
                """
                select source_id, MIN(pipeline_runid) AS pipeline_runid
                    from "dme"."dme_output_metrics"
                where ap_data_sector = '{0}'
                    AND CAST(analysis_year AS INTEGER) IN ({1})
                    AND analysis_type IN {2} 
                    AND advancement IS NOT NULL GROUP BY source_id
                """.format(
                    ap_data_sector, analysis_year, analysis_type
                )
            )

        df1 = df.merge(df_dme, on=["source_id"], how="left", suffixes=("_pvs", "_dme"))
        df1 = df1.loc[
            (df1.pipeline_runid_dme.isnull())
            | (df1.pipeline_runid_pvs > df1.pipeline_runid_dme),
        ]

        with SnowflakeConnection() as sc:
            # Find decision groups where feature_export data has refreshed more
            # recently than DME
            df2 = sc.get_data(
                """
                    select asec.source_id, MAX(fe.pipeline_runid) AS pipeline_runid
                  from(
                    select DISTINCT decision_group AS source_id, experiment_id
                        from rv_ap_sector_experiment_config
                    where ap_data_sector_name = '{0}'
                        AND CAST(analysis_year as integer) IN ({1})
                        AND adapt_display = 1
                ) asec
                INNER JOIN (
                    select DISTINCT tpad.experiment_id, tpad.trial_id, tpad.loc_selector
                      from rv_trial_pheno_analytic_dataset tpad
                    INNER JOIN rv_ap_data_sector_config adsc
                      ON LOWER(adsc.spirit_crop_name) = LOWER(tpad.crop)
                      AND adsc.spirit_country_code_list LIKE CONCAT('%', tpad.country_code, '%')
                    where adsc.ap_data_sector_name = '{0}'
                ) tpad
                ON asec.experiment_id = tpad.experiment_id
                INNER JOIN (
                    select source_id, SUBSTRING(REPLACE(CAST(MAX(last_modified) AS varchar), '-',''),0,8) AS pipeline_runid FROM rv_feature_export
                    where market_name =  '{0}'
                      AND lower(feature_name) in {2}
                    GROUP BY source_id
                   ) fe
                ON tpad.trial_id = fe.source_id
                  OR tpad.loc_selector = fe.source_id
                GROUP BY asec.source_id
                """.format(
                    ap_data_sector,
                    analysis_year,
                    breakout_level,
                )
            )

            df2 = df2.merge(df[["source_id"]], on=["source_id"], how="inner")
            df2 = df2.merge(df[["source_id"]], on=["source_id"], how="inner")
            df2 = df2.merge(
                df_dme, on=["source_id"], how="left", suffixes=("_tpad", "_dme")
            )
            df2 = df2.loc[
                df2.pipeline_runid_tpad >= df2.pipeline_runid_dme.str.slice(stop=8),
            ]

        with SnowflakeConnection() as sc:
            # Find decision groups where check flags have changed since last DME run
            df3 = sc.get_data(
                """
                select asec.source_id, MAX(tex.pipeline_runid) AS pipeline_runid
                  from(
                    select DISTINCT decision_group AS source_id, experiment_id
                        from rv_ap_sector_experiment_config
                    where ap_data_sector_name = '{0}' 
                        AND CAST(analysis_year as integer) IN ({1})
                        AND adapt_display = 1
                ) asec
                INNER JOIN (
                      SELECT
        experiment_id,
        SUBSTRING(REPLACE(CAST(MAX(
            CASE
                WHEN trial_last_chg_datetime > planting_info_last_chg_datetime THEN trial_last_chg_datetime
                ELSE planting_info_last_chg_datetime
            END
        ) AS varchar), '-', ''), 0, 8) AS pipeline_runid
    from
        rv_bb_experiment_trial_entry_daas
    where
        experiment_id IS NOT NULL
    GROUP BY
        experiment_id
                ) tex
                ON asec.experiment_id = tex.experiment_id
                GROUP BY asec.source_id
                """.format(
                    ap_data_sector, analysis_year
                )
            )

            df3 = df3.merge(df[["source_id"]], on=["source_id"], how="inner")
            df3 = df3.merge(
                df_dme, on="source_id", how="left", suffixes=("_tpad", "_dme")
            )
            df3 = df3.loc[
                df3.pipeline_runid_tpad >= df3.pipeline_runid_dme.str.slice(stop=8),
            ]
        df = pd.concat([df1, df2, df3])[["source_id"]].drop_duplicates()
    source_id_str = "('" + "', '".join(df["source_id"]) + "')"
    return source_id_str


def query_pvs_input(
    ap_data_sector: str,
    analysis_year,
    analysis_run_group: str,
    source_ids: str,
    breakout_level: str,
    use_for_erm: bool = False,
) -> pl.DataFrame:
    """Retrieves pvs analysis output

    Retrieves pvs output from updated decision groups, merged with experiment config,
    trait config, and breakout level information.

    Parameters:
    - ap_data_sector: str
    - analysis_year: int or str that is a comma-separated sequence of years
        e.g. 2022,2023
    - analysis_run_group: str that is one of ['late_phenohybrid1yr',
        'late_phenogca', 'late_mynoet', 'genomic_prediction']
    - source_ids: String in the format of a SQL list of the decision groups
        that are to be analyzed
    - breakout_level: str of one or more breakout levels. If multiple, format
        is "breakout_a,breakout_b"

    Returns:
    - output_df: polars df
    """

    analysis_type = get_analysis_types(analysis_run_group)

    asec_str = create_asec_str(
        ap_data_sector, analysis_year, analysis_run_group, source_ids
    )

    astc_str = create_astc_str(ap_data_sector, analysis_year, source_ids)

    if use_for_erm:
        fe_str = ""
        mat_filter = """
          AND (LOWER(market_seg) = 'all' OR analysis_type = 'GenoPred')
          AND material_type = 'entry' -- hybrid rm estimates only supported for now
          AND trait IN ('GMSTP', 'MRTYN')
        """
        fe_str2 = ""
        output_format = "pandas"
    else:
        fe_str = """
    LEFT JOIN ( {0} ) fe
    ON pvs.breakout_level_value = fe.breakout_level_value
        AND fe.breakout_level_value != 'all'
        """.format(
            create_fe_str(ap_data_sector, breakout_level)
        )
        mat_filter = ""
        fe_str2 = """COALESCE(fe.breakout_level, 'na') AS breakout_level,
        pvs.breakout_level_value,
        """
        output_format = "polars"

    pvs_str = """
    SELECT 
        pvs.ap_data_sector,
        pvs.analysis_type,
        pvs.analysis_year,
        pvs.decision_group,
        asec.stage,
        asec.decision_group_rm,
        {8}
        pvs.trait,
        pvs.be_bid,
        pvs.material_type,
        COALESCE(CAST(pvs.count AS integer), 0) AS count,
        CASE WHEN pvs.material_type IN ('entry', 'hybrid')
            THEN GREATEST_IGNORE_NULLS(0.0, pvs.prediction)
            ELSE pvs.prediction
        END AS prediction,
        CASE 
            WHEN pvs.stderr IS NULL 
                AND LOWER(pvs.material_type) = 'entry' 
                AND LOWER(astc.distribution_type) LIKE 'norm%'
                THEN pvs.prediction*10
            WHEN pvs.stderr > 1000 
                AND pvs.prediction < 500
                THEN LEAST_IGNORE_NULLS(pvs.stderr, pvs.prediction*10) 
            ELSE pvs.stderr
        END AS stderr,
        astc.distribution_type,
        astc.direction,
        astc.yield_trait,
        astc.metric_name,
        astc.dme_chkfl,
        astc.dme_reg_x,
        astc.dme_reg_y,
        astc.dme_rm_est,
        astc.dme_weighted_trait
      FROM (
        SELECT
            ap_data_sector AS ap_data_sector,
            CAST(source_year AS integer) AS analysis_year,
            analysis_type AS analysis_type,
            source_id AS decision_group,
            pipeline_runid AS pipeline_runid,
            REPLACE(TRIM(LOWER(market_seg)), ' ', '_') AS breakout_level_value,
            trait AS trait,
            entry_identifier AS be_bid,
            COALESCE(LOWER(material_type), 'entry') AS material_type,
            count AS count,
            prediction AS prediction,
            stderr AS stderr,
            ROW_NUMBER() OVER (PARTITION BY source_year, analysis_type, source_id, entry_identifier, market_seg, trait ORDER BY pipeline_runid desc) AS row_number
          FROM rv_ap_all_pvs
        WHERE ap_data_sector = '{0}'
          AND CAST(source_year AS integer) IN ({1})
          AND analysis_type IN {2}
          AND source_id IN {3}
          AND loc = 'ALL'
          AND prediction IS NOT NULL
          {7}
    ) pvs
    INNER JOIN ( {4} ) asec
      ON pvs.decision_group = asec.decision_group
      AND pvs.analysis_year = asec.analysis_year
    INNER JOIN ( {5} ) astc
      ON astc.ap_data_sector = asec.ap_data_sector
        AND astc.analysis_year = asec.analysis_year
        AND astc.trait = pvs.trait
    {6}
    WHERE pvs.row_number = 1
    """.format(
        ap_data_sector,
        analysis_year,
        analysis_type,
        source_ids,
        asec_str,
        astc_str,
        fe_str,
        mat_filter,
        fe_str2,
    )

    print(pvs_str)

    with SnowflakeConnection() as sc:
        output_df = sc.get_data(
            pvs_str,
            package=output_format,
            schema_overrides={
                "analysis_year": pl.Int32,
                "decision_group_rm": pl.Float32,
                "stage": pl.Float32,
                "count": pl.Int32,
                "prediction": pl.Float32,
                "stderr": pl.Float32,
            },
            do_lower=True,
        )

    return output_df


# query_check_entries
# retrieve check flags and parentage based on rv_bb_experiment_trial_entry, LaaS, and CMT where needed.
# ACTIVE 2024-02-14
def query_check_entries(
    ap_data_sector: str, analysis_year, analysis_run_group: str, decision_groups: str
) -> pl.DataFrame:
    with SnowflakeConnection() as sc:
        sql = """
            select DISTINCT
        "checks".ap_data_sector AS "ap_data_sector",
        "checks".analysis_year AS "analysis_year",
        "checks".decision_group AS "decision_group",
        "checks".untested_entry_display AS "untested_entry_display",
        "checks".be_bid AS "be_bid",
        CAST(GREATEST("checks".cpifl,
             "checks".cperf, 
             "checks".cagrf, 
             "checks".cmatf,
             "checks".cregf,
             "checks".crtnf) AS boolean) AS "cpifl",
        "checks".cperf AS "cperf",
        "checks".cagrf AS "cagrf",
        "checks".cmatf AS "cmatf",
        "checks".cregf AS "cregf",
        "checks".crtnf AS "crtnf",
        "checks".fp_ltb AS "fp_ltb",
        "checks".mp_ltb AS "mp_ltb",
        'entry' AS "material_type",
        "bbal".receiver_p AS "par1_be_bid",
        "bbal".donor_p AS "par2_be_bid"
    FROM(
        select 
        ec.ap_data_sector_name AS ap_data_sector,
        ec.analysis_year,
        ec.decision_group,
        mbu.be_bid,
            max(ec.untested_entry_display) as untested_entry_display,
            max(CASE WHEN check_agronomic AND ec.exp_year = ec.max_exp_year THEN 1 else 0 end) = 1 as cagrf,
            max(CASE WHEN check_competitor AND ec.exp_year = ec.max_exp_year THEN 1 else 0 end) = 1 as ccomp,
            max(CASE WHEN check_flag AND ec.exp_year = ec.max_exp_year THEN 1 else 0 end) = 1 as cpifl,
            max(CASE WHEN check_line_to_beat_f AND ec.exp_year = ec.max_exp_year THEN 1 else 0 end) = 1 as fp_ltb,
            max(CASE WHEN check_line_to_beat_m AND ec.exp_year = ec.max_exp_year THEN 1 else 0 end) = 1 as mp_ltb,
            max(CASE WHEN check_maturity AND ec.exp_year = ec.max_exp_year THEN 1 else 0 end) = 1 as cmatf,
            max(CASE WHEN check_performance AND ec.exp_year = ec.max_exp_year THEN 1 else 0 end) = 1 as cperf,
            max(CASE WHEN check_regional AND ec.exp_year = ec.max_exp_year THEN 1 else 0 end) = 1 as cregf,
            max(CASE WHEN check_registration AND ec.exp_year = ec.max_exp_year THEN 1 else 0 end) = 1 as crtnf
        from (
            select distinct 
                ex.code          as experiment_id,
                m.be_bid 					as be_bid,
                coalesce(td.check_agronomic, p.check_agronomic, false) as check_agronomic,
                coalesce(td.check_competitor, p.check_competitor, false) as check_competitor,
                coalesce(td.check_flag, p.check_flag, false) as check_flag,
                coalesce(td.check_line_to_beat_f, p.check_line_to_beat_f, false) as check_line_to_beat_f,
                coalesce(td.check_line_to_beat_m, p.check_line_to_beat_m, false) as check_line_to_beat_m,
                coalesce(td.check_maturity, p.check_maturity, false) as check_maturity,
                coalesce(td.check_performance, p.check_performance, false) as check_performance,
                coalesce(td.check_regional, p.check_regional, false) as check_regional,
                coalesce(td.check_registration, p.check_registration, false) as check_registration
            from rv_experiments_loc360 ex
            join rv_trials_loc360 t on t.experiment_uuid = ex.object_uuid
            join rv_mapped_plot_stats_loc360 p on p.trial_uuid = t.object_uuid
            join rv_bb_material_daas m on lower(m.material_guid) = p.material_batch_uuid
            left outer join(
                select distinct 
                    experiment_id,
                    be_bid,
                    coalesce(cpi, 0) = 1 as check_flag,
                    coalesce(check_agronomic, 0) = 1 as check_agronomic,
                    coalesce(check_maturity, 0) = 1 as check_maturity,
                    coalesce(check_performance, 0) = 1 as check_performance,
                    coalesce(check_regional, 0) = 1 as check_regional,
                    coalesce(check_registration, 0) = 1 as check_registration,
                    coalesce(check_line_to_beat_f, 0) = 1 as check_line_to_beat_f,
                    coalesce(check_line_to_beat_m, 0) = 1 as check_line_to_beat_m,
                    coalesce(check_competitor, 0) = 1 as check_competitor
                from rv_bb_experiment_trial_entry_daas) td
            on td.experiment_id = ex.code and td.be_bid = m.be_bid
            ) mbu
        inner join (
            select ap_data_sector_name,
                cast(analysis_year as integer) as analysis_year,
                max(cast(substring(experiment_id, 0, 2) as integer)) OVER (partition by decision_group) as max_exp_year,
                cast(substring(experiment_id, 0, 2) as integer) as exp_year,
                experiment_id,
                decision_group,
                untested_entry_display
            from rv_ap_sector_experiment_config
            where decision_group is not null
            and ap_data_sector_name = '{0}' 
            AND CAST(analysis_year as integer) IN ({1})
        ) ec 
        on ec.experiment_id = mbu.experiment_id 
        group by ec.ap_data_sector_name,
        ec.analysis_year,
        ec.decision_group,
        mbu.be_bid
    ) "checks"
    LEFT JOIN (
        SELECT *
          FROM rv_be_bid_ancestry 
        WHERE receiver_p <> donor_p
        AND (receiver_p IS NOT NULL OR donor_p IS NOT NULL)
    )"bbal"
    ON "checks".be_bid = "bbal".be_bid
    """.format(
            ap_data_sector,
            analysis_year,
        )
        output_df = sc.get_data(sql, package="polars")

    if (analysis_run_group == "genomic_prediction") | (
        "phenogca" in analysis_run_group.lower()
    ):
        mat_type_df = query_pvs_material_types(
            ap_data_sector, analysis_year, analysis_run_group, decision_groups
        )

        if mat_type_df.shape[0] > 0:
            mat_type_df = (
                mat_type_df.with_columns(
                    gr_count=pl.len().over(["decision_group", "be_bid"])
                )
                .filter(pl.col("gr_count") == 1)
                .drop("gr_count")
            )

            swap_cols = {
                "par1_be_bid": "par2_be_bid",
                "par2_be_bid": "par1_be_bid",
                "fp_ltb": "mp_ltb",
                "mp_ltb": "fp_ltb",
                "material_type_par1": "material_type_par2",
                "material_type_par2": "material_type_par1",
            }

            output_df = (
                output_df.join(
                    mat_type_df,
                    left_on=["decision_group", "par1_be_bid"],
                    right_on=["decision_group", "be_bid"],
                    suffix="_par1",
                    coalesce=True,
                )
                .join(
                    mat_type_df,
                    left_on=["decision_group", "par2_be_bid"],
                    right_on=["decision_group", "be_bid"],
                    suffix="_par2",
                    coalesce=True,
                )
                .with_columns(
                    pl.when(
                        (pl.col("material_type_par1").is_in(["female", "pool1"]))
                        | (pl.col("material_type_par2").is_in(["male", "pool2"]))
                    )
                    .then(pl.col(c1))
                    .otherwise(pl.col(c2))
                    .alias(c1)
                    for c1, c2 in swap_cols.items()
                )
            )
        else:
            output_df = output_df.with_columns(
                pl.lit(None).alias("material_type_par1"),
                pl.lit(None).alias("material_type_par2"),
            )

    return output_df


def query_trial_input(
    ap_data_sector: str,
    analysis_year,
    analysis_run_group: str,
    source_ids: str,
    breakout_level: str,
    result_type: str = "numeric",
) -> pl.DataFrame:
    """Retrieves trial data

    Retrieves data from rv_trial_pheno_analytic_dataset from updated decision groups, merged with experiment config,
    trait config, and breakout level information.

    Parameters:
    - ap_data_sector: str
    - analysis_year: int or str that is a comma-separated sequence of years
        e.g. 2022,2023
    - analysis_run_group: str that is one of ['late_phenohybrid1yr',
        'late_phenogca', 'late_mynoet', 'genomic_prediction']
    - source_ids: String in the format of a SQL list of the decision groups
        that are to be analyzed
    - breakout_level: str of one or more breakout levels. If multiple, format
        is "breakout_a,breakout_b"
    - result_type: "numeric" or "alpha" to indicate whether to retrieve numeric
        or alpha data

    Returns:
    - output_df: pandas df
    """

    asec_str = create_asec_str(
        ap_data_sector,
        analysis_year,
        analysis_run_group,
        source_ids,
        use_experiment_id=True,
    )

    fe_str = create_fe_str(ap_data_sector, breakout_level, use_source_id=True)

    if result_type == "numeric":
        astc_str = create_astc_str(
            ap_data_sector, analysis_year, source_ids, use_pvs_filter=True
        )
        output_str = """CASE WHEN astc.conv_factor > 0 AND astc.conv_factor != 1 AND IS_DECIMAL(astc.conv_factor)
            THEN tpad.result_{0}_value/astc.conv_factor
            ELSE tpad.result_{0}_value END AS result_{0}_value,""".format(
            result_type
        )
    else:
        astc_str = create_astc_str(
            ap_data_sector,
            analysis_year,
            source_ids,
            use_pvs_filter=True,
            use_alpha_traits=True,
        )
        output_str = "tpad.result_alpha_value,"

    tpad_str = """
        WITH tpad_base AS (SELECT DISTINCT
            asec.ap_data_sector,
            asec.analysis_year,
            asec.analysis_type,
            asec.decision_group,
            asec.decision_group_rm,
            asec.stage,
            tpad.experiment_id,
            tpad.trial_id,
            tpad.loc_selector,
            tpad.plot_barcode,
            tpad.be_bid,
            tpad.trait_measure_code AS trait,
            {5}
            astc.distribution_type,
            astc.direction,
            astc.yield_trait,
            astc.metric_name,
            astc.dme_chkfl,
            astc.dme_reg_x,
            astc.dme_reg_y,
            astc.dme_rm_est,
            astc.in_pvs,
            astc.dme_weighted_trait
        FROM rv_ap_data_sector_config adsc 
        INNER JOIN rv_trial_pheno_analytic_dataset tpad
          ON LOWER(adsc.spirit_crop_name) = LOWER(tpad.crop)
          AND adsc.spirit_country_code_list LIKE CONCAT('%', tpad.country_code, '%')
        INNER JOIN ( {2} ) asec
        on asec.experiment_id = tpad.experiment_id
        INNER JOIN ( {3} ) astc
        on astc.trait = tpad.trait_measure_code
        WHERE adsc.ap_data_sector_name = '{1}'
            AND NOT COALESCE(CAST(tpad.tr_exclude AS boolean), FALSE)
            AND NOT COALESCE(CAST(tpad.psp_exclude AS boolean), FALSE)
            AND NOT COALESCE(CAST(tpad.pr_exclude AS boolean), FALSE)
            AND NOT COALESCE(CAST(tpad.outlier AS boolean), FALSE)
            AND tpad.result_{0}_value IS NOT NULL)

        SELECT tpad_base.*, fet.breakout_level, fet.breakout_level_value FROM tpad_base
        INNER JOIN ({4}) fet
        ON tpad_base.trial_id = fet.source_id

        UNION All
        SELECT tpad_base.*, fel.breakout_level, fel.breakout_level_value FROM tpad_base
        INNER JOIN ({4}) fel
        ON tpad_base.loc_selector = fel.source_id

        UNION ALL
        SELECT *, 'na' AS breakout_level, 'all' AS breakout_level_value
        FROM tpad_base
    """.format(
        result_type, ap_data_sector, asec_str, astc_str, fe_str, output_str
    )

    print(tpad_str)

    with SnowflakeConnection() as sc:

        tr_df = sc.get_data(
            tpad_str,
            package="polars",
            schema_overrides={
                "trial_id": pl.String,
                "loc_selector": pl.String,
                "be_bid": pl.String,
                "decision_group_rm": pl.Float32,
                "stage": pl.Float32,
                "experiment_id": pl.String,
                "plot_barcode": pl.String,
                "trait": pl.String,
                "yield_trait": pl.Boolean,
                "in_pvs": pl.Boolean,
            },
            do_lower=True,
        )

        print(tr_df.schema)

    if (result_type == "numeric") & (tr_df.shape[0] > 0):
        print("Generate list of traits for stability & other enhanced metrics")
        # Generate list of traits for stability & other enhanced metrics
        stability_df = tr_df.filter(pl.col("yield_trait")).with_columns(
            pl.lit("stability").alias("metric_name")
        )

        tr_df = tr_df.filter(
            pl.col("dme_reg_x") | (pl.col("in_pvs").not_())
        ).with_columns(pl.col("metric_name").alias("metric_name"))
        tr_df = pl.concat([tr_df, stability_df])

        tr_df = tr_df.with_columns(
            pl.when(pl.col("metric_name") == "stability")
            .then(
                (
                    pl.col("result_numeric_value")
                    - pl.col("result_numeric_value").mean().over(["trial_id", "trait"])
                )
            )
            .otherwise(pl.col("result_numeric_value"))
            .cast(pl.Float32)
            .alias("result_numeric_value"),
        ).drop(["loc_selector", "in_pvs"])

    return tr_df


def get_data_sector_config(ap_data_sector: str) -> pd.DataFrame:
    """Retrieves data sector configuration details

    Retrieves a row from ap_data_sector_config.

    Parameters:
    - ap_data_sector: str

    Returns:
    - data_sector_config: pandas df
    """

    with SnowflakeConnection() as sc:
        data_sector_config = sc.get_data(
            """
            select
                ap_data_sector_name,
                spirit_crop_guid
            from rv_ap_data_sector_config
                where ap_data_sector_name = '{0}' 
            """.format(
                ap_data_sector
            ),
            do_lower=True,
        )
    return data_sector_config


def create_asec_str(
    ap_data_sector: str,
    analysis_year,
    analysis_run_group: str,
    decision_groups: str,
    use_experiment_id: bool = False,
) -> str:
    """Generates text for a subquery to rv_ap_sector_experiment_config

    Returns a subquery to retrieve the ap_data_sector, analysis_year, analysis_type,
     stage, rm, and potentially experiment_id's of the decision groups requested

     Parameters:
    - ap_data_sector: str
    - analysis_year: int or str that is a comma-separated sequence of years
        e.g. 2022,2023
    - analysis_run_group: str that is one of ['late_phenohybrid1yr',
        'late_phenogca', 'late_mynoet', 'genomic_prediction']
    - decision_groups: String in the format of a SQL list of the decision groups
        that are to be analyzed
    - use_experiment_id: bool indicating if experiment_id should be requested

    Returns:
    - out: string
    """

    if "phenogca" in analysis_run_group.lower():
        analysis_type = "'PhenoGCA'"
    elif analysis_run_group.lower() == "genomic_prediction":
        analysis_type = "'GenoPred'"
    else:
        analysis_type = """CASE WHEN COUNT(experiment_id) OVER (PARTITION BY decision_group) = 1
            THEN 'SingleExp'
            WHEN MIN(year) OVER (PARTITION BY decision_group) < MAX(year) OVER (PARTITION BY decision_group)
            THEN 'MynoET'
            ELSE 'MultiExp'
        END"""

    if use_experiment_id:
        experiment_str = "experiment_id,"
    else:
        experiment_str = ""
    out = """
    select DISTINCT
        ap_data_sector,
        analysis_year,
        {4} as analysis_type,
        decision_group,
        {3}
        MAX(CASE WHEN year = max_year then stage else null end) OVER (PARTITION BY decision_group) as stage,
        median(CASE when year = max_year then decision_group_rm else null end) OVER (PARTITION BY decision_group) as decision_group_rm
    from (
        select
            asec.ap_data_sector_name as ap_data_sector,
            CAST(MEDIAN( asec.analysis_year) as INTEGER) as analysis_year,
            asec.decision_group,
            asec.experiment_id,
            round(coalesce(MAX(try_to_number(t.stage)), MAX(asec.stage)),2) as stage,
            round(coalesce(median(asec.decision_group_rm), MEDIAN(try_to_number(t.maturity_group)), 0),3) as decision_group_rm,
            MEDIAN(t.year) as year,
            MAX(MAX(t.year)) OVER (PARTITION BY asec.decision_group) as max_year from 
        rv_ap_sector_experiment_config asec
        join rv_experiments_loc360 ex on asec.experiment_id = ex.code
        join rv_trials_loc360 t on t.experiment_uuid = ex.object_uuid
        where not ex.invalidated and not t.invalidated AND not t.exclude_from_analysis
            and ap_data_sector_name = '{0}'
                AND CAST(analysis_year AS integer) IN ({1})
                AND decision_group in {2}
        group by all
    )
    """.format(
        ap_data_sector, analysis_year, decision_groups, experiment_str, analysis_type
    )

    return out


def create_astc_str(
    ap_data_sector: str,
    analysis_year,
    decision_groups: str,
    use_pvs_filter: bool = False,
    use_alpha_traits: bool = False,
) -> str:
    """Generates text for a subquery to rv_ap_sector_trait_config

    Returns a subquery to retrieve the specifications of the traits to be analyzed
    for the decision groups requested

     Parameters:
    - ap_data_sector: str
    - analysis_year: int or str that is a comma-separated sequence of years
        e.g. 2022,2023
    - decision_groups: String in the format of a SQL list of the decision groups
        that are to be analyzed
    - use_pvs_filter: filter list based on what traits are present in pvs
    - use_alpha_traits: return alpha traits (True) or numeric traits (False)

    Returns:
    - out: string
    """

    if use_pvs_filter:
        trait_str = query_pvs_trait_list(ap_data_sector, analysis_year, decision_groups)
        pvs_str = "AND (trait NOT IN {0} OR yield_trait = '1')".format(trait_str)
        pvs_str2 = "CASE WHEN trait IN {0} THEN true ELSE false END AS in_pvs,".format(
            trait_str
        )
        pvs_str3 = "in_pvs,"

        if use_alpha_traits:
            dist_type_filter = "AND TRIM(LOWER(distribution_type)) = 'text'"
        else:
            dist_type_filter = "AND TRIM(LOWER(distribution_type)) IN ('normal', 'norm', 'rating', 'zinb')"
    else:
        pvs_str = ""
        pvs_str2 = ""
        pvs_str3 = ""
        dist_type_filter = (
            "AND TRIM(LOWER(distribution_type)) IN ('normal', 'norm', 'rating', 'zinb')"
        )

    out = """
     SELECT
            ap_data_sector,
            analysis_year,
            trait,
            CASE 
                WHEN distribution_type ='norm'
                    THEN 'normal'
                WHEN distribution_type = 'rating' AND metric_name = 'h2h'
                    THEN 'normal'
                ELSE distribution_type 
            END AS distribution_type,
            direction,
            conv_operator,
            conv_factor,
            yield_trait,
            CASE
                WHEN metric_name IN ('agronomic', 'disease')
                    THEN 'risk'
                WHEN metric_name = 'yield' 
                    THEN 'performance'
                ELSE metric_name
            END AS metric_name,
            dme_chkfl,
            dme_reg_x,
            dme_reg_y,
            dme_rm_est,
            {4}
            dme_weighted_trait
        FROM (
            SELECT 
                ap_data_sector_name AS ap_data_sector,
                CAST(analysis_year AS integer) AS analysis_year,
                trait,
                TRIM(LOWER(distribution_type)) AS distribution_type,
                TRIM(LOWER(direction)) AS direction,
                conv_operator,
                conv_factor,
                CASE WHEN yield_trait = '1' AND TRIM(LOWER(dme_metric)) IN ('performance', 'yield') THEN TRUE ELSE FALSE END AS yield_trait,
                TRIM(LOWER(dme_metric)) AS metric_name,
                dme_chkfl,
                dme_reg_x,
                dme_reg_y,
                dme_rm_est,
                dme_weighted_trait,
                {3}
                ROW_NUMBER() OVER (PARTITION BY analysis_year, trait ORDER BY distribution_type, direction) as row_number FROM rv_ap_sector_trait_config
            WHERE ap_data_sector_name = '{0}'
                AND CAST(analysis_year AS integer) IN ({1})
                {5}
                AND TRIM(LOWER(direction)) IN ('positive', 'negative', 'equal', 'not equal', 'contain', 'contains', 'does not contain', 'equals')
                AND LOWER(level) = 'plot'
                AND (dme_metric != 'na' OR dme_metric is not null or dme_metric != '' OR dme_reg_x = true OR dme_reg_y = true OR dme_rm_est != 0 OR dme_weighted_trait != 0)
                {2}
        ) astc
        WHERE row_number = 1
            """.format(
        ap_data_sector, analysis_year, pvs_str, pvs_str2, pvs_str3, dist_type_filter
    )

    return out


def create_fe_str(
    ap_data_sector: str, breakout_level: str, use_source_id: bool = False
):
    """Generates text for a subquery to rv_feature_export

    Returns a subquery to retrieve the breakout levels, values, and optionally
    related source_id's for a data sector

     Parameters:
    - ap_data_sector: str
    - breakout_level: str of one or more breakout levels. If multiple, format
        is "breakout_a,breakout_b"

    - use_source_id: bool indicating if source_id column (contains trial id's and loc selectors)
        should be requested

    Returns:
    - out: string
    """
    breakout_level = get_breakout_str(breakout_level)

    if use_source_id:
        source_id_str = "source_id,"
    else:
        source_id_str = ""
    out = """SELECT DISTINCT
            {2}
            LOWER(feature_name) AS breakout_level,
            LOWER(REGEXP_REPLACE(TRANSLATE(TRIM(TRIM(value), '.'),'-','_'), '[ :;*+/&\\_,]+', '_')) AS breakout_level_value
        FROM rv_feature_export
        WHERE LOWER(feature_name) IN {1}
            AND market_name = '{0}'""".format(
        ap_data_sector, breakout_level, source_id_str
    )

    return out


def query_pvs_trait_list(ap_data_sector, analysis_year, decision_groups):
    with SnowflakeConnection() as sc:
        out = sc.get_data(
            """SELECT DISTINCT trait from rv_ap_all_pvs WHERE ap_data_sector = '{0}'
                AND CAST(source_year AS integer) IN ({1}) AND source_id IN {2}""".format(
                ap_data_sector, analysis_year, decision_groups
            )
        )

        out = "('" + "', '".join(out["trait"]) + "')"

        print(out)
    return out


def query_pvs_material_types(
    ap_data_sector: str,
    analysis_year,
    analysis_run_group: str,
    decision_groups: str,
    untested_entry: bool = False,
):
    analysis_type = get_analysis_types(analysis_run_group)

    if untested_entry:
        mat_type_str = " = 'untested_entry'"
    else:
        mat_type_str = " not like '%entry'"

    with SnowflakeConnection() as sc:
        out = sc.get_data(
            """SELECT DISTINCT 
                  source_id as decision_group, 
                  entry_identifier as be_bid, 
                  material_type
                from rv_ap_all_pvs WHERE ap_data_sector = '{0}'
                AND CAST(source_year AS integer) IN ({1})
                AND analysis_type IN {2}
                AND source_id IN {3}
                AND LOWER(material_type){4}""".format(
                ap_data_sector,
                analysis_year,
                analysis_type,
                decision_groups,
                mat_type_str,
            ),
            package="polars",
            schema_overrides=None,
            do_lower=True,
        )

    return out
