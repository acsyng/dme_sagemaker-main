def regional_aggregate_query_sql_query_na_corn(spark, dku_dst_maturity, dku_dst_analysis_year):
    output_df = spark.sql("""
    SELECT 
                    `gp`.`place_id` AS `place_id`,
                    `gp`.`entry_id` AS `entry_id`,
                    `gp`.`maturity` AS `maturity`, 
                    CAST(`gp`.`cperf` AS boolean) AS `cperf`,
                    `gp`.`predict_ygsmn` AS `predict_ygsmn`,
                    `gp`.`YGSMNp_10` AS `YGSMNp_10`, 
                    `gp`.`YGSMNp_50` AS `YGSMNp_50`, 
                    `gp`.`YGSMNp_90` AS `YGSMNp_90`,
                    `gp`.`irrigation` AS `irrigation`,
                    `gp`.`env_input_outlier_ind` AS `env_input_outlier_ind`, 
                    `gp`.`density` AS `density`, 
                    CAST(`gp`.`previous_crop_corn` AS boolean) AS `previous_crop_corn`,
                    `gp`.`ap_data_sector` AS `ap_data_sector`, 
                    `gp`.`analysis_year` AS `analysis_year`, 
                    `gc`.`rm_band` AS `rm_band`, 
                    `gc`.`wce` AS `wce`,
                    `gc`.`tpp` AS `tpp`,
                    `gc`.`market_segment` AS `market_segment`,
                    `gc`.`maturity_acres` AS `maturity_acres`
                  FROM `grid_performance` `gp`
                INNER JOIN (
                    SELECT
                        `gc`.`place_id`,
                        `gc`.`rm_band`,
                        `gc`.`wce`,
                        `gc`.`tpp`,
                        `gc`.`market_segment`,
                        `gmm`.`maturity`,
                        `gmm`.`maturity_acres`
                      FROM `grid_classification` `gc`
                    INNER JOIN `grid_market_maturity` `gmm`
                    ON `gc`.`place_id` = `gmm`.`place_id`
                      AND `gmm`.`ap_data_sector` = `gc`.`ap_data_sector`
                      AND `gmm`.`analysis_year` = `gc`.`analysis_year`
                    WHERE `gc`.`ap_data_sector` = '{2}'
                      AND `gc`.`analysis_year` = {1}
                      AND `gmm`.`ap_data_sector` = '{2}'
                      AND `gmm`.`analysis_year` = {1}
                      AND `gmm`.`maturity` = '{0}'
                ) `gc`
                ON `gp`.`place_id` = `gc`.`place_id`
                  AND `gp`.`maturity` = `gc`.`maturity`
                WHERE `gp`.`maturity` = '{0}'
                  AND `gp`.`analysis_year` = {1}
                  AND `gp`.`ap_data_sector` = '{2}'""".format(dku_dst_maturity, dku_dst_analysis_year, "CORN_NA_SUMMER"))

    return output_df


def regional_aggregate_query_sql_query_na_soy(spark, dku_dst_maturity, dku_dst_analysis_year):
    output_df = spark.sql("""
    SELECT 
                    `gp`.`place_id` AS `place_id`,
                    `gp`.`entry_id` AS `entry_id`,
                    `gp`.`maturity` AS `maturity`, 
                    CAST(`gp`.`cperf` AS boolean) AS `cperf`,
                    `gp`.`predict_ygsmn` AS `predict_ygsmn`,
                    `gp`.`YGSMNp_10` AS `YGSMNp_10`, 
                    `gp`.`YGSMNp_50` AS `YGSMNp_50`, 
                    `gp`.`YGSMNp_90` AS `YGSMNp_90`,
                    `gp`.`irrigation` AS `irrigation`, 
                    `gp`.`env_input_outlier_ind` AS `env_input_outlier_ind`,
                    `gp`.`density` AS `density`, 
                    CAST(`gp`.`previous_crop_soy` AS boolean) AS `previous_crop_soy`,
                    `gp`.`ap_data_sector` AS `ap_data_sector`, 
                    `gp`.`analysis_year` AS `analysis_year`, 
                    `gc`.`rm_band` AS `rm_band`, 
                    `gc`.`ecw` AS `ecw`,
                    `gc`.`acs` AS `acs`,
                    `gc`.`maturity_acres` AS `maturity_acres`
                  FROM `grid_performance` `gp`
                INNER JOIN (
                    SELECT
                        `gc`.`place_id`,
                        `gc`.`rm_band`,
                        `gc`.`ecw`,
                        `gc`.`acs`,
                        `gmm`.`maturity`,
                        `gmm`.`maturity_acres`
                      FROM `grid_classification` `gc`
                    INNER JOIN `grid_market_maturity` `gmm`
                    ON `gc`.`place_id` = `gmm`.`place_id`
                      AND `gmm`.`ap_data_sector` = `gc`.`ap_data_sector`
                      AND `gmm`.`analysis_year` = `gc`.`analysis_year`
                    WHERE `gc`.`ap_data_sector` = '{2}'
                      AND `gc`.`analysis_year` = {1}
                      AND `gmm`.`ap_data_sector` = '{2}'
                      AND `gmm`.`analysis_year` = {1}
                      AND `gmm`.`maturity` = '{0}'
                ) `gc`
                ON `gp`.`place_id` = `gc`.`place_id`
                  AND `gp`.`maturity` = `gc`.`maturity`
                WHERE `gp`.`maturity` = '{0}'
                  AND `gp`.`analysis_year` = {1}
                  AND `gp`.`ap_data_sector` = '{2}'""".format(dku_dst_maturity, dku_dst_analysis_year, "SOY_NA_SUMMER"))

    return output_df


def regional_aggregate_query_sql_query_corn_brazil(spark, dku_dst_maturity, dku_dst_analysis_year,
                                                   dku_dst_ap_data_sector):
    output_df = spark.sql("""
    SELECT 
                    `gp`.`place_id` AS `place_id`,
                    `gp`.`entry_id` AS `entry_id`,
                    `gp`.`maturity` AS `maturity`,
                    `gp`.`maturity` AS `maturity_group`,  
                    CAST(`gp`.`cperf` AS boolean) AS `cperf`,
                    `gp`.`predict_ygsmn` AS `predict_ygsmn`,
                    `gp`.`YGSMNp_10` AS `YGSMNp_10`, 
                    `gp`.`YGSMNp_50` AS `YGSMNp_50`, 
                    `gp`.`YGSMNp_90` AS `YGSMNp_90`,
                    `gp`.`irrigation` AS `irrigation`, 
                    `gp`.`env_input_outlier_ind` AS `env_input_outlier_ind`, 
                    `gp`.`density` AS `density`, 
                    CAST(`gp`.`previous_crop_corn` AS boolean) AS `previous_crop_corn`,
                    `gp`.`ap_data_sector` AS `ap_data_sector`, 
                    `gp`.`analysis_year` AS `analysis_year`, 
                    `gc`.`meso` AS `meso`,
                    `gc`.`tpp` AS `tpp`,
                    `gc`.`maturity_acres` AS `maturity_acres`
                  FROM `grid_performance` `gp`
                INNER JOIN (
                    SELECT
                        `gc`.`place_id`,
                        `gc`.`meso`,
                        `gc`.`tpp`,
                        `gmm`.`maturity`,
                        `gmm`.`maturity_acres`
                      FROM `grid_classification` `gc`
                    INNER JOIN `grid_market_maturity` `gmm`
                    ON `gc`.`place_id` = `gmm`.`place_id`
                      AND `gmm`.`ap_data_sector` = `gc`.`ap_data_sector`
                      AND `gmm`.`analysis_year` = `gc`.`analysis_year`
                    WHERE `gc`.`ap_data_sector` = '{2}'
                      AND `gc`.`analysis_year` = {1}
                      AND `gmm`.`ap_data_sector` = '{2}'
                      AND `gmm`.`analysis_year` = {1}
                      AND `gmm`.`maturity` = {0}
                ) `gc`
                ON `gp`.`place_id` = `gc`.`place_id`
                  AND `gp`.`maturity` = `gc`.`maturity`
                WHERE `gp`.`maturity` = {0}
                  AND `gp`.`analysis_year` = {1}
                  AND `gp`.`ap_data_sector` = '{2}'""".format(dku_dst_maturity, dku_dst_analysis_year,
                                                              dku_dst_ap_data_sector))

    return output_df


def regional_aggregate_query_sql_query_corngrain_eame(spark, dku_dst_maturity, dku_dst_analysis_year,
                                                      dku_dst_ap_data_sector):
    output_df = spark.sql("""
    SELECT 
                    `gp`.`place_id` AS `place_id`,
                    `gp`.`entry_id` AS `entry_id`,
                    `gp`.`maturity` AS `maturity`,
                    `gp`.`maturity` AS `maturity_group`,  
                    CAST(`gp`.`cperf` AS boolean) AS `cperf`,
                    `gp`.`predict_ygsmn` AS `predict_ygsmn`,
                    `gp`.`YGSMNp_10` AS `YGSMNp_10`, 
                    `gp`.`YGSMNp_50` AS `YGSMNp_50`, 
                    `gp`.`YGSMNp_90` AS `YGSMNp_90`,
                    `gp`.`irrigation` AS `irrigation`, 
                    `gp`.`env_input_outlier_ind` AS `env_input_outlier_ind`, 
                    `gp`.`density` AS `density`, 
                    CAST(`gp`.`previous_crop_corn` AS boolean) AS `previous_crop_corn`,
                    `gp`.`ap_data_sector` AS `ap_data_sector`, 
                    `gp`.`analysis_year` AS `analysis_year`, 
                    `gc`.`country` AS `country`,
                    `gc`.`mst` AS `mst`,
                    `gc`.`maturity_acres` AS `maturity_acres`
                  FROM `grid_performance` `gp`
                INNER JOIN (
                    SELECT
                        `gc`.`place_id`,
                        `gc`.`country`,
                        `gc`.`mst`,
                        `gmm`.`maturity`,
                        `gmm`.`maturity_acres`
                      FROM `grid_classification` `gc`
                    INNER JOIN `grid_market_maturity` `gmm`
                    ON `gc`.`place_id` = `gmm`.`place_id`
                      AND `gmm`.`ap_data_sector` = `gc`.`ap_data_sector`
                      AND `gmm`.`analysis_year` = `gc`.`analysis_year`
                    WHERE `gc`.`ap_data_sector` = '{2}'
                      AND `gc`.`analysis_year` = {1}
                      AND `gmm`.`ap_data_sector` = '{2}'
                      AND `gmm`.`analysis_year` = {1}
                      AND `gmm`.`maturity` = {0}
                ) `gc`
                ON `gp`.`place_id` = `gc`.`place_id`
                  AND `gp`.`maturity` = `gc`.`maturity`
                WHERE `gp`.`maturity` = {0}
                  AND `gp`.`analysis_year` = {1}
                  AND `gp`.`ap_data_sector` = '{2}'""".format(dku_dst_maturity, dku_dst_analysis_year,
                                                              dku_dst_ap_data_sector))

    return output_df


def regional_aggregate_query_sql_query_soy_brazil_summer(spark, dku_dst_maturity, dku_dst_analysis_year):
    output_df = spark.sql("""
    SELECT 
                    `gp`.`place_id` AS `place_id`,
                    `gp`.`entry_id` AS `entry_id`,
                    `gp`.`maturity` AS `maturity`, 
                    `gp`.`maturity` AS `maturity_group`, 
                    CAST(`gp`.`cperf` AS boolean) AS `cperf`,
                    `gp`.`predict_ygsmn` AS `predict_ygsmn`,
                    `gp`.`YGSMNp_10` AS `YGSMNp_10`, 
                    `gp`.`YGSMNp_50` AS `YGSMNp_50`, 
                    `gp`.`YGSMNp_90` AS `YGSMNp_90`,  
                    `gp`.`irrigation` AS `irrigation`, 
                    `gp`.`env_input_outlier_ind` AS `env_input_outlier_ind`, 
                    `gp`.`density` AS `density`, 
                    CAST(`gp`.`previous_crop_corn` AS boolean) AS `previous_crop_corn`,
                    `gp`.`ap_data_sector` AS `ap_data_sector`, 
                    `gp`.`analysis_year` AS `analysis_year`, 
                    `gc`.`rec` AS `rec`,
                    `gc`.`tpp` AS `tpp`,
                    `gc`.`maturity_acres` AS `maturity_acres`
                  FROM `grid_performance` `gp`
                INNER JOIN (
                    SELECT
                        `gc`.`place_id`,
                        `gc`.`rec`,
                        `gc`.`tpp`,
                        `gmm`.`maturity`,
                        `gmm`.`maturity_acres`
                      FROM `grid_classification` `gc`
                    INNER JOIN `grid_market_maturity` `gmm`
                    ON `gc`.`place_id` = `gmm`.`place_id`
                      AND `gmm`.`ap_data_sector` = `gc`.`ap_data_sector`
                      AND `gmm`.`analysis_year` = `gc`.`analysis_year`
                    WHERE `gc`.`ap_data_sector` = '{2}'
                      AND `gc`.`analysis_year` = {1}
                      AND `gmm`.`ap_data_sector` = '{2}'
                      AND `gmm`.`analysis_year` = {1}
                      AND `gmm`.`maturity` = '{0}'
                ) `gc`
                ON `gp`.`place_id` = `gc`.`place_id`
                  AND `gp`.`maturity` = `gc`.`maturity`
                WHERE `gp`.`maturity` = '{0}'
                  AND `gp`.`analysis_year` = {1}
                  AND `gp`.`ap_data_sector` = '{2}'""".format(dku_dst_maturity, dku_dst_analysis_year, "SOY_BRAZIL_SUMMER"))

    return output_df


def regional_aggregate_query_sql_query_corn_brazil_summer_subset(spark, dku_dst_maturity, dku_dst_analysis_year):
    output_df = spark.sql("""
    SELECT 
                    `gp`.`place_id` AS `place_id`,
                    `gp`.`entry_id` AS `entry_id`,
                    `gp`.`maturity` AS `maturity`, 
                    CAST(`gp`.`cperf` AS boolean) AS `cperf`,
                    `gp`.`predict_ygsmn` AS `predict_ygsmn`, 
                    `gp`.`irrigation` AS `irrigation`, 
                    `gp`.`density` AS `density`, 
                    CAST(`gp`.`previous_crop_corn` AS boolean) AS `previous_crop_corn`,
                    `gp`.`ap_data_sector` AS `ap_data_sector`, 
                    `gp`.`analysis_year` AS `analysis_year`
                  FROM `grid_performance` `gp`
                WHERE `gp`.`maturity` = {0}
                  AND `gp`.`analysis_year` = {1}
                  AND `gp`.`ap_data_sector` = '{2}'""".format(dku_dst_maturity, dku_dst_analysis_year, "CORN_BRAZIL_SUMMER"))

    return output_df


def coord_bak_df_query(spark):
    output_df = spark.sql("""
    SELECT
        `loc_code`,
            AVG(`longitude_bak`) AS `x_longitude_bak`,
               AVG(`latitude_bak`) AS `y_latitude_bak`
             FROM (
               SELECT DISTINCT
                   `loc_code`,
                   CASE WHEN `country_code` IN ('US', 'CA') AND `longitude` > 0
                       THEN - `longitude`
                   ELSE `longitude`
                   END AS `longitude_bak`,
                   `latitude` AS `latitude_bak`
                 FROM `S3_CORN_NOAM_SUMR_2023_df`
               WHERE `longitude` IS NOT NULL
               AND `latitude` IS NOT NULL
               AND `ap_data_sector` = 'CORN_NOAM_SUMR'
           ) `coord_bak_raw`
           GROUP BY `loc_code`
    """)

    return output_df


def results_joined_query(spark):
    output_df = spark.sql("""
       SELECT
                CAST(`year` AS integer) AS `year`,
                `ap_data_sector`,
                COALESCE(`value_x`,
                         CAST(`maturity_group` AS string),
                         substr(`loc_selector`,0,1)) AS `maturity_group`,
                `experiment_id`,
                `trial_id`,
                `trial_status`,
                `trial_stage`,
                `loc_selector`,
                `plot_barcode`,
                `plant_date`,
                `harvest_date`,
                CASE WHEN `country_code` IN ('US', 'CA') AND `longitude` > 0
                    THEN COALESCE(-`longitude`, `x_longitude_bak`)
                ELSE COALESCE(`longitude`, `x_longitude_bak`)
                END AS `x_longitude`,
                COALESCE(`latitude`, `y_latitude_bak`) AS `y_latitude`,
                `state_province_code`,
                COALESCE(`irrigation`, 'NONE') AS `irrigation`,
                CASE
                    WHEN LOWER(`value_y`) = 'corn' THEN 2
                    WHEN `value_y` IS NOT NULL THEN 0
                    ELSE 1
                END AS `previous_crop_corn`,
                `material_id`,
                `be_bid`,
                COALESCE(`be_bid`, `be_bid`) AS `match_be_bid`,
                CASE
                    WHEN `fp_het_pool` != 'pool2' THEN COALESCE(`fp_be_bid`, `receiver_p`)
                    WHEN `mp_het_pool` = 'pool1' THEN COALESCE(`mp_be_bid`, `donor_p`)
                    ELSE `receiver_p`
                END AS `par_hp1_be_bid`,
                CASE
                    WHEN `fp_het_pool` != 'pool2' THEN `fp_sample`
                    WHEN `mp_het_pool` = 'pool1' THEN `mp_sample`
                    ELSE NULL
                END AS `par_hp1_sample`,
                CASE
                    WHEN `fp_het_pool` = 'pool2' THEN COALESCE(`fp_be_bid`, `receiver_p`)
                    WHEN `mp_het_pool` != 'pool1' THEN COALESCE(`mp_be_bid`, `donor_p`)
                    ELSE `donor_p`
                END AS `par_hp2_be_bid`,
                CASE
                    WHEN `fp_het_pool` = 'pool2' THEN `fp_sample`
                    WHEN `mp_het_pool` != 'pool1' THEN `mp_sample`
                    ELSE NULL
                END AS `par_hp2_sample`,
                `fp_het_group`,
                `mp_het_group`,
                CASE WHEN CAST(`cpifl` AS BOOLEAN) THEN 1 ELSE 0 END AS `cpifl`,
                COALESCE(CAST(`cperf` AS integer), CASE WHEN CAST(`cpifl` AS BOOLEAN) THEN 1 ELSE 0 END) AS `cperf`,
                `trait_measure_code` AS `trait`,
                `result_numeric_value` AS `result`
         FROM `results_joined` `results_joined`
        """)

    return output_df


def results_joined_query_test(spark):
    output_df = spark.sql("""
       SELECT *
         FROM `results_joined` `results_joined`
         LIMIT 10
        """)

    return output_df


def get_comparison_set_soy_spark(spark):
    output_df = spark.sql("""
        select
    `bb_mat_info`.`be_bid`,
    `mint_material`.`lbg_bid`,
    `material_info`.`sample_id`,
    COALESCE(`giga_class`.`cluster_idx`, 'None') AS `fada_group`,
    `agwc`.`sample_variant_count`,
    `cda`.`bebid_variant_count`,
    `cda2`.`cda_sample_variant_count`
from
    (
        SELECT
            `bb_mat`.`material_guid`,
            `bb_mat`.`be_bid`
        FROM
            `managed`.`rv_bb_material_sdl` `bb_mat`
        WHERE
            `bb_mat`.`crop_guid` = '6C9085C2-C442-48C4-ACA7-427C4760B642'
            AND `bb_mat`.`be_bid` LIKE 'ESY%'
    ) `bb_mat_info`
    inner join (
        SELECT
            `assay_size`.`sample_id`,
            `sample_api`.`germplasm_guid`,
            `sample_api`.`germplasm_id`,
            `assay_size`.`genotype_count`
        FROM
            `managed`.`rv_sample_api` `sample_api`
            INNER JOIN (
                SELECT
                    *
                FROM
                    (
                        SELECT
                            `sample_id` AS `sample_id`,
                            `techno_code` AS `techno_code`,
                            COUNT(`genotype`) AS `genotype_count`
                        FROM
                            `managed`.`rv_assay_genotype_with_crops`
                        WHERE
                            (
                                (`techno_code` = 'AX')
                                OR (`techno_code` = 'II')
                            )
                            AND (`crop_id` = 7)
                            AND `top_geno` IN ('A/A', 'C/C', 'G/G', 'T/T')
                        GROUP BY
                            `sample_id`,
                            `techno_code`
                    ) `unfiltered_query`
                WHERE
                    `genotype_count` > 99
            ) `assay_size` ON `sample_api`.`sample_code` = `assay_size`.`sample_id`
            AND `sample_api`.`crop_id` = 7
    ) `material_info` on `bb_mat_info`.`material_guid` = `material_info`.`germplasm_guid`
    LEFT JOIN (
        select
            `mint_material_trait`.`be_bid`,
            `mint_material_trait`.`lbg_bid`
        from
            `managed`.`rv_mint_material_trait_sp` `mint_material_trait`
    ) `mint_material` on `bb_mat_info`.`be_bid` = `mint_material`.`be_bid`
    LEFT JOIN (
        SELECT
            `giga_classification`.`be_bid`,
            `giga_classification`.`cluster_idx`
        from
            `managed`.`rv_giga_classification` `giga_classification`
    ) `giga_class` ON `bb_mat_info`.`be_bid` = `giga_class`.`be_bid`
    LEFT JOIN(
        SELECT
            `sample_id` AS `sample_id`,
            `techno_code` AS `techno_code`,
            COUNT(`genotype`) AS `sample_variant_count`
        FROM
            `managed`.`rv_assay_genotype_with_crops`
        WHERE
            (
                (`techno_code` = 'AX')
                OR (`techno_code` = 'II')
            )
            AND (CAST(`crop_id` AS integer) = 7)
            AND `top_geno` IN ('A/A', 'C/C', 'G/G', 'T/T')
        GROUP BY
            `sample_id`,
            `techno_code`
    ) `agwc` ON `material_info`.`sample_id` = `agwc`.`sample_id`
    LEFT JOIN(
        SELECT
            `be_bid`,
            COUNT(`variant_id`) AS `bebid_variant_count`
        FROM
            `managed`.`rv_cda_genotype`
        WHERE
            `genotype` IN ('A/A', 'C/C', 'G/G', 'T/T')
            AND LOWER(`species_name`) = 'soybean'
        GROUP BY
            `be_bid`
    ) `cda` ON `bb_mat_info`.`be_bid` = `cda`.`be_bid`
    LEFT JOIN(
        SELECT
            `distinct_samples`,
            COUNT(`variant_id`) AS `cda_sample_variant_count`
        FROM
            `managed`.`rv_cda_genotype`
        WHERE
            `genotype` IN ('A/A', 'C/C', 'G/G', 'T/T')
            AND LOWER(`species_name`) = 'soybean'
        GROUP BY
            `distinct_samples`
    ) `cda2` ON `material_info`.`sample_id` = `cda2`.`distinct_samples`
WHERE
    `agwc`.`sample_variant_count` IS NOT NULL
    OR `cda`.`bebid_variant_count` IS NOT NULL
""")
    return output_df

