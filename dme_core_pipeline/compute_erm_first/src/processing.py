import json
import os

import pandas as pd

from libs.dme_sql_queries import get_data_sector_config, query_check_entries, get_tops_checks
from libs.event_bridge.event import error_event
from libs.logger.cloudwatch_logger import CloudWatchLogger
from libs.metric_utils import create_check_df
from libs.processing.dme_sql_recipe import DmeSqlRecipe

TEST1 = '''SELECT 
    "rv_trait_sp"."code",
    "rv_trait_sp"."name",
    "rv_variety_hybrid_sp"."crop_guid",
    "rv_variety_hybrid_sp"."genetic_affiliation_guid",
    AVG("rv_variety_hybrid_trait_sp"."number_value") AS "number_value"
  FROM "managed"."rv_variety_hybrid_trait_sp"
INNER JOIN (
    SELECT 
        "trait_guid",
        "code",
        "name"
      FROM "managed"."rv_trait_sp" "rv_trait_sp"
      WHERE LOWER("descr") LIKE '%maturity%' 
        AND  LOWER("descr") NOT LIKE '%gene%'
        AND  LOWER("descr") NOT LIKE '%weight%'
        AND  (LOWER("descr") NOT LIKE '%days%' OR '${DKU_DST_ap_data_sector}' LIKE 'CORN%')
    ) "rv_trait_sp"
  ON "rv_variety_hybrid_trait_sp"."trait_guid" = "rv_trait_sp"."trait_guid"
INNER JOIN "managed"."rv_variety_hybrid" "rv_variety_hybrid_sp"
  ON "rv_variety_hybrid_trait_sp"."variety_hybrid_guid" = "rv_variety_hybrid_sp"."variety_hybrid_guid"
WHERE "rv_variety_hybrid_sp"."genetic_affiliation_guid" IS NOT NULL
  AND "rv_variety_hybrid_trait_sp"."number_value" > 60
  AND "rv_variety_hybrid_trait_sp"."number_value" < 200
GROUP BY
    "rv_trait_sp"."code",
    "rv_trait_sp"."name",
    "rv_variety_hybrid_sp"."crop_guid",
    "rv_variety_hybrid_sp"."genetic_affiliation_guid"'''


def compute_test1():
    dr = DmeSqlRecipe('variety_trait_data')
    dr.process(TEST1)


MATERIAL_TRAIT_DATA = '''SELECT DISTINCT
    "rv_ap_sector_experiment_config"."analysis_year" AS "analysis_year",
    "rv_ap_sector_experiment_config"."ap_data_sector_name" AS "ap_data_sector_name",
--    "rv_ap_sector_experiment_config"."experiment_id" AS "experiment_id",
    "rv_ap_sector_experiment_config"."decision_group" AS "decision_group",
    "rv_ap_sector_experiment_config"."decision_group_rm" AS "decision_group_rm",
    "rv_ap_sector_experiment_config"."stage" AS "stage",
    "rv_ap_sector_experiment_config"."combined" AS "combined",
    "rv_ap_sector_experiment_config"."technology" AS "technology",
    "rv_bb_experiment_trial_entry_sdl"."entry_id" AS "entry_id",
--    "rv_material_trait_sp"."alpha_value" AS "alpha_value",
     CASE 
        WHEN "rv_material_trait_sp"."number_value" < 15
            THEN 80 + "rv_material_trait_sp"."number_value"*5
        WHEN "rv_ap_sector_experiment_config"."ap_data_sector_name" LIKE 'SOY%' AND "rv_trait_sp"."code" = 'MRTYN'
            THEN "rv_material_trait_sp"."number_value" - 30
        ELSE "rv_material_trait_sp"."number_value"
    END AS "number_value",
    CASE 
        WHEN "rv_material_trait_sp"."number_value" < 15
            THEN 1
        ELSE 0
    END AS "rescaled_flag",
--    "rv_material_trait_sp"."date_value" AS "date_value",
--    "rv_material_trait_sp"."observation_date" AS "observation_date",
--    "rv_material_trait_sp"."inherited" AS "inherited",
--    "rv_material_trait_sp"."last_chg_date" AS "last_chg_date",
--    "rv_material_trait_sp"."rowguid" AS "rowguid",
--    "rv_material_trait_sp"."pegasys_update_date" AS "pegasys_update_date",
    "rv_trait_sp"."code" AS "code",
    "rv_trait_sp"."name" AS "name"
  FROM (
    SELECT 
        "trait_guid",
        "material_guid",
        AVG("number_value") AS "number_value"
      FROM "managed"."rv_material_trait_sp"
      GROUP BY
        "trait_guid",
        "material_guid"
  ) "rv_material_trait_sp"
  INNER JOIN (
    SELECT "rv_trait_sp".*
      FROM "managed"."rv_trait_sp" "rv_trait_sp"
      WHERE LOWER("descr") LIKE '%maturity%' 
        AND  LOWER("descr") NOT LIKE '%gene%'
        AND  LOWER("descr") NOT LIKE '%weight%'
        AND (LOWER("descr") NOT LIKE '%days%' OR '${DKU_DST_ap_data_sector}' LIKE 'CORN%')
    ) "rv_trait_sp"
    ON "rv_material_trait_sp"."trait_guid" = "rv_trait_sp"."trait_guid"
  INNER JOIN "managed"."rv_ap_data_sector_config" "rv_ap_data_sector_config"
    ON "rv_trait_sp"."crop_guid" = "rv_ap_data_sector_config"."spirit_crop_guid"
  INNER JOIN "managed"."rv_material" "rv_material_sp"
    ON "rv_material_trait_sp"."material_guid" = "rv_material_sp"."material_guid"
  INNER JOIN (
    SELECT DISTINCT
        "ap_data_sector",
        "experiment_id",
        "material_id",
        "entry_id"
      FROM "managed"."rv_trial_pheno_analytic_dataset"
      WHERE "ap_data_sector" = '${DKU_DST_ap_data_sector}'
  ) "rv_bb_experiment_trial_entry_sdl"
    ON "rv_material_sp"."material_id" = "rv_bb_experiment_trial_entry_sdl"."material_id"
  INNER JOIN "managed"."rv_ap_sector_experiment_config" "rv_ap_sector_experiment_config"
    ON "rv_bb_experiment_trial_entry_sdl"."experiment_id" = "rv_ap_sector_experiment_config"."experiment_id"
  WHERE "rv_ap_sector_experiment_config"."ap_data_sector_name" = '${DKU_DST_ap_data_sector}'
    AND "rv_ap_sector_experiment_config"."analysis_year" IN (${DKU_DST_analysis_year})
    AND "rv_material_trait_sp"."number_value" < 200
    AND ("rv_material_trait_sp"."number_value" <15 OR "rv_material_trait_sp"."number_value" > 65)'''


def compute_material_trait_data():
    dr = DmeSqlRecipe('material_trait_data')
    dr.process(MATERIAL_TRAIT_DATA)


AP_SECTOR_EXPERIMENT_CONFIG = '''SELECT DISTINCT
    CAST("analysis_year" AS integer) AS "analysis_year",
    "ap_data_sector_id",
    "ap_data_sector_name" AS "ap_data_sector",
    "experiment_id",
    "decision_group",
    "decision_group_rm",
    "stage",
    COALESCE("technology",'all') AS "technology"
  FROM "managed"."rv_ap_sector_experiment_config"
WHERE "ap_data_sector_name" = '${DKU_DST_ap_data_sector}'
    AND "analysis_year" IN (${DKU_DST_analysis_year})
'''


def compute_ap_sector_experiment_config():
    dr = DmeSqlRecipe('ap_sector_experiment_config')
    dr.process(AP_SECTOR_EXPERIMENT_CONFIG)


VARIETY_ENTRY_DATA = '''SELECT DISTINCT
    "rv_ap_sector_experiment_config"."analysis_year" AS "analysis_year",
    "rv_ap_sector_experiment_config"."ap_data_sector_name" AS "ap_data_sector_name",
    "rv_ap_sector_experiment_config"."decision_group" AS "decision_group",
    "rv_ap_sector_experiment_config"."decision_group_rm" AS "decision_group_rm",
    "rv_ap_sector_experiment_config"."stage" AS "stage",
    "rv_ap_sector_experiment_config"."combined" AS "combined",
    "rv_ap_sector_experiment_config"."technology" AS "technology",
    "rv_bb_experiment_trial_entry_sdl"."entry_id" AS "entry_id",
    "rv_material_sp"."genetic_affiliation_guid",
    "rv_material_sp"."crop_guid"
  FROM "managed"."rv_material" "rv_material_sp"
  INNER JOIN "managed"."rv_ap_data_sector_config" "rv_ap_data_sector_config"
    ON "rv_material_sp"."crop_guid" = "rv_ap_data_sector_config"."spirit_crop_guid"
  INNER JOIN (
    SELECT DISTINCT
        "ap_data_sector",
        "experiment_id",
        "material_id",
        "entry_id"
      FROM "managed"."rv_trial_pheno_analytic_dataset"
      WHERE "ap_data_sector" = '${DKU_DST_ap_data_sector}'
  ) "rv_bb_experiment_trial_entry_sdl"
    ON "rv_material_sp"."material_id" = "rv_bb_experiment_trial_entry_sdl"."material_id"
  INNER JOIN "managed"."rv_ap_sector_experiment_config" "rv_ap_sector_experiment_config"
    ON "rv_bb_experiment_trial_entry_sdl"."experiment_id" = "rv_ap_sector_experiment_config"."experiment_id"
  WHERE "rv_ap_sector_experiment_config"."ap_data_sector_name" = '${DKU_DST_ap_data_sector}'
    AND "rv_ap_sector_experiment_config"."analysis_year" IN (${DKU_DST_analysis_year})'''


def compute_variety_entry_data():
    dr = DmeSqlRecipe('variety_entry_data')
    dr.process(VARIETY_ENTRY_DATA)


TRIAL_PHENO_DATA = '''WITH "entry_list" AS (
    -- entries that are to be analyzed
    SELECT
        "rv_trial_pheno_analytic_dataset"."ap_data_sector" AS "ap_data_sector",
        CAST("rv_ap_sector_experiment_config"."analysis_year" AS integer) AS "analysis_year",
        "rv_trial_pheno_analytic_dataset"."entry_id",
        MAX("rv_trial_pheno_analytic_dataset"."trial_stage") AS "max_stage",
        1 AS "analysis_target"
    FROM "managed"."rv_ap_sector_experiment_config" "rv_ap_sector_experiment_config"
    INNER JOIN "managed"."rv_trial_pheno_analytic_dataset" "rv_trial_pheno_analytic_dataset"
        ON "rv_ap_sector_experiment_config"."experiment_id" = "rv_trial_pheno_analytic_dataset"."experiment_id"
        AND "rv_ap_sector_experiment_config"."ap_data_sector_name" = "rv_trial_pheno_analytic_dataset"."ap_data_sector"
        AND "rv_trial_pheno_analytic_dataset"."entry_id" IS NOT NULL
    WHERE "rv_ap_sector_experiment_config"."ap_data_sector_name" = '${DKU_DST_ap_data_sector}'
        AND CAST("rv_ap_sector_experiment_config"."analysis_year" AS integer) IN  (${DKU_DST_analysis_year})
        AND "rv_trial_pheno_analytic_dataset"."ap_data_sector" = '${DKU_DST_ap_data_sector}'
    GROUP BY
        "rv_trial_pheno_analytic_dataset"."ap_data_sector",
        CAST("rv_ap_sector_experiment_config"."analysis_year" AS integer),
        "rv_trial_pheno_analytic_dataset"."entry_id"
),
"feature_export_ms" AS (
    -- breakout_level information
    SELECT 
        "rv_feature_export"."market_name" AS "ap_data_sector",
        "rv_feature_export"."source_id",
        "rv_feature_export"."feature_level",
        REPLACE(LOWER("rv_feature_export"."value"), ' ', '_') AS "market_seg"
    FROM "managed"."rv_feature_export" "rv_feature_export"
        WHERE "rv_feature_export"."source" = 'SPIRIT'
        AND "rv_feature_export"."value" != 'undefined'
        AND ("rv_feature_export"."feature_level" = 'TRIAL' OR "rv_feature_export"."feature_level" = 'LOCATION')
        AND LOWER("rv_feature_export"."feature_name") = COALESCE('${breakout_level}','market_segment')
)

SELECT
    "trial_entry_list"."ap_data_sector",
    "trial_entry_list"."analysis_year" AS "analysis_year",
    "trial_pheno_subset"."trial_id",
    "trial_pheno_subset"."entry_id",
    "trial_pheno_subset"."year" AS "year",
    "trial_pheno_subset"."experiment_id" AS "experiment_id",
    COALESCE("feature_export_et"."et_value",'undefined') AS "et_value",
    COALESCE("feature_export_ms_loc"."market_seg","feature_export_ms_trial"."market_seg",'all') AS "market_segment",
    "trial_pheno_subset"."plot_barcode" AS "plot_barcode",
    "trial_pheno_subset"."trait" AS "trait",
    "trial_pheno_subset"."loc_selector" AS "loc_selector",
    "trial_pheno_subset"."x_longitude" AS "x_longitude",
    "trial_pheno_subset"."y_latitude" AS "y_latitude",
    CASE 
        WHEN LOWER("trial_pheno_subset"."irrigation") = 'irr'
            THEN 'IRR'
        ELSE 'DRY'
    END AS "irrigation",
    "trial_pheno_subset"."maturity_group" AS "maturity_group",
    "trial_pheno_subset"."result_numeric_value" AS "result_numeric_value",
    COALESCE("entry_list2"."analysis_target",0) AS "analysis_target",
    "trial_pheno_subset"."yield_trait",
    "trial_pheno_subset"."dme_metric",
    "trial_pheno_subset"."dme_chkfl",
    "trial_pheno_subset"."dme_reg_x",
    "trial_pheno_subset"."dme_reg_y",
    "trial_pheno_subset"."dme_rm_est",
    "trial_pheno_subset"."dme_weighted_trait",
    COALESCE("rv_ap_all_yhat"."outlier_flag", NULL) AS "outlier_flag"
FROM (
    SELECT
        "entry_list"."ap_data_sector",
        "entry_list"."analysis_year",
        "rv_trial_pheno_analytic_dataset"."trial_id",
        MAX("entry_list"."max_stage") AS "max_stage"
    FROM "managed"."rv_trial_pheno_analytic_dataset" "rv_trial_pheno_analytic_dataset"
    INNER JOIN "entry_list" ON "rv_trial_pheno_analytic_dataset"."entry_id" = "entry_list"."entry_id"
        AND "entry_list"."ap_data_sector" = "rv_trial_pheno_analytic_dataset"."ap_data_sector"
    WHERE "rv_trial_pheno_analytic_dataset"."entry_id" IS NOT NULL
        AND "rv_trial_pheno_analytic_dataset"."ap_data_sector" = '${DKU_DST_ap_data_sector}'
    GROUP BY
        "entry_list"."ap_data_sector",
        "entry_list"."analysis_year",
        "rv_trial_pheno_analytic_dataset"."trial_id"
) "trial_entry_list"
INNER JOIN (
    SELECT
        "rv_trial_pheno_analytic_dataset"."ap_data_sector" AS "ap_data_sector",
        CAST("rv_ap_sector_trait_config"."analysis_year" AS integer) AS "analysis_year",
        CAST("rv_trial_pheno_analytic_dataset"."year" AS integer) AS "year",
        "rv_trial_pheno_analytic_dataset"."trial_rm" AS "trial_rm",
        "rv_trial_pheno_analytic_dataset"."experiment_id" AS "experiment_id",
        "rv_trial_pheno_analytic_dataset"."plot_barcode" AS "plot_barcode",
        "rv_trial_pheno_analytic_dataset"."entry_id" AS "entry_id",
        "rv_trial_pheno_analytic_dataset"."be_bid" AS "be_bid",
        "rv_trial_pheno_analytic_dataset"."trait_measure_code" AS "trait",
        "rv_trial_pheno_analytic_dataset"."trial_stage" AS "trial_stage",
        "rv_trial_pheno_analytic_dataset"."loc_selector" AS "loc_selector",
        "rv_trial_pheno_analytic_dataset"."trial_id" AS "trial_id",
        "rv_trial_pheno_analytic_dataset"."x_longitude" AS "x_longitude",
        "rv_trial_pheno_analytic_dataset"."y_latitude" AS "y_latitude",
        "rv_trial_pheno_analytic_dataset"."irrigation" AS "irrigation",
        "rv_trial_pheno_analytic_dataset"."maturity_group" AS "maturity_group",
        "rv_trial_pheno_analytic_dataset"."result_numeric_value" AS "result_numeric_value",
        "rv_ap_sector_trait_config"."yield_trait" AS "yield_trait",
        "rv_ap_sector_trait_config"."dme_metric" AS "dme_metric",
        "rv_ap_sector_trait_config"."dme_chkfl" AS "dme_chkfl",
        "rv_ap_sector_trait_config"."dme_reg_x" AS "dme_reg_x",
        "rv_ap_sector_trait_config"."dme_reg_y" AS "dme_reg_y",
        "rv_ap_sector_trait_config"."dme_rm_est" AS "dme_rm_est",
        "rv_ap_sector_trait_config"."dme_weighted_trait" AS "dme_weighted_trait"
    FROM (
        SELECT
            "rv_trial_pheno_analytic_dataset"."ap_data_sector" AS "ap_data_sector",
            CAST("rv_trial_pheno_analytic_dataset"."year" AS integer) AS "year",
            AVG(COALESCE("rv_trial_pheno_analytic_dataset"."maturity_group",0)) AS "trial_rm",
            "rv_trial_pheno_analytic_dataset"."experiment_id" AS "experiment_id",
            "rv_trial_pheno_analytic_dataset"."plot_barcode" AS "plot_barcode",
            "rv_trial_pheno_analytic_dataset"."entry_id" AS "entry_id",
            "rv_trial_pheno_analytic_dataset"."be_bid" AS "be_bid",
            "rv_trial_pheno_analytic_dataset"."trait_measure_code" AS "trait_measure_code",
            "rv_trial_pheno_analytic_dataset"."trial_stage" AS "trial_stage",
            "rv_trial_pheno_analytic_dataset"."loc_selector" AS "loc_selector",
            "rv_trial_pheno_analytic_dataset"."trial_id" AS "trial_id",
            AVG("rv_trial_pheno_analytic_dataset"."x_longitude") AS "x_longitude",
            AVG("rv_trial_pheno_analytic_dataset"."y_latitude") AS "y_latitude",
            "rv_trial_pheno_analytic_dataset"."irrigation" AS "irrigation",
            AVG("rv_trial_pheno_analytic_dataset"."maturity_group") AS "maturity_group",
            AVG("rv_trial_pheno_analytic_dataset"."result_numeric_value") AS "result_numeric_value"
          FROM "managed"."rv_trial_pheno_analytic_dataset" "rv_trial_pheno_analytic_dataset"
        WHERE "rv_trial_pheno_analytic_dataset"."result_numeric_value" IS NOT NULL
            AND "rv_trial_pheno_analytic_dataset"."entry_id" IS NOT NULL
            AND "rv_trial_pheno_analytic_dataset"."ap_data_sector" = '${DKU_DST_ap_data_sector}'
            AND NOT CAST("rv_trial_pheno_analytic_dataset"."tr_exclude" AS boolean)
            AND NOT CAST("rv_trial_pheno_analytic_dataset"."psp_exclude" AS boolean)
            AND NOT CAST("rv_trial_pheno_analytic_dataset"."pr_exclude" AS boolean)
        GROUP BY
            "ap_data_sector",
            "year",
            "experiment_id",
            "plot_barcode",
            "entry_id",
            "be_bid",
            "trait_measure_code",
            "trial_stage",
            "loc_selector",
            "trial_id",
            "irrigation"
    ) "rv_trial_pheno_analytic_dataset"
    INNER JOIN (
        SELECT DISTINCT
            CAST("analysis_year" AS integer) AS "analysis_year",
            "ap_data_sector_name",
            "trait",
            CAST(CASE WHEN "yield_trait" = 0 or "yield_trait" = 1 THEN "yield_trait" ELSE 0 END AS integer) AS "yield_trait",
            "dme_metric",
            "dme_chkfl",
            "dme_reg_x",
            "dme_reg_y",
            "dme_rm_est",
            "dme_weighted_trait"
        FROM "managed"."rv_ap_sector_trait_config" "rv_ap_sector_trait_config"
        WHERE "rv_ap_sector_trait_config"."ap_data_sector_name" = '${DKU_DST_ap_data_sector}'
        AND CAST("rv_ap_sector_trait_config"."analysis_year" AS integer) IN  (${DKU_DST_analysis_year})
            AND ("dme_metric" != 'na' OR "dme_reg_x" = true OR "dme_reg_y" = true OR "dme_rm_est" != 0 OR "dme_weighted_trait" != 0)
    ) "rv_ap_sector_trait_config"
    ON "rv_trial_pheno_analytic_dataset"."ap_data_sector" = "rv_ap_sector_trait_config"."ap_data_sector_name"
        AND "rv_trial_pheno_analytic_dataset"."trait_measure_code" = "rv_ap_sector_trait_config"."trait"
        AND ("rv_trial_pheno_analytic_dataset"."year"> ("rv_ap_sector_trait_config"."analysis_year"-3)
            AND "rv_trial_pheno_analytic_dataset"."year" <= "rv_ap_sector_trait_config"."analysis_year")
) "trial_pheno_subset"
ON "trial_pheno_subset"."trial_id" = "trial_entry_list"."trial_id"
    AND "trial_pheno_subset"."ap_data_sector" = "trial_entry_list"."ap_data_sector"
    AND "trial_pheno_subset"."analysis_year" = "trial_entry_list"."analysis_year"
--    AND ("trial_pheno_subset"."year" = "trial_entry_list"."analysis_year" OR "trial_entry_list"."max_stage" >= 4)
LEFT JOIN  "entry_list" "entry_list2"
ON "trial_pheno_subset"."entry_id" = "entry_list2"."entry_id"
    AND "trial_pheno_subset"."ap_data_sector" = "entry_list2"."ap_data_sector"
    AND "trial_pheno_subset"."analysis_year" = "entry_list2"."analysis_year"
LEFT JOIN (
    SELECT DISTINCT
      "rv_feature_export"."source_id" AS "trial_id",
      "rv_feature_export"."value" AS "et_value"
    FROM "managed"."rv_feature_export"
    WHERE "rv_feature_export"."source" = 'SPIRIT' AND
        "rv_feature_export"."value" != 'undefined' AND
        (
            ("rv_feature_export"."market_name" = 'CORN_NA_SUMMER' AND
                 ("rv_feature_export"."feature_name" = 'EnvironmentType_level2' AND
                  "rv_feature_export"."value" LIKE 'ET08%')
             OR
                ("rv_feature_export"."feature_name" = 'EnvironmentType_level1' AND
                 "rv_feature_export"."value" != 'ET08')
            )
         OR
            ("rv_feature_export"."market_name" != 'CORN_NA_SUMMER' AND
             "rv_feature_export"."feature_name" = 'EnvironmentType_level1')
        )
) "feature_export_et"
ON "trial_pheno_subset"."trial_id" = "feature_export_et"."trial_id"
LEFT JOIN "feature_export_ms" "feature_export_ms_loc"
ON "trial_pheno_subset"."loc_selector" = "feature_export_ms_loc"."source_id"
    AND "trial_pheno_subset"."ap_data_sector" = "feature_export_ms_loc"."ap_data_sector"
    AND "feature_export_ms_loc"."feature_level" = 'LOCATION'
LEFT JOIN "feature_export_ms" "feature_export_ms_trial"
ON "trial_pheno_subset"."trial_id" = "feature_export_ms_trial"."source_id"
    AND "trial_pheno_subset"."ap_data_sector" = "feature_export_ms_trial"."ap_data_sector"
    AND "feature_export_ms_trial"."feature_level" = 'TRIAL'
LEFT JOIN(
    SELECT 
        "ap_data_sector",
        "plot_barcode",
        "trait",
        MAX(CASE WHEN "outlier_flag"=TRUE THEN 1 ELSE 0 END) AS "outlier_flag"
      FROM "advancement"."rv_ap_all_yhat"
      WHERE "ap_data_sector" = '${DKU_DST_ap_data_sector}'
        AND "source_year" IN (${DKU_DST_analysis_year})
    GROUP BY
        "ap_data_sector",
        "plot_barcode",
        "trait"
) "rv_ap_all_yhat"
ON "rv_ap_all_yhat"."ap_data_sector" = "trial_pheno_subset"."ap_data_sector"
    AND "rv_ap_all_yhat"."plot_barcode" = "trial_pheno_subset"."plot_barcode"
    AND "rv_ap_all_yhat"."trait" = "trial_pheno_subset"."trait"
WHERE ("trial_pheno_subset"."dme_reg_x" = true
    OR "trial_pheno_subset"."dme_reg_y" = true
    OR "analysis_target" = 1)
    AND ("outlier_flag" = 0 or "outlier_flag" IS NULL)
--LIMIT 1000000'''


def compute_trial_pheno_data():
    dr = DmeSqlRecipe('trial_pheno_data')
    dr.process(TRIAL_PHENO_DATA)


PVS_DATA = '''SELECT DISTINCT 
    "rv_ap_all_pvs"."ap_data_sector" AS "ap_data_sector",
    "rv_ap_all_pvs"."analysis_type" AS "analysis_type",
    "rv_ap_sector_trait_config"."analysis_year" AS "analysis_year",
    "rv_ap_all_pvs"."source_id" AS "source_id",
    LOWER("rv_ap_all_pvs"."market_seg") AS "market_seg",
    "rv_ap_all_pvs"."pipeline_runid" AS "pipeline_runid",
    "rv_ap_all_pvs"."trait" AS "trait",
    "rv_ap_all_pvs"."model" AS "model",
    "rv_ap_all_pvs"."entry_identifier" AS "entry_identifier",
    CASE 
        WHEN "rv_ap_all_pvs"."material_type" LIKE '%ale'
        THEN LOWER("rv_ap_all_pvs"."material_type")
        ELSE 'entry'
        END AS "material_type",
    "rv_ap_all_pvs"."count" AS "count",
    "rv_ap_all_pvs"."loc" AS "loc",
    "rv_ap_all_pvs"."prediction" AS "prediction",
    "rv_ap_all_pvs"."stderr" AS "stderr",
    "rv_ap_sector_trait_config"."metric" AS "metric",
    "rv_ap_sector_trait_config"."dme_chkfl" AS "dme_chkfl",
    "rv_ap_sector_trait_config"."dme_reg_x" AS "dme_reg_x",
    "rv_ap_sector_trait_config"."dme_reg_y" AS "dme_reg_y",
    "rv_ap_sector_trait_config"."dme_rm_est" AS "dme_rm_est"
  FROM (
    SELECT "rv_ap_all_pvs".*
      FROM "advancement"."rv_ap_all_pvs" "rv_ap_all_pvs"
      WHERE (((LOWER("analysis_type") = 'multiyear') AND (LOWER("loc") != 'all')) 
             OR ((LOWER("loc") = 'all') AND (LOWER("analysis_type") != 'multiyear')))
          AND "ap_data_sector" = '${DKU_DST_ap_data_sector}'
          AND CAST("source_year" AS integer) IN  (${DKU_DST_analysis_year}) 
          AND "analysis_type" = '${DKU_DST_analysis_type}'
    ) "rv_ap_all_pvs"
  INNER JOIN (
    SELECT DISTINCT
        CAST("analysis_year" AS integer) AS "analysis_year",
        "ap_data_sector_id" AS "ap_data_sector_id",
        "ap_data_sector_name" AS "ap_data_sector_name",
        "trait" AS "trait",
        "yield_trait" AS "yield_trait",
        "level" AS "level",
        "dme_metric" AS "dme_metric",
        "dme_chkfl" AS "dme_chkfl",
        "dme_reg_x" AS "dme_reg_x",
        "dme_reg_y" AS "dme_reg_y",
        NULL AS "dme_qualifier",
        "dme_rm_est" AS "dme_rm_est",
        "dme_weighted_trait" AS "dme_weighted_trait",
        CASE 
            WHEN "dme_metric" = 'performance'
              THEN 'performance'
            WHEN "dme_metric" IN ('agronomic', 'disease', 'risk') 
              THEN 'risk'
            WHEN "dme_metric" = 'qualifier'
              THEN 'qualifier'
            WHEN "dme_metric" = 'maturity'
              THEN 'maturity'
            ELSE 'other'
      END AS "metric"
      FROM "managed"."rv_ap_sector_trait_config" "rv_ap_sector_trait_config"
      WHERE "ap_data_sector_name" = '${DKU_DST_ap_data_sector}'
          AND CAST("analysis_year" AS integer) IN (${DKU_DST_analysis_year})
          AND ("dme_metric" != 'na' OR "dme_reg_x" = true OR "dme_reg_y" = true OR "dme_rm_est" != 0 OR "dme_weighted_trait" != 0)
    ) "rv_ap_sector_trait_config"
    ON (LOWER("rv_ap_all_pvs"."ap_data_sector") = LOWER("rv_ap_sector_trait_config"."ap_data_sector_name"))
      AND (LOWER("rv_ap_all_pvs"."trait") = LOWER("rv_ap_sector_trait_config"."trait"))
      AND (CAST("rv_ap_all_pvs"."source_year" AS integer) = "rv_ap_sector_trait_config"."analysis_year")'''


def compute_pvs_data():
    dr = DmeSqlRecipe('pvs_data')
    dr.process(PVS_DATA)


if __name__ == '__main__':
    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        ap_data_sector = data['ap_data_sector']
        analysis_year = data['analysis_year']
        analysis_type = data['analysis_type']
        current_source_ids = data['source_ids']
        pipeline_runid = data['target_pipeline_runid']
        breakout_level = data['breakout_level']
        logger = CloudWatchLogger.get_logger()
        try:
            compute_test1()
            compute_material_trait_data()
            compute_ap_sector_experiment_config()
            compute_variety_entry_data()
            compute_trial_pheno_data()
            compute_pvs_data()
        except Exception as e:
            logger.error(e)
            error_event(ap_data_sector, analysis_year, pipeline_runid, str(e))
            raise e
