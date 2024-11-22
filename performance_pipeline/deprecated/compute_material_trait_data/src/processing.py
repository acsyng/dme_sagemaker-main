from libs.processing.dme_sql_recipe import DmeSqlRecipe

SQL_TEMPLATE = '''SELECT DISTINCT
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
  INNER JOIN "managed"."rv_material_sp" "rv_material_sp"
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
    AND "rv_ap_sector_experiment_config"."analysis_year" = ${DKU_DST_analysis_year}
    AND "rv_material_trait_sp"."number_value" < 200
    AND ("rv_material_trait_sp"."number_value" <15 OR "rv_material_trait_sp"."number_value" > 65)'''


if __name__ == '__main__':
    dr = DmeSqlRecipe('material_trait_data')
    dr.process(SQL_TEMPLATE)
