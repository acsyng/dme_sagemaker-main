from libs.processing.dme_sql_recipe import DmeSqlRecipe

SQL_TEMPLATE = '''SELECT DISTINCT
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
  FROM (
    SELECT 
        "rv_material_sp"."crop_guid",
        "rv_material_sp"."genetic_affiliation_guid",
        "rv_material_sp"."material_id"
      FROM "managed"."rv_material_sp"
  ) "rv_material_sp"
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
    AND "rv_ap_sector_experiment_config"."analysis_year" = ${DKU_DST_analysis_year}'''


if __name__ == '__main__':
    dr = DmeSqlRecipe('variety_entry_data')
    dr.process(SQL_TEMPLATE)
