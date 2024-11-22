from libs.processing.dme_sql_recipe import DmeSqlRecipe

SQL_TEMPLATE = '''SELECT DISTINCT 
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
          AND CAST("source_year" AS integer) =  ${DKU_DST_analysis_year} 
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
          AND CAST("analysis_year" AS integer) =  ${DKU_DST_analysis_year}
          AND ("dme_metric" != 'na' OR "dme_reg_x" = true OR "dme_reg_y" = true OR "dme_rm_est" != 0 OR "dme_weighted_trait" != 0)
    ) "rv_ap_sector_trait_config"
    ON (LOWER("rv_ap_all_pvs"."ap_data_sector") = LOWER("rv_ap_sector_trait_config"."ap_data_sector_name"))
      AND (LOWER("rv_ap_all_pvs"."trait") = LOWER("rv_ap_sector_trait_config"."trait"))
      AND (CAST("rv_ap_all_pvs"."source_year" AS integer) = "rv_ap_sector_trait_config"."analysis_year")'''


if __name__ == '__main__':
    dr = DmeSqlRecipe('pvs_data')
    dr.process(SQL_TEMPLATE)
