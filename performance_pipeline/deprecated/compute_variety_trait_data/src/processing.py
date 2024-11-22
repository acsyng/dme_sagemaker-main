from libs.processing.dme_sql_recipe import DmeSqlRecipe

SQL_TEMPLATE = '''SELECT 
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
INNER JOIN "managed"."rv_variety_hybrid_sp"
  ON "rv_variety_hybrid_trait_sp"."variety_hybrid_guid" = "rv_variety_hybrid_sp"."variety_hybrid_guid"
WHERE "rv_variety_hybrid_sp"."genetic_affiliation_guid" IS NOT NULL
  AND "rv_variety_hybrid_trait_sp"."number_value" > 60
  AND "rv_variety_hybrid_trait_sp"."number_value" < 200
GROUP BY
    "rv_trait_sp"."code",
    "rv_trait_sp"."name",
    "rv_variety_hybrid_sp"."crop_guid",
    "rv_variety_hybrid_sp"."genetic_affiliation_guid"'''


if __name__ == '__main__':
    dr = DmeSqlRecipe('variety_trait_data')
    dr.process(SQL_TEMPLATE)
