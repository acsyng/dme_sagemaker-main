from libs.processing.dme_sql_recipe import DmeSqlRecipe

SQL_TEMPLATE = '''SELECT "ap_data_sector_name",
    "year",
    "trial_stage",
    "be_bid",
    MAX("selection_remark") as "selection_remark"

FROM "advancement"."rv_early_stg_data_corn_na_summer"
WHERE "year"=${DKU_DST_analysis_year} AND "ap_data_sector_name"='${DKU_DST_ap_data_sector}'

GROUP BY "ap_data_sector_name","year","trial_stage","be_bid"'''


if __name__ == '__main__':
    dr = DmeSqlRecipe('rv_early_stg_selection_remark')
    dr.process(SQL_TEMPLATE)
