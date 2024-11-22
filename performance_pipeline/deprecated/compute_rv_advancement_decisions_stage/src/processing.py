from libs.processing.dme_sql_recipe import DmeSqlRecipe
# this recipe gets advance rm and be_bid for materials. Advance RM is used to
# check was_adv column, since some materials are tested in multiple RMs but
# only advanced in a single RM

SQL_TEMPLATE = '''WITH "rv_dec" as (
    SELECT "ap_data_sector","year","entry_id","current_stage","advance_stage","advance_rm","advance_market_segment"
    FROM "advancement"."rv_advancement_decisions"
    WHERE "year">=2019 -- No data for advanced_stage in 2019 or before 
        AND "current_stage">=1 AND "current_stage"<=4 
) 

SELECT "ap_data_sector","year","be_bid", -- get non-null versions of any duplicates
    "fp_be_bid", "mp_be_bid",
    "current_stage",
    MAX("advance_stage") as "advance_stage",
    MAX("advance_rm") as "advance_rm",
    MAX("advance_market_segment") as "advance_market_segment",
    MAX("was_adv") as "was_adv"

FROM (

        SELECT
            "rv_dec".*,
            "advancement"."rv_corn_material_tester"."be_bid",
            "advancement"."rv_corn_material_tester"."fp_be_bid",
            "advancement"."rv_corn_material_tester"."mp_be_bid",
            CASE 
                WHEN "rv_dec"."advance_stage" > "rv_dec"."current_stage"
                    AND "rv_dec"."advance_stage" < 7 AND "rv_dec"."current_stage" < 7 THEN 1
                ELSE 0
            END "was_adv"
        FROM "rv_dec"

        LEFT JOIN "advancement"."rv_corn_material_tester"
            ON "rv_dec"."entry_id"="advancement"."rv_corn_material_tester"."abbr_code"


    UNION 

        SELECT
            "rv_dec".*,
            "advancement"."rv_corn_material_tester"."be_bid",
            "advancement"."rv_corn_material_tester"."fp_be_bid",
            "advancement"."rv_corn_material_tester"."mp_be_bid",
            CASE 
                WHEN "rv_dec"."advance_stage" > "rv_dec"."current_stage"
                    AND "rv_dec"."advance_stage" < 7 AND "rv_dec"."current_stage" < 7 THEN 1
                ELSE 0
            END "was_adv"
        FROM "rv_dec"

        LEFT JOIN "advancement"."rv_corn_material_tester"
            ON "rv_dec"."entry_id"="advancement"."rv_corn_material_tester"."pedigree"

    UNION 

        SELECT
            "rv_dec".*,
            "advancement"."rv_corn_material_tester"."be_bid",
            "advancement"."rv_corn_material_tester"."fp_be_bid",
            "advancement"."rv_corn_material_tester"."mp_be_bid",
            CASE 
                WHEN "rv_dec"."advance_stage" > "rv_dec"."current_stage"
                    AND "rv_dec"."advance_stage" < 7 AND "rv_dec"."current_stage" < 7 THEN 1
                ELSE 0
            END "was_adv"
        FROM "rv_dec"

        LEFT JOIN "advancement"."rv_corn_material_tester"
            ON "rv_dec"."entry_id"="advancement"."rv_corn_material_tester"."gna_pedigree"
)

GROUP BY "ap_data_sector","year","be_bid","fp_be_bid","mp_be_bid","current_stage"'''


if __name__ == '__main__':
    dr = DmeSqlRecipe('rv_advancement_decisions_stage')
    dr.process(SQL_TEMPLATE)
