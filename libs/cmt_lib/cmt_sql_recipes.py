from libs.denodo.denodo_connection import DenodoConnection

############################################################
# get_geno_sample_list()
#
# get sample id, material_id, and number of SNPs in the sample
#
# ACTIVE 2024-03-13
############################################################
def get_geno_sample_list(crop_id=9):
    # crop id : 9 = corn
    query_str = """
        SELECT
          "assay_size"."sample_id",
          "sample_api"."germplasm_guid",
          "sample_api"."germplasm_id",
          "assay_size"."genotype_count"
        FROM "managed"."rv_sample_api" "sample_api"
        INNER JOIN (
            SELECT *
              FROM (
                SELECT 
                    "sample_id" AS "sample_id",
                    "techno_code" AS "techno_code",
                    COUNT("genotype") AS "genotype_count"
                  FROM "managed"."rv_assay_genotype_with_crops"
                WHERE "techno_code" IN ('AX', 'II', 'NEXAR') AND ("crop_id" =  {0} )
                  AND "top_geno" IN ('A/A', 'C/C', 'G/G', 'T/T')
                GROUP BY "sample_id", "techno_code"
                ) "unfiltered_query"
              WHERE "genotype_count" > 99
        ) "assay_size"
        ON "sample_api"."sample_code" = "assay_size"."sample_id"
              AND "sample_api"."crop_id" =  {0}
    """.format(crop_id)

    with DenodoConnection() as dc:
        df = dc.get_data(query_str)

    return df

############################################################
# get_material_info_sp
#
# Query rv_material, rv_material_trait, and rv_be_bid_ancestry_laas 
# to retrieve some core material properties
#
# ACTIVE 2024-03-13
############################################################
def get_material_info_sp(crop_guid='B79D32C9-7850-41D0-BE44-894EC95AB285'):
    # default crop guid is for corn (B79D32C9-7850-41D0-BE44-894EC95AB285)
    # because of .format, I replaced all {*} in regex with {{*}} where * is some regex related input
    # the extra set of curly braces tells .format to not replace with an input
    query_str = """
        SELECT
            "rv_material"."material_guid" AS "material_guid",
            "rv_material"."material_id" AS "material_id",
            CASE 
                WHEN LOWER("rv_material"."pre_line_code") = 'unknown' OR LOWER("rv_material"."pre_line_code") LIKE '%n/a'
                    THEN ''
                ELSE "rv_material"."pre_line_code"
            END AS "pre_line_code",
            "rv_material"."abbr_code" AS "abbr_code",
            CASE 
                WHEN LOWER("rv_material"."highname") = 'unknown' OR LOWER("rv_material"."highname") LIKE '%n/a'
                    THEN ''
                ELSE "rv_material"."highname"
            END AS "highname",
            "rv_material"."generation_code" AS "generation_code",
            "rv_material"."gmo_lid" AS "gmo_lid",
            "rv_material_trait"."cgenes" AS "cgenes",
            CASE 
                WHEN LOWER("rv_material"."pedigree") LIKE '%unknown%' THEN ''
                ELSE "rv_material"."pedigree" 
            END AS "pedigree",
            "rv_material"."pollination_type_lid" AS "pollination_type_lid",
            "rv_material"."material_type_lid" AS "material_type_lid",
            "rv_material_trait"."batch_bid" AS "batch_bid",
            "rv_material_trait"."be_bid" AS "be_bid",
            "rv_material_trait"."lbg_bid" AS "lbg_bid",
            "rv_be_bid_ancestry_laas"."receiver_p" AS "female_be_bid",
            "rv_be_bid_ancestry_laas"."donor_p" AS "male_be_bid",
            "rv_be_bid_ancestry_laas"."donor_donor_gp" AS "mp_mp_be_bid",
            "rv_be_bid_ancestry_laas"."donor_receiver_gp" AS "mp_fp_be_bid",
            "rv_be_bid_ancestry_laas"."receiver_donor_gp" AS "fp_mp_be_bid",
            "rv_be_bid_ancestry_laas"."receiver_receiver_gp" AS "fp_fp_be_bid",
            "rv_material"."line_guid" AS "line_guid",
            EXTRACT( YEAR FROM "rv_material"."create_date") AS "create_year",
            CASE 
                    WHEN "rv_material"."encumbrance_nafta_lid" != '' THEN 'NAFTA'
                    WHEN "rv_material"."encumbrance_latam_lid" != '' THEN 'LATAM'
                    WHEN "rv_material"."encumbrance_eame_lid" != '' THEN 'EAME'
                    WHEN "rv_material"."encumbrance_apac_lid" != '' THEN 'APAC'
                    ELSE ''
            END AS "region_lid",
            "rv_material"."stack_guid" AS "stack_guid"
          FROM "managed"."rv_material_trait" "rv_material_trait"
        INNER JOIN "managed"."rv_be_bid_ancestry_laas"  "rv_be_bid_ancestry_laas"
            ON "rv_be_bid_ancestry_laas"."be_bid" = "rv_material_trait"."be_bid"
        INNER JOIN "managed"."rv_material" "rv_material"
          ON "rv_material_trait"."material_guid" = "rv_material"."material_guid" 
        WHERE ("rv_be_bid_ancestry_laas"."be_bid" LIKE '_MF%')
            AND (("rv_be_bid_ancestry_laas"."receiver_p" != "rv_be_bid_ancestry_laas"."donor_p")
            OR ("rv_be_bid_ancestry_laas"."receiver_p" IS NULL AND "rv_be_bid_ancestry_laas"."donor_p" IS NULL)
            OR ("rv_be_bid_ancestry_laas"."receiver_p" IS NOT NULL AND "rv_be_bid_ancestry_laas"."donor_p" IS NULL)
            OR ("rv_be_bid_ancestry_laas"."receiver_p" IS NULL AND "rv_be_bid_ancestry_laas"."donor_p" IS NOT NULL))
            AND ("rv_material_trait"."be_bid" LIKE '_MF%')
            AND ("rv_material"."crop_guid" = 'B79D32C9-7850-41D0-BE44-894EC95AB285')
        """

    with DenodoConnection() as dc:
        df = dc.get_data(query_str)

    return df


############################################################
# get_line_info()
#
# Query rv_line for line & chassis properties. To be joined with output from get_material_info_sp()
#
# ACTIVE 2024-03-13
############################################################
def get_line_info():
    with DenodoConnection() as dc:
        df = dc.get_data("""
        SELECT
            "rv_line"."line_guid",
            "rv_line"."line_code",
            REGEXP("rv_line"."line_code",'^([A-Z]{2,4}\d{3,5}).*','$1') AS "chassis",
            "rv_line"."primary_genetic_family_lid",
            "rv_line"."secondary_genetic_family_lid",
            COALESCE(
                "rv_line"."region_lid",
                CASE 
                    WHEN "rv_line"."encumbrance_nafta_lid" != '' THEN 'NAFTA'
                    WHEN "rv_line"."encumbrance_latam_lid" != '' THEN 'LATAM'
                    WHEN "rv_line"."encumbrance_eame_lid" != '' THEN 'EAME'
                    WHEN "rv_line"."encumbrance_apac_lid" != '' THEN 'APAC'
                    ELSE ''
                END
            ) AS "region_lid_line",
            COALESCE(CAST(ROUND("rv_line"."prod_year") AS integer), CAST(ROUND("rv_line"."coding_decision_year") AS integer), EXTRACT( YEAR FROM "rv_line"."create_date")) AS "line_create_year",
            "chassis_sp"."create_year" AS "chassis_year"
        FROM "managed"."rv_line" "rv_line"
        LEFT JOIN (
            SELECT
                REGEXP("line_code",'^([A-Z]{2,4}\d{3,5}).*','$1') AS "chassis",
                MIN(COALESCE(CAST(ROUND("prod_year") AS integer), CAST(ROUND("coding_decision_year") AS integer), EXTRACT( YEAR FROM "create_date"))) AS "create_year"
                FROM "managed"."rv_line"
            WHERE "crop_guid" = 'B79D32C9-7850-41D0-BE44-894EC95AB285'
                AND "line_code" REGEXP_LIKE '^[A-Z]{2,4}\d{3,5}.*'
            GROUP BY "chassis"
        ) "chassis_sp"
        ON REGEXP("rv_line"."line_code",'^([A-Z]{2,4}\d{3,5}).*','$1') = "chassis_sp"."chassis"
        WHERE "crop_guid" = 'B79D32C9-7850-41D0-BE44-894EC95AB285'
        """)

    return df

############################################################
# get_check_info
#
# Retrieves cpi and line-to-beat flags for SPIRIT trials and returns in a pandas df
#
# ACTIVE 2024-03-13
############################################################
def get_check_info():

    with DenodoConnection() as dc:
        df = dc.get_data("""
        SELECT
            "planted_material_guid",
            MAX("cpi") AS "cpi",
            MAX("check_line_to_beat_f") AS "check_line_to_beat_f",
            MAX("check_line_to_beat_m") AS "check_line_to_beat_m"
            FROM "managed"."rv_bb_experiment_trial_entry_sdl"
        WHERE LOWER("crp_name") = 'corn'
        GROUP BY "planted_material_guid"
        """)

    return df


############################################################
# get_check_info_tops
#
# Retrieves cpi for 2023 Corn NA trials and returns in a pandas df
#
# ACTIVE 2024-03-13
############################################################
def get_check_info_tops():

    with DenodoConnection() as dc:
        df = dc.get_data("""
         SELECT material_be_bid AS be_bid, MAX( CASE WHEN check_flag THEN 1 ELSE 0 END) as cpi_tops
         FROM trialing_daas.rv_2023_na_experiment_germplasms_daas 
         GROUP BY material_be_bid
        """)

    return df

############################################################
# get_genetic_affiliation()
#
# Query rv_genetic_affiliation for family properties. To be joined with output from get_material_info_sp()
#
# ACTIVE 2024-03-13
############################################################
def get_genetic_affiliation():

    with DenodoConnection() as dc:
        df = dc.get_data("""
        SELECT
            mat.material_guid,
            CASE 
                WHEN LOWER(ga.pedigree_stem) = 'unknown' OR LOWER(ga.pedigree_stem) LIKE '%n/a'
                    THEN ''
                ELSE ga.pedigree_stem
            END AS gas_pedigree_stem,
            CASE 
                WHEN LOWER(ga.family) = 'unknown' 
                OR LOWER(ga.family) LIKE '%n/a' 
                OR LEN(ga.family) < 3
                OR INSTR(ga.family,'/') = 0
                    THEN ''
                ELSE ga.family
            END AS family
        FROM managed.rv_genetic_affiliation ga
        INNER JOIN managed.rv_material mat
        ON ga.genetic_affiliation_guid = mat.genetic_affiliation_guid
        WHERE mat.crop_guid = 'B79D32C9-7850-41D0-BE44-894EC95AB285'
        """)

    return df

############################################################
# get_rv_v_p_genetic_groups()
#
# Query FADA groups. To be used to determine heterotic pool.
#
# ACTIVE 2024-03-13
############################################################
def get_rv_v_p_genetic_groups():
    query_str = """
        SELECT "nbr_spid", 
            "nbr_code", 
            "nbr_type", 
            "membership_percentage",
            "genetic_group_name"
        FROM "managed"."rv_v_p_genetic_group_classif_fada"
    """

    with DenodoConnection() as dc:
        df = dc.get_data(query_str)

    return df
