import math

import pandas as pd

from libs.denodo.denodo_connection import DenodoConnection


def get_comparison_set_corn():
    with DenodoConnection() as dc:
        output_df = dc.get_data("""
    SELECT DISTINCT
        "mat"."be_bid",
        "mat"."lbg_bid",
        "mat"."sample_id",
        COALESCE("mat"."fada_group", 'None') AS "fada_group",
--        "mat"."het_pool",
        "agwc"."techno_code",
        "agwc"."sample_variant_count",
        "cda"."bebid_variant_count",
        "cda2"."cda_sample_variant_count"
      FROM(
        SELECT
            "bb_mat"."material_guid",
            "bb_mat"."line_guid",
            "bb_mat"."be_bid",
            COALESCE("cmt_fp"."fp_lbg_bid", "cmt_mp"."mp_lbg_bid") AS "lbg_bid",
            COALESCE("cmt_fp"."fp_sample", "cmt_mp"."mp_sample") AS "sample_id",
            COALESCE("cmt_fp"."fp_fada_group", "cmt_mp"."mp_fada_group", 'None') AS "fada_group",
            COALESCE("cmt_fp"."fp_het_pool", "cmt_mp"."mp_het_pool") AS "het_pool"
        FROM "managed"."rv_bb_material_sdl" "bb_mat"
        LEFT JOIN(
            SELECT DISTINCT "fp_be_bid", "fp_lbg_bid", "fp_sample", "fp_fada_group", "fp_het_pool" FROM "managed"."rv_corn_material_tester_adapt"
            WHERE "fp_be_bid" IS NOT NULL
        ) "cmt_fp"
          ON "bb_mat"."be_bid" = "cmt_fp"."fp_be_bid"
        LEFT JOIN(
            SELECT DISTINCT "mp_be_bid", "mp_lbg_bid", "mp_sample", "mp_fada_group", "mp_het_pool" FROM "managed"."rv_corn_material_tester_adapt"
            WHERE "mp_be_bid" IS NOT NULL
        ) "cmt_mp"
          ON "bb_mat"."be_bid" = "cmt_mp"."mp_be_bid"
        WHERE "bb_mat"."crop_guid" = 'B79D32C9-7850-41D0-BE44-894EC95AB285'
            AND "bb_mat"."be_bid" LIKE 'EMF%'
            AND ("cmt_fp"."fp_be_bid" IS NOT NULL OR "cmt_mp"."mp_be_bid" IS NOT NULL)
    )"mat"
    LEFT JOIN(
        SELECT 
            "sample_id" AS "sample_id",
            "techno_code" AS "techno_code",
            COUNT("genotype") AS "sample_variant_count"
          FROM "managed"."rv_assay_genotype_with_crops"
        WHERE (("techno_code" = 'AX') OR ("techno_code" = 'II')) 
          AND (CAST("crop_id" AS integer) = 9)
          AND "top_geno" IN ('A/A', 'C/C', 'G/G', 'T/T')
        GROUP BY "sample_id", "techno_code"
    )"agwc"
    ON "mat"."sample_id" = "agwc"."sample_id"
    LEFT JOIN(
        SELECT
            "be_bid",
            COUNT("variant_id") AS "bebid_variant_count"
          FROM "managed"."rv_cda_genotype"
        WHERE "genotype" IN ('A/A', 'C/C', 'G/G', 'T/T')
            AND LOWER("species_name") = 'corn'
        GROUP BY "be_bid"
    )"cda"
    ON "mat"."be_bid" = "cda"."be_bid"
    LEFT JOIN(
        SELECT
            "distinct_samples",
            COUNT("variant_id") AS "cda_sample_variant_count"
          FROM "managed"."rv_cda_genotype"
        WHERE "genotype" IN ('A/A', 'C/C', 'G/G', 'T/T')
            AND LOWER("species_name") = 'corn'
        GROUP BY "distinct_samples"
    )"cda2"
    ON "mat"."sample_id" = "cda2"."distinct_samples"
    WHERE "agwc"."sample_variant_count" IS NOT NULL
      OR "cda"."bebid_variant_count" IS NOT NULL
    """)
        return output_df


def get_comparison_set_soy():
    with DenodoConnection() as dc:
        output_df = dc.get_data("""
        select
    "bb_mat_info"."be_bid",
    "mint_material"."lbg_bid",
    "material_info"."sample_id",
    COALESCE("giga_class"."cluster_idx", 'None') AS "fada_group",
    "agwc"."sample_variant_count",
    "cda"."bebid_variant_count",
    "cda2"."cda_sample_variant_count"
from
    (
        SELECT
            "bb_mat"."material_guid",
            "bb_mat"."be_bid"
        FROM
            "managed"."rv_bb_material_sdl" "bb_mat"
        WHERE
            "bb_mat"."crop_guid" = '6C9085C2-C442-48C4-ACA7-427C4760B642'
            AND "bb_mat"."be_bid" LIKE 'ESY%'
    ) "bb_mat_info"
    inner join (
        SELECT
            "assay_size"."sample_id",
            "sample_api"."germplasm_guid",
            "sample_api"."germplasm_id",
            "assay_size"."genotype_count"
        FROM
            "managed"."rv_sample_api" "sample_api"
            INNER JOIN (
                SELECT
                    *
                FROM
                    (
                        SELECT
                            "sample_id" AS "sample_id",
                            "techno_code" AS "techno_code",
                            COUNT("genotype") AS "genotype_count"
                        FROM
                            "managed"."rv_assay_genotype_with_crops"
                        WHERE
                            (
                                ("techno_code" = 'AX')
                                OR ("techno_code" = 'II')
                            )
                            AND ("crop_id" = 7)
                            AND "top_geno" IN ('A/A', 'C/C', 'G/G', 'T/T')
                        GROUP BY
                            "sample_id",
                            "techno_code"
                    ) "unfiltered_query"
                WHERE
                    "genotype_count" > 99
            ) "assay_size" ON "sample_api"."sample_code" = "assay_size"."sample_id"
            AND "sample_api"."crop_id" = 7
    ) "material_info" on "bb_mat_info"."material_guid" = "material_info"."germplasm_guid"
    LEFT JOIN (
        select
            "mint_material_trait"."be_bid",
            "mint_material_trait"."lbg_bid"
        from
            "managed"."rv_mint_material_trait_sp" "mint_material_trait"
    ) "mint_material" on "bb_mat_info"."be_bid" = "mint_material"."be_bid"
    LEFT JOIN (
        SELECT
            "giga_classification"."be_bid",
            "giga_classification"."cluster_idx"
        from
            "managed"."rv_giga_classification" "giga_classification"
    ) "giga_class" ON "bb_mat_info"."be_bid" = "giga_class"."be_bid"
    LEFT JOIN(
        SELECT
            "sample_id" AS "sample_id",
            "techno_code" AS "techno_code",
            COUNT("genotype") AS "sample_variant_count"
        FROM
            "managed"."rv_assay_genotype_with_crops"
        WHERE
            (
                ("techno_code" = 'AX')
                OR ("techno_code" = 'II')
            )
            AND (CAST("crop_id" AS integer) = 7)
            AND "top_geno" IN ('A/A', 'C/C', 'G/G', 'T/T')
        GROUP BY
            "sample_id",
            "techno_code"
    ) "agwc" ON "material_info"."sample_id" = "agwc"."sample_id"
    LEFT JOIN(
        SELECT
            "be_bid",
            COUNT("variant_id") AS "bebid_variant_count"
        FROM
            "managed"."rv_cda_genotype"
        WHERE
            "genotype" IN ('A/A', 'C/C', 'G/G', 'T/T')
            AND LOWER("species_name") = 'soybean'
        GROUP BY
            "be_bid"
    ) "cda" ON "bb_mat_info"."be_bid" = "cda"."be_bid"
    LEFT JOIN(
        SELECT
            "distinct_samples",
            COUNT("variant_id") AS "cda_sample_variant_count"
        FROM
            "managed"."rv_cda_genotype"
        WHERE
            "genotype" IN ('A/A', 'C/C', 'G/G', 'T/T')
            AND LOWER("species_name") = 'soybean'
        GROUP BY
            "distinct_samples"
    ) "cda2" ON "material_info"."sample_id" = "cda2"."distinct_samples"
WHERE
    "agwc"."sample_variant_count" IS NOT NULL
    AND "cda2"."cda_sample_variant_count" > 0
    AND "cda"."bebid_variant_count" > 0
""")
        return output_df


def get_comparison_set_sunflower():
    with DenodoConnection() as dc:
        output_df = dc.get_data("""
    SELECT DISTINCT
        "mat"."be_bid",
        "mat"."lbg_bid",
        "mat"."sample_id",
        COALESCE("mat"."fada_group", 'None') AS "fada_group",
--        "mat"."het_pool",
        "agwc"."techno_code",
        "agwc"."sample_variant_count",
        "cda"."bebid_variant_count",
        "cda2"."cda_sample_variant_count"
      FROM(
        SELECT
            "bb_mat"."material_guid",
            "bb_mat"."line_guid",
            "bb_mat"."be_bid",
            COALESCE("cmt_fp"."fp_lbg_bid", "cmt_mp"."mp_lbg_bid") AS "lbg_bid",
            COALESCE("cmt_fp"."fp_sample", "cmt_mp"."mp_sample") AS "sample_id",
            COALESCE("cmt_fp"."fp_fada_group", "cmt_mp"."mp_fada_group", 'None') AS "fada_group",
            COALESCE("cmt_fp"."fp_het_pool", "cmt_mp"."mp_het_pool") AS "het_pool"
        FROM "managed"."rv_bb_material_sdl" "bb_mat"
        LEFT JOIN(
            SELECT DISTINCT "fp_be_bid", "fp_lbg_bid", "fp_sample", "fp_fada_group", "fp_het_pool" FROM "managed"."rv_corn_material_tester_adapt"
            WHERE "fp_be_bid" IS NOT NULL
        ) "cmt_fp"
          ON "bb_mat"."be_bid" = "cmt_fp"."fp_be_bid"
        LEFT JOIN(
            SELECT DISTINCT "mp_be_bid", "mp_lbg_bid", "mp_sample", "mp_fada_group", "mp_het_pool" FROM "managed"."rv_corn_material_tester_adapt"
            WHERE "mp_be_bid" IS NOT NULL
        ) "cmt_mp"
          ON "bb_mat"."be_bid" = "cmt_mp"."mp_be_bid"
        WHERE "bb_mat"."crop_guid" = '824A412A-2EA6-4288-832D-16E165BD033E'
            AND "bb_mat"."be_bid" LIKE 'ESU%'
            AND ("cmt_fp"."fp_be_bid" IS NOT NULL OR "cmt_mp"."mp_be_bid" IS NOT NULL)
    )"mat"
    LEFT JOIN(
        SELECT 
            "sample_id" AS "sample_id",
            "techno_code" AS "techno_code",
            COUNT("genotype") AS "sample_variant_count"
          FROM "managed"."rv_assay_genotype_with_crops"
        WHERE (("techno_code" = 'AX') OR ("techno_code" = 'II')) 
          AND (CAST("crop_id" AS integer) = 12)
          AND "top_geno" IN ('A/A', 'C/C', 'G/G', 'T/T')
        GROUP BY "sample_id", "techno_code"
    )"agwc"
    ON "mat"."sample_id" = "agwc"."sample_id"
    LEFT JOIN(
        SELECT
            "be_bid",
            COUNT("variant_id") AS "bebid_variant_count"
          FROM "managed"."rv_cda_genotype"
        WHERE "genotype" IN ('A/A', 'C/C', 'G/G', 'T/T')
            AND LOWER("species_name") = 'common sunflower'
        GROUP BY "be_bid"
    )"cda"
    ON "mat"."be_bid" = "cda"."be_bid"
    LEFT JOIN(
        SELECT
            "distinct_samples",
            COUNT("variant_id") AS "cda_sample_variant_count"
          FROM "managed"."rv_cda_genotype"
        WHERE "genotype" IN ('A/A', 'C/C', 'G/G', 'T/T')
            AND LOWER("species_name") = 'common sunflower'
        GROUP BY "distinct_samples"
    )"cda2"
    ON "mat"."sample_id" = "cda2"."distinct_samples"
    WHERE "agwc"."sample_variant_count" IS NOT NULL
      OR "cda"."bebid_variant_count" IS NOT NULL
    """)
        return output_df


def get_comparison_set_sunflower_cda():
    with DenodoConnection() as dc:
        output_df = dc.get_data("""
        SELECT
            "be_bid",
            COUNT("variant_id") AS "bebid_variant_count"
          FROM "managed"."rv_cda_genotype"
        WHERE "genotype" IN ('A/A', 'C/C', 'G/G', 'T/T')
            AND LOWER("species_name") = 'common sunflower'
        GROUP BY "be_bid"
        """)
        return output_df


def get_comparison_set_sunflower_cda2():
    with DenodoConnection() as dc:
        output_df = dc.get_data("""
        SELECT
            "distinct_samples",
            COUNT("variant_id") AS "cda_sample_variant_count"
          FROM "managed"."rv_cda_genotype"
        WHERE "genotype" IN ('A/A', 'C/C', 'G/G', 'T/T')
            AND LOWER("species_name") = 'common sunflower'
        GROUP BY "distinct_samples"
        """)
        return output_df


def get_comparison_set_soy_subset():
    with DenodoConnection() as dc:
        output_df = dc.get_data("""
        select
*
from
    (
        SELECT
            "bb_mat"."material_guid",
            "bb_mat"."be_bid"
        FROM
            "managed"."rv_bb_material_sdl" "bb_mat"
        WHERE
            "bb_mat"."crop_guid" = '6C9085C2-C442-48C4-ACA7-427C4760B642'
            AND "bb_mat"."be_bid" LIKE 'ESY%'
    ) "bb_mat_info"
    inner join (
        SELECT
            "assay_size"."sample_id",
            "sample_api"."germplasm_guid",
            "sample_api"."germplasm_id",
            "assay_size"."genotype_count"
        FROM
            "managed"."rv_sample_api" "sample_api"
            INNER JOIN (
                SELECT
                    *
                FROM
                    (
                        SELECT
                            "sample_id" AS "sample_id",
                            "techno_code" AS "techno_code",
                            COUNT("genotype") AS "genotype_count"
                        FROM
                            "managed"."rv_assay_genotype_with_crops"
                        WHERE
                            (
                                ("techno_code" = 'AX')
                                OR ("techno_code" = 'II')
                            )
                            AND ("crop_id" = 7)
                            AND "top_geno" IN ('A/A', 'C/C', 'G/G', 'T/T')
                        GROUP BY
                            "sample_id",
                            "techno_code"
                    ) "unfiltered_query"
                WHERE
                    "genotype_count" > 99
            ) "assay_size" ON "sample_api"."sample_code" = "assay_size"."sample_id"
            AND "sample_api"."crop_id" = 7
    ) "material_info" on "bb_mat_info"."material_guid" = "material_info"."germplasm_guid"
""")
        return output_df


def get_comparison_set_soy_subset_material_sdl():
    with DenodoConnection() as dc:
        output_df = dc.get_data("""
        SELECT
            "bb_mat"."material_guid",
            "bb_mat"."be_bid"
        FROM
            "managed"."rv_bb_material_sdl" "bb_mat"
        WHERE
            "bb_mat"."crop_guid" = '6C9085C2-C442-48C4-ACA7-427C4760B642'
            AND "bb_mat"."be_bid" LIKE 'ESY%'
""")
        return output_df


def get_comparison_set_sunflower_subset_material_sdl():
    with DenodoConnection() as dc:
        output_df = dc.get_data("""
        SELECT
            "bb_mat"."material_guid",
            "bb_mat"."be_bid"
        FROM
            "managed"."rv_bb_material_sdl" "bb_mat"
        WHERE
            "bb_mat"."crop_guid" = '824A412A-2EA6-4288-832D-16E165BD033E'
            AND "bb_mat"."be_bid" LIKE 'ESU%'
""")
        return output_df


def get_comparison_set_soy_subset_genotype_count():
    with DenodoConnection() as dc:
        output_df = dc.get_data("""   
    SELECT
                    *
                FROM
                    (
                        SELECT
                            "sample_id" AS "sample_id",
                            "techno_code" AS "techno_code",
                            COUNT("genotype") AS "genotype_count"
                        FROM
                            "managed"."rv_assay_genotype_with_crops"
                        WHERE
                            (
                                ("techno_code" = 'AX')
                                OR ("techno_code" = 'II')
                            )
                            AND ("crop_id" = 7)
                            AND "top_geno" IN ('A/A', 'C/C', 'G/G', 'T/T')
                        GROUP BY
                            "sample_id",
                            "techno_code"
                    ) "unfiltered_query"
                WHERE
                    "genotype_count" > 99
""")
        return output_df


def get_comparison_set_sunflower_subset_genotype_count():
    with DenodoConnection() as dc:
        output_df = dc.get_data("""   
    SELECT
                    *
                FROM
                    (
                        SELECT
                            "sample_id" AS "sample_id",
                            "techno_code" AS "techno_code",
                            COUNT("genotype") AS "genotype_count"
                        FROM
                            "managed"."rv_assay_genotype_with_crops"
                        WHERE
                            (
                                ("techno_code" = 'AX')
                                OR ("techno_code" = 'II')
                            )
                            AND ("crop_id" = 12)
                            AND "top_geno" IN ('A/A', 'C/C', 'G/G', 'T/T')
                        GROUP BY
                            "sample_id",
                            "techno_code"
                    ) "unfiltered_query"
                WHERE
                    "genotype_count" > 99
""")
        return output_df


def get_comparison_set_soy_subset_sample_api(sample_df, batch_size):
    df_geno_count = pd.DataFrame()
    sample_n = len(sample_df["sample_id"].drop_duplicates())
    print('sample_n: ', sample_n)
    sample_n_iter = int(math.ceil(sample_n / batch_size))
    print('sample_n_iter: ', sample_n_iter)

    for i in range(sample_n_iter):
        sample_list = "('" + sample_df.iloc[i * batch_size:min((i + 1) * batch_size - 1, sample_n), :][
            "sample_id"].drop_duplicates().str.cat(sep="', '") + "')"

        with DenodoConnection() as dc:
            out_df = dc.get_data("""
            SELECT
            -- "assay_size"."sample_id",
            "sample_api"."sample_code",
            "sample_api"."germplasm_guid",
            "sample_api"."germplasm_id" --,
            --"assay_size"."genotype_count"
        FROM
            "managed"."rv_sample_api" "sample_api"
            where "sample_api"."crop_id" = 7 and "sample_api"."sample_code" IN {0}""".format(sample_list))

        df_geno_count = pd.concat([df_geno_count, out_df], ignore_index=True)

        # print("Executing query")
        # print('i: ', i, ' of: ', sample_n_iter)
    return df_geno_count


def get_comparison_set_sunflower_subset_sample_api(sample_df, batch_size):
    df_geno_count = pd.DataFrame()
    sample_n = len(sample_df["sample_id"].drop_duplicates())
    print('sample_n: ', sample_n)
    sample_n_iter = int(math.ceil(sample_n / batch_size))
    print('sample_n_iter: ', sample_n_iter)

    for i in range(sample_n_iter):
        sample_list = "('" + sample_df.iloc[i * batch_size:min((i + 1) * batch_size - 1, sample_n), :][
            "sample_id"].drop_duplicates().str.cat(sep="', '") + "')"

        with DenodoConnection() as dc:
            out_df = dc.get_data("""
            SELECT
            -- "assay_size"."sample_id",
            "sample_api"."sample_code",
            "sample_api"."germplasm_guid",
            "sample_api"."germplasm_id" --,
            --"assay_size"."genotype_count"
        FROM
            "managed"."rv_sample_api" "sample_api"
            where "sample_api"."crop_id" = 12 and "sample_api"."sample_code" IN {0}""".format(sample_list))

        df_geno_count = pd.concat([df_geno_count, out_df], ignore_index=True)

        # print("Executing query")
        # print('i: ', i, ' of: ', sample_n_iter)
    return df_geno_count


def get_comparison_set_soy_subset_mint_material_giga_class(sample_df, batch_size):
    df_mint_material = pd.DataFrame()
    df_giga_class = pd.DataFrame()
    sample_n = len(sample_df["be_bid"].drop_duplicates())
    print('sample_n: ', sample_n)
    sample_n_iter = int(math.ceil(sample_n / batch_size))
    print('sample_n_iter: ', sample_n_iter)

    for i in range(sample_n_iter):
        sample_list = "('" + sample_df.iloc[i * batch_size:min((i + 1) * batch_size - 1, sample_n), :][
            "be_bid"].drop_duplicates().str.cat(sep="', '") + "')"

        with DenodoConnection() as dc:
            out_df_mint = dc.get_data("""
            select
            "mint_material_trait"."be_bid",
            "mint_material_trait"."lbg_bid"
        from
            "managed"."rv_mint_material_trait_sp" "mint_material_trait"
             where "mint_material_trait"."be_bid" IN {0}""".format(sample_list))

            out_df_giga = dc.get_data("""
            SELECT
            "giga_classification"."be_bid",
            "giga_classification"."cluster_idx"
        from
            "managed"."rv_giga_classification" "giga_classification"
            where "giga_classification"."be_bid" IN {0}""".format(sample_list))

        df_mint_material = pd.concat([df_mint_material, out_df_mint], ignore_index=True)
        df_giga_class = pd.concat([df_giga_class, out_df_giga], ignore_index=True)

        # print("Executing query")
        # print('i: ', i, ' of: ', sample_n_iter)
    return df_mint_material, df_giga_class


def get_comparison_set_soy_subset_agwc(sample_df, batch_size):
    df_agwc = pd.DataFrame()
    sample_n = len(sample_df["sample_id"].drop_duplicates())
    print('sample_n: ', sample_n)
    sample_n_iter = int(math.ceil(sample_n / batch_size))
    print('sample_n_iter: ', sample_n_iter)

    for i in range(sample_n_iter):
        sample_list = "('" + sample_df.iloc[i * batch_size:min((i + 1) * batch_size - 1, sample_n), :][
            "sample_id"].drop_duplicates().str.cat(sep="', '") + "')"

        with DenodoConnection() as dc:
            out_df = dc.get_data("""
            SELECT
            "sample_id" AS "sample_id",
            "techno_code" AS "techno_code",
            COUNT("genotype") AS "sample_variant_count"
        FROM
            "managed"."rv_assay_genotype_with_crops"
        WHERE
            (
                ("techno_code" = 'AX')
                OR ("techno_code" = 'II')
            )
            AND (CAST("crop_id" AS integer) = 7)
            AND "top_geno" IN ('A/A', 'C/C', 'G/G', 'T/T')
            and "sample_id" IN {0}
        GROUP BY
            "sample_id",
            "techno_code" 
            """.format(sample_list))

        df_agwc = pd.concat([df_agwc, out_df], ignore_index=True)

        # print("Executing query")
        # print('i: ', i, ' of: ', sample_n_iter)
    return df_agwc


def get_comparison_set_sunflower_subset_agwc(sample_df, batch_size):
    df_agwc = pd.DataFrame()
    sample_n = len(sample_df["sample_id"].drop_duplicates())
    print('sample_n: ', sample_n)
    sample_n_iter = int(math.ceil(sample_n / batch_size))
    print('sample_n_iter: ', sample_n_iter)

    for i in range(sample_n_iter):
        sample_list = "('" + sample_df.iloc[i * batch_size:min((i + 1) * batch_size - 1, sample_n), :][
            "sample_id"].drop_duplicates().str.cat(sep="', '") + "')"

        with DenodoConnection() as dc:
            out_df = dc.get_data("""
            SELECT
            "sample_id" AS "sample_id",
            "techno_code" AS "techno_code",
            COUNT("genotype") AS "sample_variant_count"
        FROM
            "managed"."rv_assay_genotype_with_crops"
        WHERE
            (
                ("techno_code" = 'AX')
                OR ("techno_code" = 'II')
            )
            AND (CAST("crop_id" AS integer) = 12)
            AND "top_geno" IN ('A/A', 'C/C', 'G/G', 'T/T')
            and "sample_id" IN {0}
        GROUP BY
            "sample_id",
            "techno_code" 
            """.format(sample_list))

        df_agwc = pd.concat([df_agwc, out_df], ignore_index=True)

        # print("Executing query")
        # print('i: ', i, ' of: ', sample_n_iter)
    return df_agwc


def get_comparison_set_soy_subset_cda(sample_df, batch_size):
    df_cda = pd.DataFrame()
    sample_n = len(sample_df["be_bid"].drop_duplicates())
    print('sample_n: ', sample_n)
    sample_n_iter = int(math.ceil(sample_n / batch_size))
    print('sample_n_iter: ', sample_n_iter)

    for i in range(sample_n_iter):
        sample_list = "('" + sample_df.iloc[i * batch_size:min((i + 1) * batch_size - 1, sample_n), :][
            "be_bid"].drop_duplicates().str.cat(sep="', '") + "')"

        with DenodoConnection() as dc:
            out_df = dc.get_data("""
            SELECT
            "be_bid",
            COUNT("variant_id") AS "bebid_variant_count"
        FROM
            "managed"."rv_cda_genotype"
        WHERE
            "genotype" IN ('A/A', 'C/C', 'G/G', 'T/T')
            AND LOWER("species_name") = 'soybean'
            and "be_bid" IN {0}
        GROUP BY
            "be_bid"
            """.format(sample_list))

        df_cda = pd.concat([df_cda, out_df], ignore_index=True)

        # print("Executing query")
        # print('i: ', i, ' of: ', sample_n_iter)
    return df_cda


def get_comparison_set_soy_subset_cda2(sample_df, batch_size):
    df_cda2 = pd.DataFrame()
    sample_n = len(sample_df["sample_id"].drop_duplicates())
    print('sample_n: ', sample_n)
    sample_n_iter = int(math.ceil(sample_n / batch_size))
    print('sample_n_iter: ', sample_n_iter)

    for i in range(sample_n_iter):
        sample_list = "('" + sample_df.iloc[i * batch_size:min((i + 1) * batch_size - 1, sample_n), :][
            "sample_id"].drop_duplicates().str.cat(sep="', '") + "')"

        with DenodoConnection() as dc:
            output_df = dc.get_data("""     
        SELECT
                "distinct_samples",
                COUNT("variant_id") AS "cda_sample_variant_count"
              FROM "managed"."rv_cda_genotype"
            WHERE "genotype" IN ('A/A', 'C/C', 'G/G', 'T/T')
                AND LOWER("species_name") = 'soybean'
                and "distinct_samples" IN {0}
            GROUP BY "distinct_samples"
            """.format(sample_list))

        df_cda2 = pd.concat([df_cda2, output_df], ignore_index=True)

        # print("Executing query")
        # print('i: ', i, ' of: ', sample_n_iter)
    return df_cda2


def get_comparison_set_soy_year_bebid(year, region_lid):
    with DenodoConnection() as dc:
        out_df = dc.get_data("""
        SELECT
            DISTINCT min(rv_bb_ete.year) AS year,
            rv_bb_mat.be_bid
        FROM
            managed.rv_bb_experiment_trial_entry_sdl rv_bb_ete
            INNER JOIN managed.rv_bb_material_sdl rv_bb_mat ON rv_bb_ete.planted_material_guid = rv_bb_mat.material_guid
            INNER JOIN managed.rv_bb_location_sdl rv_bb_loc ON rv_bb_ete.location_guid = rv_bb_loc.location_guid
        WHERE
            rv_bb_ete.crop_code = 'SOY'
            AND rv_bb_ete.season = 'SUMR'
            AND rv_bb_loc.region_lid = '{1}'
            AND rv_bb_ete.year <= {0}
            AND rv_bb_ete.year >= 2014
            AND rv_bb_ete.exclude_from_analysis < 1
            AND LOWER(rv_bb_mat.material_id) NOT IN ('filler', 'purple')
            AND rv_bb_ete.trial_crop_type_lid LIKE 'YG%'
        GROUP BY
            rv_bb_mat.be_bid
            """.format(year, region_lid))

    # print("Executing query")
    # print('i: ', i, ' of: ', sample_n_iter)
    return out_df


def get_comparison_set_corn_mat():
    with DenodoConnection() as dc:
        output_df = dc.get_data("""    
    SELECT
            "bb_mat"."material_guid",
            "bb_mat"."line_guid",
            "bb_mat"."be_bid",
            COALESCE("cmt_fp"."fp_lbg_bid", "cmt_mp"."mp_lbg_bid") AS "lbg_bid",
            COALESCE("cmt_fp"."fp_sample", "cmt_mp"."mp_sample") AS "sample_id",
            COALESCE("cmt_fp"."fp_fada_group", "cmt_mp"."mp_fada_group", 'None') AS "fada_group",
            COALESCE("cmt_fp"."fp_het_pool", "cmt_mp"."mp_het_pool") AS "het_pool"
        FROM "managed"."rv_bb_material_sdl" "bb_mat"
        LEFT JOIN(
            SELECT DISTINCT "fp_be_bid", "fp_lbg_bid", "fp_sample", "fp_fada_group", "fp_het_pool" FROM "managed"."rv_corn_material_tester_adapt"
            WHERE "fp_be_bid" IS NOT NULL
        ) "cmt_fp"
          ON "bb_mat"."be_bid" = "cmt_fp"."fp_be_bid"
        LEFT JOIN(
            SELECT DISTINCT "mp_be_bid", "mp_lbg_bid", "mp_sample", "mp_fada_group", "mp_het_pool" FROM "managed"."rv_corn_material_tester_adapt"
            WHERE "mp_be_bid" IS NOT NULL
        ) "cmt_mp"
          ON "bb_mat"."be_bid" = "cmt_mp"."mp_be_bid"
        WHERE "bb_mat"."crop_guid" = 'B79D32C9-7850-41D0-BE44-894EC95AB285'
            AND "bb_mat"."be_bid" LIKE 'EMF%'
            AND ("cmt_fp"."fp_be_bid" IS NOT NULL OR "cmt_mp"."mp_be_bid" IS NOT NULL)
            """)
        return output_df


def get_comparison_set_corn_subset_agwc(sample_df, batch_size):
    df_agwc = pd.DataFrame()
    sample_n = len(sample_df["sample_id"].drop_duplicates())
    print('sample_n: ', sample_n)
    sample_n_iter = int(math.ceil(sample_n / batch_size))
    print('sample_n_iter: ', sample_n_iter)

    for i in range(sample_n_iter):
        sample_list = "('" + sample_df.iloc[i * batch_size:min((i + 1) * batch_size - 1, sample_n), :][
            "sample_id"].drop_duplicates().str.cat(sep="', '") + "')"

        with DenodoConnection() as dc:
            out_df = dc.get_data("""
            SELECT
            "sample_id" AS "sample_id",
            "techno_code" AS "techno_code",
            COUNT("genotype") AS "sample_variant_count"
        FROM
            "managed"."rv_assay_genotype_with_crops"
        WHERE
            (
                ("techno_code" = 'AX')
                OR ("techno_code" = 'II')
            )
            AND (CAST("crop_id" AS integer) = 9)
            AND "top_geno" IN ('A/A', 'C/C', 'G/G', 'T/T')
            and "sample_id" IN {0}
        GROUP BY
            "sample_id",
            "techno_code" 
            """.format(sample_list))

        df_agwc = pd.concat([df_agwc, out_df], ignore_index=True)

        # print("Executing query")
        # print('i: ', i, ' of: ', sample_n_iter)
    return df_agwc


def get_comparison_set_corn_subset_cda(sample_df, batch_size):
    df_cda = pd.DataFrame()
    sample_n = len(sample_df["be_bid"].drop_duplicates())
    print('sample_n: ', sample_n)
    sample_n_iter = int(math.ceil(sample_n / batch_size))
    print('sample_n_iter: ', sample_n_iter)

    for i in range(sample_n_iter):
        sample_list = "('" + sample_df.iloc[i * batch_size:min((i + 1) * batch_size - 1, sample_n), :][
            "be_bid"].drop_duplicates().str.cat(sep="', '") + "')"

        with DenodoConnection() as dc:
            out_df = dc.get_data("""
            SELECT
            "be_bid",
            COUNT("variant_id") AS "bebid_variant_count"
        FROM
            "managed"."rv_cda_genotype"
        WHERE
            "genotype" IN ('A/A', 'C/C', 'G/G', 'T/T')
            AND LOWER("species_name") = 'corn'
            and "be_bid" IN {0}
        GROUP BY
            "be_bid"
            """.format(sample_list))

        df_cda = pd.concat([df_cda, out_df], ignore_index=True)

        # print("Executing query")
        # print('i: ', i, ' of: ', sample_n_iter)
    return df_cda


def get_comparison_set_corn_cda2(sample_df, batch_size):

    df_cda2 = pd.DataFrame()
    sample_n = len(sample_df["sample_id"].drop_duplicates())
    print('sample_n: ', sample_n)
    sample_n_iter = int(math.ceil(sample_n / batch_size))
    print('sample_n_iter: ', sample_n_iter)

    for i in range(sample_n_iter):
        sample_list = "('" + sample_df.iloc[i * batch_size:min((i + 1) * batch_size - 1, sample_n), :][
            "sample_id"].drop_duplicates().str.cat(sep="', '") + "')"

        with DenodoConnection() as dc:
            output_df = dc.get_data("""     
        SELECT
                "distinct_samples",
                COUNT("variant_id") AS "cda_sample_variant_count"
              FROM "managed"."rv_cda_genotype"
            WHERE "genotype" IN ('A/A', 'C/C', 'G/G', 'T/T')
                AND LOWER("species_name") = 'corn'
                and "distinct_samples" IN {0}
            GROUP BY "distinct_samples"
            """.format(sample_list))

        df_cda2 = pd.concat([df_cda2, output_df], ignore_index=True)

        # print("Executing query")
        print('i: ', i, ' of: ', sample_n_iter)
    return df_cda2


def get_comparison_set_corn_cda2_no_loop():

    with DenodoConnection() as dc:
        output_df = dc.get_data("""     
        SELECT
                "distinct_samples",
                COUNT("variant_id") AS "cda_sample_variant_count"
              FROM "managed"."rv_cda_genotype"
            WHERE "genotype" IN ('A/A', 'C/C', 'G/G', 'T/T')
                AND LOWER("species_name") = 'corn'
            GROUP BY "distinct_samples"
            """)

    return output_df


def get_comparison_set_soy_cda2_no_loop():

    with DenodoConnection() as dc:
        output_df = dc.get_data("""     
        SELECT
                "distinct_samples",
                COUNT("variant_id") AS "cda_sample_variant_count"
              FROM "managed"."rv_cda_genotype"
            WHERE "genotype" IN ('A/A', 'C/C', 'G/G', 'T/T')
                AND LOWER("species_name") = 'soybean'
            GROUP BY "distinct_samples"
            """)

    return output_df
