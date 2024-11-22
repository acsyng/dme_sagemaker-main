import math as math
import os

# import duckdb
import pandas as pd
# from pandasql import sqldf

from libs.denodo.denodo_connection import DenodoConnection

"""
get_trial_data

Queries managed.rv_trial_pheno_analytic_dataset, managed.rv_feature_export, managed.rv_corn_material_tester_adapt / advancement.rv_corn_material_tester,
and managed.rv_be_bid_ancestry_laas to pull in a complete trial dataset for a single year
"""


def get_trial_data(trial_year):
    with DenodoConnection() as dc:
        output_df = dc.get_data("""
            SELECT
                CAST(rv_tpad.year AS integer) AS year,
                rv_tpad.ap_data_sector,
                COALESCE(rv_fe.value, 
                         CAST(rv_tpad.maturity_group AS varchar), 
                         SUBSTRING(rv_tpad.loc_selector,0,1)) AS maturity_group,
                rv_tpad.experiment_id,
                rv_tpad.trial_id,
                rv_tpad.trial_status,
                rv_tpad.trial_stage,
                rv_tpad.loc_selector,
                rv_tpad.plot_barcode,
                rv_tpad.plant_date,
                rv_tpad.harvest_date,
                CASE WHEN country_code IN ('US', 'CA') AND x_longitude > 0
                    THEN COALESCE(-rv_tpad.x_longitude, coord_bak.x_longitude_bak)
                ELSE COALESCE(rv_tpad.x_longitude, coord_bak.x_longitude_bak)
                END AS x_longitude,
                COALESCE(rv_tpad.y_latitude, coord_bak.y_latitude_bak) AS y_latitude,
                rv_tpad.state_province_code,
                COALESCE(rv_tpad.irrigation, 'NONE') AS irrigation,
                CASE 
                    WHEN LOWER(rv_fe_pcrpc.value) = 'corn' THEN 2
                    WHEN rv_fe_pcrpc.value IS NOT NULL THEN 0
                    ELSE 1
                END AS previous_crop_corn,
                rv_tpad.material_id,
                rv_tpad.be_bid,
                COALESCE(rv_cmt.be_bid, rv_bba.be_bid) AS match_be_bid,
                CASE
                    WHEN rv_cmt.fp_het_pool != 'pool2' THEN COALESCE(rv_cmt.fp_be_bid, rv_bba.receiver_p)
                    WHEN rv_cmt.mp_het_pool = 'pool1' THEN COALESCE(rv_cmt.mp_be_bid, rv_bba.donor_p)
                    ELSE rv_bba.receiver_p
                END AS par_hp1_be_bid,
                CASE
                    WHEN rv_cmt.fp_het_pool != 'pool2' THEN rv_cmt.fp_sample
                    WHEN rv_cmt.mp_het_pool = 'pool1' THEN rv_cmt.mp_sample
                    ELSE NULL
                END AS par_hp1_sample,
                CASE
                    WHEN rv_cmt.fp_het_pool = 'pool2' THEN COALESCE(rv_cmt.fp_be_bid, rv_bba.receiver_p)
                    WHEN rv_cmt.mp_het_pool != 'pool1' THEN COALESCE(rv_cmt.mp_be_bid, rv_bba.donor_p)
                    ELSE rv_bba.donor_p
                END AS par_hp2_be_bid,
                CASE
                    WHEN rv_cmt.fp_het_pool = 'pool2' THEN rv_cmt.fp_sample
                    WHEN rv_cmt.mp_het_pool != 'pool1' THEN rv_cmt.mp_sample
                    ELSE NULL
                END AS par_hp2_sample,
                rv_cmt.fp_het_group,
                rv_cmt.mp_het_group,
                CASE WHEN rv_tpad.cpifl THEN 1 ELSE 0 END AS cpifl,
                COALESCE(CAST(rv_tpad.cperf AS integer), CASE WHEN rv_tpad.cpifl THEN 1 ELSE 0 END) AS cperf,
                rv_tpad.trait_measure_code AS trait,
                rv_tpad.result_numeric_value AS result
              FROM managed.rv_trial_pheno_analytic_dataset rv_tpad
            LEFT JOIN (
                SELECT
                    loc_code,
                    AVG(x_longitude_bak) AS x_longitude_bak,
                    AVG(y_latitude_bak) AS y_latitude_bak
                  FROM (
                    SELECT DISTINCT
                        loc_code,
                        CASE WHEN country_code IN ('US', 'CA') AND x_longitude > 0
                            THEN - x_longitude
                        ELSE x_longitude
                        END AS x_longitude_bak,
                        y_latitude AS y_latitude_bak
                      FROM managed.rv_trial_pheno_analytic_dataset
                    WHERE x_longitude IS NOT NULL
                    AND y_latitude IS NOT NULL
                    AND ap_data_sector = 'CORN_NA_SUMMER'
                ) coord_bak_raw
                GROUP BY loc_code
            ) coord_bak
              ON rv_tpad.loc_code = coord_bak.loc_code
            LEFT JOIN (
                SELECT
                    source_id,
                    value
                  FROM managed.rv_feature_export
                WHERE feature_name = 'MATURITY_ZONE'
                  AND feature_level = 'LOCATION'
                  AND feature_provider = 'SPIRIT'
                  AND market_name = 'CORN_NA_SUMMER'
                  AND CAST(year AS integer) = {0}
            ) rv_fe
              ON rv_tpad.loc_selector = rv_fe.source_id
            LEFT JOIN(
                SELECT
                    source_id,
                    value
                  FROM managed.rv_feature_export
                WHERE feature_name = 'PCRPC'
                  AND feature_level = 'LOCATION'
                  AND feature_provider = 'SPIRIT'
                  AND market_name = 'CORN_NA_SUMMER'
                  AND CAST(year AS integer) = {0}
            ) rv_fe_pcrpc
            ON rv_tpad.loc_selector = rv_fe_pcrpc.source_id
            LEFT JOIN managed.rv_corn_material_tester_adapt rv_cmt
              ON rv_tpad.be_bid = rv_cmt.be_bid
            LEFT JOIN managed.rv_be_bid_ancestry_laas rv_bba
              ON rv_tpad.be_bid = rv_bba.be_bid
            WHERE NOT rv_tpad.pr_exclude
              AND NOT rv_tpad.psp_exclude
              AND NOT rv_tpad.tr_exclude
              AND LOWER(rv_tpad.material_id) NOT IN ('filler', 'purple')
              AND rv_tpad.crop_type_code LIKE 'YG%'
              AND rv_tpad.bg_code IN ('AB','BD','BF','BG','BL','CC','DE','DL','DW','EG','GF','GL','GU','HE','HT','IB','JR',
                      'KF','LC','LD','LR','LT','LX','ME','MY','NC','NE','NW','RL','SE','SY','TP','WE','XB','ZY','GY','MB','DB')
              AND ((rv_tpad.trait_measure_code = 'YGSMN' AND rv_tpad.result_numeric_value > 0 AND rv_tpad.result_numeric_value < 300)
                  OR (rv_tpad.trait_measure_code ='GMSTP' AND rv_tpad.result_numeric_value >= 4.5 AND rv_tpad.result_numeric_value < 35)
                  OR (rv_tpad.trait_measure_code ='HAVPN'))
              AND rv_tpad.year = {0}
              AND rv_tpad.ap_data_sector = 'CORN_NA_SUMMER'
            """.format(trial_year))

    return output_df


def get_trial_data_CORN_BRAZIL_SUMMER(trial_year):
    with DenodoConnection() as dc:
        output_df = dc.get_data("""
            SELECT
                CAST(rv_tpad.year AS integer) AS year,
                rv_tpad.ap_data_sector,
                COALESCE(rv_fe.value, 
                         --CAST(rv_tpad.maturity_group AS varchar), 
                         SUBSTRING(rv_tpad.loc_selector,0,1)) AS tpp_region,
                rv_tpad.experiment_id,
                rv_tpad.trial_id,
                rv_tpad.trial_status,
                rv_tpad.trial_stage,
                rv_tpad.loc_selector,
                rv_tpad.plot_barcode,
                rv_tpad.plant_date,
                rv_tpad.harvest_date,
                CASE WHEN country_code IN ('US', 'CA') AND x_longitude > 0
                    THEN COALESCE(-rv_tpad.x_longitude, coord_bak.x_longitude_bak)
                ELSE COALESCE(rv_tpad.x_longitude, coord_bak.x_longitude_bak)
                END AS x_longitude,
                COALESCE(rv_tpad.y_latitude, coord_bak.y_latitude_bak) AS y_latitude,
                rv_tpad.state_province_code,
                COALESCE(rv_tpad.irrigation, 'NONE') AS irrigation,
                --CASE 
                --    WHEN LOWER(rv_fe_pcrpc.value) = 'corn' THEN 2
                --    WHEN rv_fe_pcrpc.value IS NOT NULL THEN 0
                --    ELSE 1
                --END AS previous_crop_corn,
                rv_tpad.material_id,
                rv_tpad.be_bid,
                COALESCE(rv_cmt.be_bid, rv_bba.be_bid) AS match_be_bid,
                CASE
                    WHEN rv_cmt.fp_het_pool != 'pool2' THEN COALESCE(rv_cmt.fp_be_bid, rv_bba.receiver_p)
                    WHEN rv_cmt.mp_het_pool = 'pool1' THEN COALESCE(rv_cmt.mp_be_bid, rv_bba.donor_p)
                    ELSE rv_bba.receiver_p
                END AS par_hp1_be_bid,
                CASE
                    WHEN rv_cmt.fp_het_pool != 'pool2' THEN rv_cmt.fp_sample
                    WHEN rv_cmt.mp_het_pool = 'pool1' THEN rv_cmt.mp_sample
                    ELSE NULL
                END AS par_hp1_sample,
                CASE
                    WHEN rv_cmt.fp_het_pool = 'pool2' THEN COALESCE(rv_cmt.fp_be_bid, rv_bba.receiver_p)
                    WHEN rv_cmt.mp_het_pool != 'pool1' THEN COALESCE(rv_cmt.mp_be_bid, rv_bba.donor_p)
                    ELSE rv_bba.donor_p
                END AS par_hp2_be_bid,
                CASE
                    WHEN rv_cmt.fp_het_pool = 'pool2' THEN rv_cmt.fp_sample
                    WHEN rv_cmt.mp_het_pool != 'pool1' THEN rv_cmt.mp_sample
                    ELSE NULL
                END AS par_hp2_sample,
                rv_cmt.fp_het_group,
                rv_cmt.mp_het_group,
                CASE WHEN rv_tpad.cpifl THEN 1 ELSE 0 END AS cpifl,
                COALESCE(CAST(rv_tpad.cperf AS integer), CASE WHEN rv_tpad.cpifl THEN 1 ELSE 0 END) AS cperf,
                rv_tpad.trait_measure_code AS trait,
                rv_tpad.result_numeric_value AS result
              FROM managed.rv_trial_pheno_analytic_dataset rv_tpad
            LEFT JOIN (
                SELECT
                    loc_code,
                    AVG(x_longitude_bak) AS x_longitude_bak,
                    AVG(y_latitude_bak) AS y_latitude_bak
                  FROM (
                    SELECT DISTINCT
                        loc_code,
                        CASE WHEN country_code IN ('US', 'CA') AND x_longitude > 0
                            THEN - x_longitude
                        ELSE x_longitude
                        END AS x_longitude_bak,
                        y_latitude AS y_latitude_bak
                      FROM managed.rv_trial_pheno_analytic_dataset
                    WHERE x_longitude IS NOT NULL
                    AND y_latitude IS NOT NULL
                    AND ap_data_sector = 'CORN_BRAZIL_SUMMER'
                ) coord_bak_raw
                GROUP BY loc_code
            ) coord_bak
              ON rv_tpad.loc_code = coord_bak.loc_code
            LEFT JOIN (
                SELECT
                    source_id,
                    SUBSTRING(value,4,6) as value
                  FROM managed.rv_feature_export
                WHERE feature_name = 'TPP_REGION'
                  AND feature_level = 'LOCATION'
                  --AND feature_provider = 'SPIRIT'
                  AND market_name = 'CORN_BRAZIL_SUMMER'
                  AND CAST(year AS integer) = {0}
            ) rv_fe
              ON rv_tpad.loc_selector = rv_fe.source_id
            --LEFT JOIN(
            --    SELECT
            --        source_id,
            --      value
            --      FROM managed.rv_feature_export
            --    WHERE feature_name = 'PCRPC'
            --      AND feature_level = 'LOCATION'
            --      AND feature_provider = 'SPIRIT'
            --     AND market_name = 'CORN_NA_SUMMER'
            --     AND CAST(year AS integer) = {0}
            --) rv_fe_pcrpc
            --ON rv_tpad.loc_selector = rv_fe_pcrpc.source_id
            LEFT JOIN managed.rv_corn_material_tester_adapt rv_cmt
              ON rv_tpad.be_bid = rv_cmt.be_bid
            LEFT JOIN managed.rv_be_bid_ancestry_laas rv_bba
              ON rv_tpad.be_bid = rv_bba.be_bid
            WHERE NOT rv_tpad.pr_exclude
              AND NOT rv_tpad.psp_exclude
              AND NOT rv_tpad.tr_exclude
              AND LOWER(rv_tpad.material_id) NOT IN ('filler', 'purple')
              AND rv_tpad.crop_type_code LIKE 'YG%'
              --AND rv_tpad.bg_code IN ('AB','BD','BF','BG','BL','CC','DE','DL','DW','EG','GF','GL','GU','HE','HT','IB','JR',
              --        'KF','LC','LD','LR','LT','LX','ME','MY','NC','NE','NW','RL','SE','SY','TP','WE','XB','ZY','GY','MB','DB')
              AND ((rv_tpad.trait_measure_code = 'YGSMN' AND rv_tpad.result_numeric_value > 0 AND rv_tpad.result_numeric_value < 300)
                  OR (rv_tpad.trait_measure_code ='GMSTP' AND rv_tpad.result_numeric_value >= 4.5 AND rv_tpad.result_numeric_value < 35)
                  OR (rv_tpad.trait_measure_code ='HAVPN'))
              AND rv_tpad.year = {0}
              AND rv_tpad.ap_data_sector = 'CORN_BRAZIL_SUMMER'
            """.format(trial_year))

    return output_df


def get_trial_data_CORNGRAIN_EAME_SUMMER(trial_year):
    with DenodoConnection() as dc:
        output_df = dc.get_data("""
            SELECT
                CAST(rv_tpad.year AS integer) AS year,
                rv_tpad.ap_data_sector,
                COALESCE(rv_fe.value, 
                         --CAST(rv_tpad.maturity_group AS varchar), 
                         SUBSTRING(rv_tpad.loc_selector,0,1)) AS ET,
                rv_tpad.experiment_id,
                rv_tpad.trial_id,
                rv_tpad.trial_status,
                rv_tpad.trial_stage,
                rv_tpad.loc_selector,
                rv_tpad.plot_barcode,
                rv_tpad.plant_date,
                rv_tpad.harvest_date,
                CASE WHEN country_code IN ('US', 'CA') AND x_longitude > 0
                    THEN COALESCE(-rv_tpad.x_longitude, coord_bak.x_longitude_bak)
                ELSE COALESCE(rv_tpad.x_longitude, coord_bak.x_longitude_bak)
                END AS x_longitude,
                COALESCE(rv_tpad.y_latitude, coord_bak.y_latitude_bak) AS y_latitude,
                rv_tpad.state_province_code,
                COALESCE(rv_tpad.irrigation, 'NONE') AS irrigation,
                --CASE 
                --    WHEN LOWER(rv_fe_pcrpc.value) = 'corn' THEN 2
                --    WHEN rv_fe_pcrpc.value IS NOT NULL THEN 0
                --    ELSE 1
                --END AS previous_crop_corn,
                rv_tpad.material_id,
                rv_tpad.be_bid,
                COALESCE(rv_cmt.be_bid, rv_bba.be_bid) AS match_be_bid,
                CASE
                    WHEN rv_cmt.fp_het_pool != 'pool2' THEN COALESCE(rv_cmt.fp_be_bid, rv_bba.receiver_p)
                    WHEN rv_cmt.mp_het_pool = 'pool1' THEN COALESCE(rv_cmt.mp_be_bid, rv_bba.donor_p)
                    ELSE rv_bba.receiver_p
                END AS par_hp1_be_bid,
                CASE
                    WHEN rv_cmt.fp_het_pool != 'pool2' THEN rv_cmt.fp_sample
                    WHEN rv_cmt.mp_het_pool = 'pool1' THEN rv_cmt.mp_sample
                    ELSE NULL
                END AS par_hp1_sample,
                CASE
                    WHEN rv_cmt.fp_het_pool = 'pool2' THEN COALESCE(rv_cmt.fp_be_bid, rv_bba.receiver_p)
                    WHEN rv_cmt.mp_het_pool != 'pool1' THEN COALESCE(rv_cmt.mp_be_bid, rv_bba.donor_p)
                    ELSE rv_bba.donor_p
                END AS par_hp2_be_bid,
                CASE
                    WHEN rv_cmt.fp_het_pool = 'pool2' THEN rv_cmt.fp_sample
                    WHEN rv_cmt.mp_het_pool != 'pool1' THEN rv_cmt.mp_sample
                    ELSE NULL
                END AS par_hp2_sample,
                rv_cmt.fp_het_group,
                rv_cmt.mp_het_group,
                CASE WHEN rv_tpad.cpifl THEN 1 ELSE 0 END AS cpifl,
                COALESCE(CAST(rv_tpad.cperf AS integer), CASE WHEN rv_tpad.cpifl THEN 1 ELSE 0 END) AS cperf,
                rv_tpad.trait_measure_code AS trait,
                rv_tpad.result_numeric_value AS result
              FROM managed.rv_trial_pheno_analytic_dataset rv_tpad
            LEFT JOIN (
                SELECT
                    loc_code,
                    AVG(x_longitude_bak) AS x_longitude_bak,
                    AVG(y_latitude_bak) AS y_latitude_bak
                  FROM (
                    SELECT DISTINCT
                        loc_code,
                        CASE WHEN country_code IN ('US', 'CA') AND x_longitude > 0
                            THEN - x_longitude
                        ELSE x_longitude
                        END AS x_longitude_bak,
                        y_latitude AS y_latitude_bak
                      FROM managed.rv_trial_pheno_analytic_dataset
                    WHERE x_longitude IS NOT NULL
                    AND y_latitude IS NOT NULL
                    AND ap_data_sector = 'CORNGRAIN_EAME_SUMMER'
                ) coord_bak_raw
                GROUP BY loc_code
            ) coord_bak
              ON rv_tpad.loc_code = coord_bak.loc_code
            LEFT JOIN (
                select 
                    source_id,
                    SUBSTRING(value,4,6) as value
                  FROM managed.rv_feature_export
                WHERE feature_name = 'EnvironmentType_Hierarchical_Level1_Code'
                  --AND feature_level = 'LOCATION'
                  --AND feature_provider = 'SPIRIT'
                and value != 'undefined'
                   AND market_name = 'CORNGRAIN_EAME_SUMMER'
                  AND CAST(year AS integer) = {0}
            ) rv_fe
              ON rv_tpad.loc_selector = rv_fe.source_id
            --LEFT JOIN(
            --    SELECT
            --        source_id,
            --      value
            --      FROM managed.rv_feature_export
            --    WHERE feature_name = 'PCRPC'
            --      AND feature_level = 'LOCATION'
            --      AND feature_provider = 'SPIRIT'
            --     AND market_name = 'CORN_NA_SUMMER'
            --     AND CAST(year AS integer) = {0}
            --) rv_fe_pcrpc
            --ON rv_tpad.loc_selector = rv_fe_pcrpc.source_id
            LEFT JOIN managed.rv_corn_material_tester_adapt rv_cmt
              ON rv_tpad.be_bid = rv_cmt.be_bid
            LEFT JOIN managed.rv_be_bid_ancestry_laas rv_bba
              ON rv_tpad.be_bid = rv_bba.be_bid
            WHERE NOT rv_tpad.pr_exclude
              AND NOT rv_tpad.psp_exclude
              AND NOT rv_tpad.tr_exclude
              AND LOWER(rv_tpad.material_id) NOT IN ('filler', 'purple')
              AND rv_tpad.crop_type_code LIKE 'YG%'
              --AND rv_tpad.bg_code IN ('AB','BD','BF','BG','BL','CC','DE','DL','DW','EG','GF','GL','GU','HE','HT','IB','JR',
              --        'KF','LC','LD','LR','LT','LX','ME','MY','NC','NE','NW','RL','SE','SY','TP','WE','XB','ZY','GY','MB','DB')
              AND ((rv_tpad.trait_measure_code = 'YGSMN' AND rv_tpad.result_numeric_value > 0 AND rv_tpad.result_numeric_value < 300)
                  OR (rv_tpad.trait_measure_code ='GMSTP' AND rv_tpad.result_numeric_value >= 4.5 AND rv_tpad.result_numeric_value < 35)
                  OR (rv_tpad.trait_measure_code ='HAVPN'))
              AND rv_tpad.year = {0}
              AND rv_tpad.ap_data_sector = 'CORNGRAIN_EAME_SUMMER'
            """.format(trial_year))

    return output_df


def get_trial_data_CORNSILAGE_EAME_SUMMER(trial_year):
    with DenodoConnection() as dc:
        output_df = dc.get_data("""
            SELECT
                CAST(rv_tpad.year AS integer) AS year,
                rv_tpad.ap_data_sector,
                COALESCE(rv_fe.value, 
                         --CAST(rv_tpad.maturity_group AS varchar), 
                         SUBSTRING(rv_tpad.loc_selector,0,1)) AS ET,
                rv_tpad.experiment_id,
                rv_tpad.trial_id,
                rv_tpad.trial_status,
                rv_tpad.trial_stage,
                rv_tpad.loc_selector,
                rv_tpad.plot_barcode,
                rv_tpad.plant_date,
                rv_tpad.harvest_date,
                CASE WHEN country_code IN ('US', 'CA') AND x_longitude > 0
                    THEN COALESCE(-rv_tpad.x_longitude, coord_bak.x_longitude_bak)
                ELSE COALESCE(rv_tpad.x_longitude, coord_bak.x_longitude_bak)
                END AS x_longitude,
                COALESCE(rv_tpad.y_latitude, coord_bak.y_latitude_bak) AS y_latitude,
                rv_tpad.state_province_code,
                COALESCE(rv_tpad.irrigation, 'NONE') AS irrigation,
                --CASE 
                --    WHEN LOWER(rv_fe_pcrpc.value) = 'corn' THEN 2
                --    WHEN rv_fe_pcrpc.value IS NOT NULL THEN 0
                --    ELSE 1
                --END AS previous_crop_corn,
                rv_tpad.material_id,
                rv_tpad.be_bid,
                COALESCE(rv_cmt.be_bid, rv_bba.be_bid) AS match_be_bid,
                CASE
                    WHEN rv_cmt.fp_het_pool != 'pool2' THEN COALESCE(rv_cmt.fp_be_bid, rv_bba.receiver_p)
                    WHEN rv_cmt.mp_het_pool = 'pool1' THEN COALESCE(rv_cmt.mp_be_bid, rv_bba.donor_p)
                    ELSE rv_bba.receiver_p
                END AS par_hp1_be_bid,
                CASE
                    WHEN rv_cmt.fp_het_pool != 'pool2' THEN rv_cmt.fp_sample
                    WHEN rv_cmt.mp_het_pool = 'pool1' THEN rv_cmt.mp_sample
                    ELSE NULL
                END AS par_hp1_sample,
                CASE
                    WHEN rv_cmt.fp_het_pool = 'pool2' THEN COALESCE(rv_cmt.fp_be_bid, rv_bba.receiver_p)
                    WHEN rv_cmt.mp_het_pool != 'pool1' THEN COALESCE(rv_cmt.mp_be_bid, rv_bba.donor_p)
                    ELSE rv_bba.donor_p
                END AS par_hp2_be_bid,
                CASE
                    WHEN rv_cmt.fp_het_pool = 'pool2' THEN rv_cmt.fp_sample
                    WHEN rv_cmt.mp_het_pool != 'pool1' THEN rv_cmt.mp_sample
                    ELSE NULL
                END AS par_hp2_sample,
                rv_cmt.fp_het_group,
                rv_cmt.mp_het_group,
                CASE WHEN rv_tpad.cpifl THEN 1 ELSE 0 END AS cpifl,
                COALESCE(CAST(rv_tpad.cperf AS integer), CASE WHEN rv_tpad.cpifl THEN 1 ELSE 0 END) AS cperf,
                rv_tpad.trait_measure_code AS trait,
                rv_tpad.result_numeric_value AS result
              FROM managed.rv_trial_pheno_analytic_dataset rv_tpad
            LEFT JOIN (
                SELECT
                    loc_code,
                    AVG(x_longitude_bak) AS x_longitude_bak,
                    AVG(y_latitude_bak) AS y_latitude_bak
                  FROM (
                    SELECT DISTINCT
                        loc_code,
                        CASE WHEN country_code IN ('US', 'CA') AND x_longitude > 0
                            THEN - x_longitude
                        ELSE x_longitude
                        END AS x_longitude_bak,
                        y_latitude AS y_latitude_bak
                      FROM managed.rv_trial_pheno_analytic_dataset
                    WHERE x_longitude IS NOT NULL
                    AND y_latitude IS NOT NULL
                    AND ap_data_sector = 'CORNSILAGE_EAME_SUMMER'
                ) coord_bak_raw
                GROUP BY loc_code
            ) coord_bak
              ON rv_tpad.loc_code = coord_bak.loc_code
            LEFT JOIN (
                select 
                    source_id,
                    SUBSTRING(value,4,6) as value
                  FROM managed.rv_feature_export
                WHERE feature_name = 'EnvironmentType_Hierarchical_Level1_Code'
                  --AND feature_level = 'LOCATION'
                  --AND feature_provider = 'SPIRIT'
                and value != 'undefined'
                   AND market_name = 'CORNSILAGE_EAME_SUMMER'
                  AND CAST(year AS integer) = {0}
            ) rv_fe
              ON rv_tpad.loc_selector = rv_fe.source_id
            --LEFT JOIN(
            --    SELECT
            --        source_id,
            --      value
            --      FROM managed.rv_feature_export
            --    WHERE feature_name = 'PCRPC'
            --      AND feature_level = 'LOCATION'
            --      AND feature_provider = 'SPIRIT'
            --     AND market_name = 'CORN_NA_SUMMER'
            --     AND CAST(year AS integer) = {0}
            --) rv_fe_pcrpc
            --ON rv_tpad.loc_selector = rv_fe_pcrpc.source_id
            LEFT JOIN managed.rv_corn_material_tester_adapt rv_cmt
              ON rv_tpad.be_bid = rv_cmt.be_bid
            LEFT JOIN managed.rv_be_bid_ancestry_laas rv_bba
              ON rv_tpad.be_bid = rv_bba.be_bid
            WHERE NOT rv_tpad.pr_exclude
              AND NOT rv_tpad.psp_exclude
              AND NOT rv_tpad.tr_exclude
              AND LOWER(rv_tpad.material_id) NOT IN ('filler', 'purple')
              AND (rv_tpad.crop_type_code LIKE 'YG%' OR rv_tpad.crop_type_code LIKE 'YS%') 
              --AND rv_tpad.bg_code IN ('AB','BD','BF','BG','BL','CC','DE','DL','DW','EG','GF','GL','GU','HE','HT','IB','JR',
              --        'KF','LC','LD','LR','LT','LX','ME','MY','NC','NE','NW','RL','SE','SY','TP','WE','XB','ZY','GY','MB','DB')
              AND ((rv_tpad.trait_measure_code = 'YSDMN' AND rv_tpad.result_numeric_value > 0 AND rv_tpad.result_numeric_value < 300)
                  OR (rv_tpad.trait_measure_code ='SDMCP' AND rv_tpad.result_numeric_value >= 4.5 AND rv_tpad.result_numeric_value < 35)
                  OR (rv_tpad.trait_measure_code ='HAVPN'))
              AND rv_tpad.year = {0}
              AND rv_tpad.ap_data_sector = 'CORNSILAGE_EAME_SUMMER'
            """.format(trial_year))

    return output_df


def get_trial_data_CORN_BRAZIL_SAFRINHA(trial_year):
    with DenodoConnection() as dc:
        output_df = dc.get_data("""
            SELECT
                CAST(rv_tpad.year AS integer) AS year,
                rv_tpad.ap_data_sector,
                COALESCE(rv_fe.value, 
                         --CAST(rv_tpad.maturity_group AS varchar), 
                         SUBSTRING(rv_tpad.loc_selector,0,1)) AS tpp_region,
                rv_tpad.experiment_id,
                rv_tpad.trial_id,
                rv_tpad.trial_status,
                rv_tpad.trial_stage,
                rv_tpad.loc_selector,
                rv_tpad.plot_barcode,
                rv_tpad.plant_date,
                rv_tpad.harvest_date,
                CASE WHEN country_code IN ('US', 'CA') AND x_longitude > 0
                    THEN COALESCE(-rv_tpad.x_longitude, coord_bak.x_longitude_bak)
                ELSE COALESCE(rv_tpad.x_longitude, coord_bak.x_longitude_bak)
                END AS x_longitude,
                COALESCE(rv_tpad.y_latitude, coord_bak.y_latitude_bak) AS y_latitude,
                rv_tpad.state_province_code,
                COALESCE(rv_tpad.irrigation, 'NONE') AS irrigation,
                --CASE 
                --    WHEN LOWER(rv_fe_pcrpc.value) = 'corn' THEN 2
                --    WHEN rv_fe_pcrpc.value IS NOT NULL THEN 0
                --    ELSE 1
                --END AS previous_crop_corn,
                rv_tpad.material_id,
                rv_tpad.be_bid,
                COALESCE(rv_cmt.be_bid, rv_bba.be_bid) AS match_be_bid,
                CASE
                    WHEN rv_cmt.fp_het_pool != 'pool2' THEN COALESCE(rv_cmt.fp_be_bid, rv_bba.receiver_p)
                    WHEN rv_cmt.mp_het_pool = 'pool1' THEN COALESCE(rv_cmt.mp_be_bid, rv_bba.donor_p)
                    ELSE rv_bba.receiver_p
                END AS par_hp1_be_bid,
                CASE
                    WHEN rv_cmt.fp_het_pool != 'pool2' THEN rv_cmt.fp_sample
                    WHEN rv_cmt.mp_het_pool = 'pool1' THEN rv_cmt.mp_sample
                    ELSE NULL
                END AS par_hp1_sample,
                CASE
                    WHEN rv_cmt.fp_het_pool = 'pool2' THEN COALESCE(rv_cmt.fp_be_bid, rv_bba.receiver_p)
                    WHEN rv_cmt.mp_het_pool != 'pool1' THEN COALESCE(rv_cmt.mp_be_bid, rv_bba.donor_p)
                    ELSE rv_bba.donor_p
                END AS par_hp2_be_bid,
                CASE
                    WHEN rv_cmt.fp_het_pool = 'pool2' THEN rv_cmt.fp_sample
                    WHEN rv_cmt.mp_het_pool != 'pool1' THEN rv_cmt.mp_sample
                    ELSE NULL
                END AS par_hp2_sample,
                rv_cmt.fp_het_group,
                rv_cmt.mp_het_group,
                CASE WHEN rv_tpad.cpifl THEN 1 ELSE 0 END AS cpifl,
                COALESCE(CAST(rv_tpad.cperf AS integer), CASE WHEN rv_tpad.cpifl THEN 1 ELSE 0 END) AS cperf,
                rv_tpad.trait_measure_code AS trait,
                rv_tpad.result_numeric_value AS result
              FROM managed.rv_trial_pheno_analytic_dataset rv_tpad
            LEFT JOIN (
                SELECT
                    loc_code,
                    AVG(x_longitude_bak) AS x_longitude_bak,
                    AVG(y_latitude_bak) AS y_latitude_bak
                  FROM (
                    SELECT DISTINCT
                        loc_code,
                        CASE WHEN country_code IN ('US', 'CA') AND x_longitude > 0
                            THEN - x_longitude
                        ELSE x_longitude
                        END AS x_longitude_bak,
                        y_latitude AS y_latitude_bak
                      FROM managed.rv_trial_pheno_analytic_dataset
                    WHERE x_longitude IS NOT NULL
                    AND y_latitude IS NOT NULL
                    AND ap_data_sector = 'CORN_BRAZIL_SAFRINHA'
                ) coord_bak_raw
                GROUP BY loc_code
            ) coord_bak
              ON rv_tpad.loc_code = coord_bak.loc_code
            LEFT JOIN (
                SELECT
                    source_id,
                    SUBSTRING(value,4,6) as value
                  FROM managed.rv_feature_export
                WHERE feature_name = 'TPP_REGION'
                  AND feature_level = 'LOCATION'
                  --AND feature_provider = 'SPIRIT'
                  AND market_name = 'CORN_BRAZIL_SAFRINHA'
                  AND CAST(year AS integer) = {0}
            ) rv_fe
              ON rv_tpad.loc_selector = rv_fe.source_id
            --LEFT JOIN(
            --    SELECT
            --        source_id,
            --      value
            --      FROM managed.rv_feature_export
            --    WHERE feature_name = 'PCRPC'
            --      AND feature_level = 'LOCATION'
            --      AND feature_provider = 'SPIRIT'
            --     AND market_name = 'CORN_NA_SUMMER'
            --     AND CAST(year AS integer) = {0}
            --) rv_fe_pcrpc
            --ON rv_tpad.loc_selector = rv_fe_pcrpc.source_id
            LEFT JOIN managed.rv_corn_material_tester_adapt rv_cmt
              ON rv_tpad.be_bid = rv_cmt.be_bid
            LEFT JOIN managed.rv_be_bid_ancestry_laas rv_bba
              ON rv_tpad.be_bid = rv_bba.be_bid
            WHERE NOT rv_tpad.pr_exclude
              AND NOT rv_tpad.psp_exclude
              AND NOT rv_tpad.tr_exclude
              AND LOWER(rv_tpad.material_id) NOT IN ('filler', 'purple')
              AND rv_tpad.crop_type_code LIKE 'YG%'
              --AND rv_tpad.bg_code IN ('AB','BD','BF','BG','BL','CC','DE','DL','DW','EG','GF','GL','GU','HE','HT','IB','JR',
              --        'KF','LC','LD','LR','LT','LX','ME','MY','NC','NE','NW','RL','SE','SY','TP','WE','XB','ZY','GY','MB','DB')
              AND ((rv_tpad.trait_measure_code = 'YGSMN' AND rv_tpad.result_numeric_value > 0 AND rv_tpad.result_numeric_value < 300)
                  OR (rv_tpad.trait_measure_code ='GMSTP' AND rv_tpad.result_numeric_value >= 4.5 AND rv_tpad.result_numeric_value < 35)
                  OR (rv_tpad.trait_measure_code ='HAVPN'))
              AND rv_tpad.year = {0}
              AND rv_tpad.ap_data_sector = 'CORN_BRAZIL_SAFRINHA'
            """.format(trial_year))

    return output_df


def get_trial_data_SOY_BRAZIL(trial_year):
    with DenodoConnection() as dc:
        output_df = dc.get_data("""
               SELECT
               CAST(rv_tpad.year AS integer) AS year,
               rv_tpad.ap_data_sector,
               COALESCE(rv_fe.value, 
                        --CAST(rv_tpad.maturity_group AS varchar), 
                        SUBSTRING(rv_tpad.loc_selector,0,1)) AS tpp_region,
               rv_tpad.experiment_id,
               rv_tpad.trial_id,
               rv_tpad.trial_status,
               rv_tpad.trial_stage,
               rv_tpad.loc_selector,
               rv_tpad.plot_barcode,
               rv_tpad.plant_date,
               rv_tpad.harvest_date,
               CASE WHEN country_code IN ('US', 'CA') AND x_longitude > 0
                   THEN COALESCE(-rv_tpad.x_longitude, coord_bak.x_longitude_bak)
               ELSE COALESCE(rv_tpad.x_longitude, coord_bak.x_longitude_bak)
               END AS x_longitude,
               COALESCE(rv_tpad.y_latitude, coord_bak.y_latitude_bak) AS y_latitude,
               rv_tpad.state_province_code,
               COALESCE(rv_tpad.irrigation, 'NONE') AS irrigation,
               --CASE 
               --    WHEN LOWER(rv_fe_pcrpc.value) = 'corn' THEN 2
               --    WHEN rv_fe_pcrpc.value IS NOT NULL THEN 0
               --    ELSE 1
               --END AS previous_crop_corn,
               rv_tpad.material_id,
               rv_tpad.be_bid,
               rv_bba.be_bid AS match_be_bid,
               samp_query.sample_id as sample_id,
               CASE WHEN rv_tpad.cpifl THEN 1 ELSE 0 END AS cpifl,
               COALESCE(CAST(rv_tpad.cperf AS integer), CASE WHEN rv_tpad.cpifl THEN 1 ELSE 0 END) AS cperf,
               rv_tpad.trait_measure_code AS trait,
               rv_tpad.result_numeric_value AS result
              FROM managed.rv_trial_pheno_analytic_dataset rv_tpad
            LEFT JOIN (
                SELECT
                    loc_code,
                    AVG(x_longitude_bak) AS x_longitude_bak,
                    AVG(y_latitude_bak) AS y_latitude_bak
                  FROM (
                    SELECT DISTINCT
                        loc_code,
                        CASE WHEN country_code IN ('US', 'CA') AND x_longitude > 0
                            THEN - x_longitude
                        ELSE x_longitude
                        END AS x_longitude_bak,
                        y_latitude AS y_latitude_bak
                      FROM managed.rv_trial_pheno_analytic_dataset
                    WHERE x_longitude IS NOT NULL
                    AND y_latitude IS NOT NULL
                    AND ap_data_sector = 'SOY_BRAZIL_SUMMER'
                ) coord_bak_raw
                GROUP BY loc_code
            ) coord_bak
              ON rv_tpad.loc_code = coord_bak.loc_code
            LEFT JOIN (
                SELECT
                    source_id,
                    SUBSTRING(value,4,6) as value
                  FROM managed.rv_feature_export
                WHERE feature_name = 'TPP_REGION'
                  AND feature_level = 'LOCATION'
                  --AND feature_provider = 'SPIRIT'
                  AND market_name = 'SOY_BRAZIL_SUMMER'
                  AND CAST(year AS integer) = {0}
            ) rv_fe
              ON rv_tpad.loc_selector = rv_fe.source_id
            --LEFT JOIN(
            --    SELECT
            --        source_id,
            --      value
            --      FROM managed.rv_feature_export
            --    WHERE feature_name = 'PCRPC'
            --      AND feature_level = 'LOCATION'
            --      AND feature_provider = 'SPIRIT'
            --     AND market_name = 'CORN_NA_SUMMER'
            --     AND CAST(year AS integer) = {0}
            --) rv_fe_pcrpc
            --ON rv_tpad.loc_selector = rv_fe_pcrpc.source_id
            --LEFT JOIN managed.rv_corn_material_tester_adapt rv_cmt
            --  ON rv_tpad.be_bid = rv_cmt.be_bid
            LEFT JOIN managed.rv_be_bid_ancestry_laas rv_bba
              ON rv_tpad.be_bid = rv_bba.be_bid
              left JOIN(
              select
    "bb_mat_info"."be_bid",
    "material_info"."sample_id"
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
              ) "samp_query" on "rv_tpad"."be_bid" = "samp_query"."be_bid"
            WHERE NOT rv_tpad.pr_exclude
              AND NOT rv_tpad.psp_exclude
              AND NOT rv_tpad.tr_exclude
              AND LOWER(rv_tpad.material_id) NOT IN ('filler', 'purple')
              AND rv_tpad.crop_type_code LIKE 'YG%'
              --AND rv_tpad.bg_code IN ('AB','BD','BF','BG','BL','CC','DE','DL','DW','EG','GF','GL','GU','HE','HT','IB','JR',
              --        'KF','LC','LD','LR','LT','LX','ME','MY','NC','NE','NW','RL','SE','SY','TP','WE','XB','ZY','GY','MB','DB')
              AND ((rv_tpad.trait_measure_code = 'YGSMN' AND rv_tpad.result_numeric_value > 0 AND rv_tpad.result_numeric_value < 300)
                  OR (rv_tpad.trait_measure_code ='GMSTP' AND rv_tpad.result_numeric_value >= 4.5 AND rv_tpad.result_numeric_value < 35))
            --      OR (rv_tpad.trait_measure_code ='HAVPN'))
              AND rv_tpad.year = {0}
              AND rv_tpad.ap_data_sector = 'SOY_BRAZIL_SUMMER'
              """.format(trial_year))

        return output_df


def get_trial_data_SUNFLOWER_EAME_SUMMER(trial_year):
    with DenodoConnection() as dc:
        output_df = dc.get_data("""
               SELECT
               CAST(rv_tpad.year AS integer) AS year,
               rv_tpad.ap_data_sector,
               COALESCE(rv_fe.value, 
                        --CAST(rv_tpad.maturity_group AS varchar), 
                        SUBSTRING(rv_tpad.loc_selector,0,1)) AS tpp_region,
               rv_tpad.experiment_id,
               rv_tpad.trial_id,
               rv_tpad.trial_status,
               rv_tpad.trial_stage,
               rv_tpad.loc_selector,
               rv_tpad.plot_barcode,
               rv_tpad.plant_date,
               rv_tpad.harvest_date,
               CASE WHEN country_code IN ('US', 'CA') AND x_longitude > 0
                   THEN COALESCE(-rv_tpad.x_longitude, coord_bak.x_longitude_bak)
               ELSE COALESCE(rv_tpad.x_longitude, coord_bak.x_longitude_bak)
               END AS x_longitude,
               COALESCE(rv_tpad.y_latitude, coord_bak.y_latitude_bak) AS y_latitude,
               rv_tpad.state_province_code,
               COALESCE(rv_tpad.irrigation, 'NONE') AS irrigation,
               --CASE 
               --    WHEN LOWER(rv_fe_pcrpc.value) = 'corn' THEN 2
               --    WHEN rv_fe_pcrpc.value IS NOT NULL THEN 0
               --    ELSE 1
               --END AS previous_crop_corn,
               rv_tpad.material_id,
               rv_tpad.be_bid,
               rv_bba.be_bid AS match_be_bid,
               CASE WHEN rv_tpad.cpifl THEN 1 ELSE 0 END AS cpifl,
               COALESCE(CAST(rv_tpad.cperf AS integer), CASE WHEN rv_tpad.cpifl THEN 1 ELSE 0 END) AS cperf,
               rv_tpad.trait_measure_code AS trait,
               rv_tpad.result_numeric_value AS result
              FROM managed.rv_trial_pheno_analytic_dataset rv_tpad
            LEFT JOIN (
                SELECT
                    loc_code,
                    AVG(x_longitude_bak) AS x_longitude_bak,
                    AVG(y_latitude_bak) AS y_latitude_bak
                  FROM (
                    SELECT DISTINCT
                        loc_code,
                        CASE WHEN country_code IN ('US', 'CA') AND x_longitude > 0
                            THEN - x_longitude
                        ELSE x_longitude
                        END AS x_longitude_bak,
                        y_latitude AS y_latitude_bak
                      FROM managed.rv_trial_pheno_analytic_dataset
                    WHERE x_longitude IS NOT NULL
                    AND y_latitude IS NOT NULL
                    AND ap_data_sector = 'SUNFLOWER_EAME_SUMMER'
                ) coord_bak_raw
                GROUP BY loc_code
            ) coord_bak
              ON rv_tpad.loc_code = coord_bak.loc_code
            LEFT JOIN (
                SELECT
                    source_id,value
                    --SUBSTRING(value,-2,1) as value
                  FROM managed.rv_feature_export
                WHERE feature_name = 'EnvironmentType_Hierarchical_Level0_Code'
                  AND feature_level = 'TRIAL'
                  --AND feature_provider = 'SPIRIT'
                  and market_name = 'SUNFLOWER_EAME_SUMMER'
                  AND CAST(year AS integer) = {0}
            ) rv_fe
              ON rv_tpad.loc_selector = rv_fe.source_id
            --LEFT JOIN(
            --    SELECT
            --        source_id,
            --      value
            --      FROM managed.rv_feature_export
            --    WHERE feature_name = 'PCRPC'
            --      AND feature_level = 'LOCATION'
            --      AND feature_provider = 'SPIRIT'
            --     AND market_name = 'CORN_NA_SUMMER'
            --     AND CAST(year AS integer) = {0}
            --) rv_fe_pcrpc
            --ON rv_tpad.loc_selector = rv_fe_pcrpc.source_id
            --LEFT JOIN managed.rv_corn_material_tester_adapt rv_cmt
            --  ON rv_tpad.be_bid = rv_cmt.be_bid
            LEFT JOIN managed.rv_be_bid_ancestry_laas rv_bba
              ON rv_tpad.be_bid = rv_bba.be_bid
            WHERE NOT rv_tpad.pr_exclude
              AND NOT rv_tpad.psp_exclude
              AND NOT rv_tpad.tr_exclude
              AND LOWER(rv_tpad.material_id) NOT IN ('filler', 'purple')
              AND rv_tpad.crop_type_code LIKE 'YG%'
              --AND rv_tpad.bg_code IN ('AB','BD','BF','BG','BL','CC','DE','DL','DW','EG','GF','GL','GU','HE','HT','IB','JR',
              --        'KF','LC','LD','LR','LT','LX','ME','MY','NC','NE','NW','RL','SE','SY','TP','WE','XB','ZY','GY','MB','DB')
              AND ((rv_tpad.trait_measure_code = 'YGSMN' AND rv_tpad.result_numeric_value > 0 AND rv_tpad.result_numeric_value < 300)
                  OR (rv_tpad.trait_measure_code ='GMSTP' AND rv_tpad.result_numeric_value >= 4.5 AND rv_tpad.result_numeric_value < 35))
            --      OR (rv_tpad.trait_measure_code ='HAVPN'))
              AND rv_tpad.year = {0}
              AND rv_tpad.ap_data_sector = 'SUNFLOWER_EAME_SUMMER'
              """.format(trial_year))

        return output_df


def get_trial_data_SOY_NA(trial_year):
    with DenodoConnection() as dc:
        output_df = dc.get_data("""
               SELECT
               CAST(rv_tpad.year AS integer) AS year,
               rv_tpad.ap_data_sector,
               COALESCE(rv_fe.value, 
                        --CAST(rv_tpad.maturity_group AS varchar), 
                        SUBSTRING(rv_tpad.loc_selector,0,1)) AS maturity_zone,
               rv_tpad.experiment_id,
               rv_tpad.trial_id,
               rv_tpad.trial_status,
               rv_tpad.trial_stage,
               rv_tpad.loc_selector,
               rv_tpad.plot_barcode,
               rv_tpad.plant_date,
               rv_tpad.harvest_date,
               CASE WHEN country_code IN ('US', 'CA') AND x_longitude > 0
                   THEN COALESCE(-rv_tpad.x_longitude, coord_bak.x_longitude_bak)
               ELSE COALESCE(rv_tpad.x_longitude, coord_bak.x_longitude_bak)
               END AS x_longitude,
               COALESCE(rv_tpad.y_latitude, coord_bak.y_latitude_bak) AS y_latitude,
               rv_tpad.state_province_code,
               COALESCE(rv_tpad.irrigation, 'NONE') AS irrigation,
               CASE 
                   WHEN LOWER(rv_fe_pcrpc.value) = 'soybean' THEN 2
                   WHEN rv_fe_pcrpc.value IS NOT NULL THEN 0
                   ELSE 1
               END AS previous_crop_soy,
               rv_tpad.material_id,
               rv_tpad.be_bid,
               rv_bba.be_bid AS match_be_bid,
               samp_query.sample_id as sample_id,
               CASE WHEN rv_tpad.cpifl THEN 1 ELSE 0 END AS cpifl,
               COALESCE(CAST(rv_tpad.cperf AS integer), CASE WHEN rv_tpad.cpifl THEN 1 ELSE 0 END) AS cperf,
               rv_tpad.trait_measure_code AS trait,
               rv_tpad.result_numeric_value AS result
              FROM managed.rv_trial_pheno_analytic_dataset rv_tpad
            LEFT JOIN (
                SELECT
                    loc_code,
                    AVG(x_longitude_bak) AS x_longitude_bak,
                    AVG(y_latitude_bak) AS y_latitude_bak
                  FROM (
                    SELECT DISTINCT
                        loc_code,
                        CASE WHEN country_code IN ('US', 'CA') AND x_longitude > 0
                            THEN - x_longitude
                        ELSE x_longitude
                        END AS x_longitude_bak,
                        y_latitude AS y_latitude_bak
                      FROM managed.rv_trial_pheno_analytic_dataset
                    WHERE x_longitude IS NOT NULL
                    AND y_latitude IS NOT NULL
                    AND ap_data_sector = 'SOY_NA_SUMMER'
                ) coord_bak_raw
                GROUP BY loc_code
            ) coord_bak
              ON rv_tpad.loc_code = coord_bak.loc_code
            LEFT JOIN (
                SELECT 
                source_id,
                    value
                  FROM managed.rv_feature_export
                WHERE feature_name = 'MATURITY_ZONE'
                  AND feature_level = 'LOCATION'
                  -- AND source = 'MYYIELD_SMARTMART'
                  AND market_name = 'SOY_NA_SUMMER'
                  AND CAST(year AS integer) = {0}
            ) rv_fe
              ON rv_tpad.loc_selector = rv_fe.source_id
            LEFT JOIN(
                SELECT
                    source_id,
                  value
                  FROM managed.rv_feature_export
                WHERE feature_name = 'PCRPC'
                  AND feature_level = 'LOCATION'
                  AND feature_provider = 'SPIRIT'
                 AND market_name = 'SOY_NA_SUMMER'
                 AND CAST(year AS integer) = {0}
            ) rv_fe_pcrpc
            ON rv_tpad.loc_selector = rv_fe_pcrpc.source_id
            --LEFT JOIN managed.rv_corn_material_tester_adapt rv_cmt
            --  ON rv_tpad.be_bid = rv_cmt.be_bid
            LEFT JOIN managed.rv_be_bid_ancestry_laas rv_bba
              ON rv_tpad.be_bid = rv_bba.be_bid
              left JOIN(
              select
    "bb_mat_info"."be_bid",
    "material_info"."sample_id"
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
              ) "samp_query" on "rv_tpad"."be_bid" = "samp_query"."be_bid"
            WHERE NOT rv_tpad.pr_exclude
              AND NOT rv_tpad.psp_exclude
              AND NOT rv_tpad.tr_exclude
              AND LOWER(rv_tpad.material_id) NOT IN ('filler', 'purple')
              AND rv_tpad.crop_type_code LIKE 'YG%'
              --AND rv_tpad.bg_code IN ('AB','BD','BF','BG','BL','CC','DE','DL','DW','EG','GF','GL','GU','HE','HT','IB','JR',
              --        'KF','LC','LD','LR','LT','LX','ME','MY','NC','NE','NW','RL','SE','SY','TP','WE','XB','ZY','GY','MB','DB')
              AND ((rv_tpad.trait_measure_code = 'YGSMN' AND rv_tpad.result_numeric_value > 0 AND rv_tpad.result_numeric_value < 300)
                  OR (rv_tpad.trait_measure_code ='GMSTP' AND rv_tpad.result_numeric_value >= 4.5 AND rv_tpad.result_numeric_value < 35))
            --      OR (rv_tpad.trait_measure_code ='HAVPN'))
              AND rv_tpad.year = {0}
              AND rv_tpad.ap_data_sector = 'SOY_NA_SUMMER'
              """.format(trial_year))

        return output_df


"""
get_snp_data

Queries managed.rv_assay_genotype_with_crop to get SNPs (at the marker_id level, should be translated to variant_id's) associated with a series of samples.

Input:
    executor: SQLExecutor2 object specifying connection information for Denodo managed
    sample_df: df containing at least the names of the samples to pull in a column named "sample"
    
Output:
    out_df: a tall df containing sample_id (str), marker_id (str), and geno_index (int8 1-4)
    
Author: Keri Rehm, 2023-Jun-13
"""


def get_snp_data(sample_df, crop_id_numeric=9):
    sample_list = "('" + sample_df["sample_id"].str.cat(sep="', '") + "')"
    with DenodoConnection() as dc:
        out_df = dc.get_data("""
        SELECT
            rv_agwc.sample_id AS sample_id,
            rv_agwc.assay_id AS marker_id,
            CASE 
                WHEN top_geno = 'A/A' THEN 1
                WHEN top_geno = 'C/C' THEN 2
                WHEN top_geno = 'G/G' THEN 3
                WHEN top_geno = 'T/T' THEN 4
                ELSE 0
            END AS geno_index
          FROM managed.rv_assay_genotype_with_crops rv_agwc
        WHERE CAST(ROUND(rv_agwc.crop_id,0) AS integer) = {1}
          AND rv_agwc.top_geno IN ('A/A', 'C/C', 'G/G', 'T/T')
          AND rv_agwc.sample_id IN {0}""".format(sample_list, crop_id_numeric))

    # print(query_str)
    print("Executing query")
    # out_df = executor.query_to_df(query_str, dtypes = {'sample_id': np.dtype('str'), 'marker_id': np.dtype('str'), 'geno_index': np.dtype('int8')})
    out_df = out_df.loc[out_df.geno_index > 0, :]

    return out_df


"""
get_snp_data_batch

Batched calls to get_snp_data(), and perform translation from marker_id to variant_id as well as filtering to used variant_id's to save on memory.

Input:
    executor: SQLExecutor2 object specifying connection information for Denodo managed
    sample_df: df containing at least the names of the samples to pull in a column named "sample"
    variant_set_df: df containing at least "marker_id" and "variant_id" mapping
    batch_size: number of samples to pull + process in a batch.
    
Output:
    output_df: df of ["sample_id", "variant_id", "geno_index"] for all samples in sample_df
    
Author: Keri Rehm, 2023-Jun-05
"""


def get_snp_data_batch(sample_df, variant_set_df, batch_size=1000, crop_id_numeric=9):
    sample_n = sample_df.shape[0]
    sample_n_iter = int(math.ceil(sample_n / batch_size))

    output_list = []
    for i in range(sample_n_iter):
        sample_geno_iter_df = get_snp_data(sample_df.iloc[i * batch_size:min((i + 1) * batch_size - 1, sample_n), :],
                                           crop_id_numeric)

        sample_geno_iter_df = sample_geno_iter_df.merge(variant_set_df,
                                                        on="marker_id",
                                                        how="inner").drop(columns=["marker_id"]).groupby(
            ["sample_id", "variant_id"]).first()

        output_list.append(sample_geno_iter_df)

    output_df = pd.concat(output_list)

    return output_df


"""
get_trial_bebid_list()

Retrieves a list of be_bids and the first year where they were used for select breeding groups in Corn NA Summer, with some QC rules applied.

Inputs:
    executor: SQLExecutor2 object specifying connection information for Denodo managed
    
Outputs:
    output_df: be_bids in trial_pheno and first year of usage

Author: Keri Rehm, 2023-Jun-05
"""


def get_trial_bebid_list():
    print("Retrieving trial bebid list")
    with DenodoConnection() as dc:
        output_df = dc.get_data("""
            SELECT
                min(year) AS year,
                be_bid
              FROM managed.rv_trial_pheno_analytic_dataset
            WHERE ap_data_sector = 'CORN_NA_SUMMER'
              AND year >= 2014
              AND NOT pr_exclude
              AND NOT psp_exclude
              AND NOT tr_exclude
              AND LOWER(material_id) NOT IN ('filler', 'purple')
              AND crop_type_code LIKE 'YG%'
--              AND bg_code IN ('AB','BD','BF','BG','BL','CC','DE','DL','DW','EG','GF','GL','GU','HE','HT','IB','JR',
--                      'KF','LC','LD','LR','LT','LX','ME','MY','NC','NE','NW','RL','SE','SY','TP','WE','XB','ZY','GY','MB','DB')
              AND (trait_measure_code = 'YGSMN' AND result_numeric_value > 0 AND result_numeric_value < 300)
            GROUP BY be_bid
            """)

    return output_df


"""
get_cmt()

Retrieves corn_material_tester from its PostgreSQL source. This is more reliable/faster than Denodo advancement view 
and isn't at risk for being out-of-date like Denodo managed view in nonprod environments. But best practice would be to use the managed view.

Inputs:
    executor: SQLExecutor2 object specifying connection information for PostgreSQL advancement db
    
Outputs:
    output_df: Selected columns from corn_material_tester

Author: Keri Rehm, 2023-Jun-05
"""


def get_cmt():
    print("Retrieving corn material tester")
    with DenodoConnection() as dc:
        output_df = dc.get_data("""
        SELECT
            be_bid,
            fp_be_bid,
            fp_sample,
            fp_het_pool,
            mp_be_bid,
            mp_sample,
            mp_het_pool
          FROM dme.corn_material_tester
        WHERE fp_be_bid IS NOT NULL
          OR fp_sample IS NOT NULL
          OR mp_be_bid IS NOT NULL
          OR mp_sample IS NOT NULL
        """)

    return output_df


"""
get_het_pools()

Retrieves a df of [year, sample_id] or [year, be_bid] for het pools 1&2 that were first used in the specified year in Corn NA Summer, with some QC rules applied.

Inputs:
    executor: SQLExecutor2 object specifying connection information for Denodo managed
    year: year to search
    id_type: Specify either 'sample' or 'be_bid' to return hp df's with corresponding identifier
    
Outputs:
    hp1_df: [year, sample_id] or [year, be_bid] df of that identifier that was first used in het pool 1 in specified year
    hp1_df: [year, sample_id] or [year, be_bid] df of that identifier that was first used in het pool 2 in specified year

Author: Keri Rehm, 2023-Jun-05
"""


def get_het_pools(year, id_type, region_lid, season_id):
    with DenodoConnection() as dc:
        sql_df = dc.get_data("""
    SELECT
        CAST(rv_bb_ete.year AS integer) AS year,
        rv_cmt.be_bid,
        CASE
            WHEN rv_cmt.fp_het_pool != 'pool2' THEN COALESCE(rv_cmt.fp_be_bid, rv_bba.receiver_p)
            WHEN rv_cmt.mp_het_pool = 'pool1' THEN COALESCE(rv_cmt.mp_be_bid, rv_bba.donor_p)
            ELSE rv_bba.receiver_p
        END AS par_hp1_be_bid,
        CASE
            WHEN rv_cmt.fp_het_pool != 'pool2' THEN rv_cmt.fp_sample
            WHEN rv_cmt.mp_het_pool = 'pool1' THEN rv_cmt.mp_sample
            ELSE NULL
        END AS par_hp1_sample,
        CASE
            WHEN rv_cmt.fp_het_pool = 'pool2' THEN COALESCE(rv_cmt.fp_be_bid, rv_bba.receiver_p)
            WHEN rv_cmt.mp_het_pool != 'pool1' THEN COALESCE(rv_cmt.mp_be_bid, rv_bba.donor_p)
            ELSE rv_bba.donor_p
        END AS par_hp2_be_bid,
        CASE
            WHEN rv_cmt.fp_het_pool = 'pool2' THEN rv_cmt.fp_sample
            WHEN rv_cmt.mp_het_pool != 'pool1' THEN rv_cmt.mp_sample
            ELSE NULL
        END AS par_hp2_sample
      FROM (
        SELECT DISTINCT
            min(rv_bb_ete.year) AS year,
            rv_bb_mat.be_bid
          FROM managed.rv_bb_experiment_trial_entry_sdl rv_bb_ete
        INNER JOIN managed.rv_bb_material_sdl rv_bb_mat
          ON rv_bb_ete.planted_material_guid = rv_bb_mat.material_guid
        INNER JOIN managed.rv_bb_location_sdl rv_bb_loc
          ON rv_bb_ete.location_guid = rv_bb_loc.location_guid
        WHERE rv_bb_ete.crop_code = 'CORN'
          AND rv_bb_ete.season = '{2}'
          AND rv_bb_loc.region_lid = '{1}'
          AND rv_bb_ete.year <= {0}
          AND rv_bb_ete.year >= 2014
          AND rv_bb_ete.exclude_from_analysis < 1
          AND LOWER(rv_bb_mat.material_id) NOT IN ('filler', 'purple')
          AND rv_bb_ete.trial_crop_type_lid LIKE 'YG%'
        GROUP BY rv_bb_mat.be_bid
    ) rv_bb_ete
    LEFT JOIN managed.rv_corn_material_tester_adapt rv_cmt
      ON rv_bb_ete.be_bid = rv_cmt.be_bid
    LEFT JOIN managed.rv_be_bid_ancestry_laas rv_bba
      ON rv_bb_ete.be_bid = rv_bba.be_bid""".format(year, region_lid, season_id))

    # Calculate material list at sample or be_bid level depending on provided argument
    if id_type == 'sample':
        hp1_df = sql_df.loc[sql_df["par_hp1_sample"].notnull(), ["year", "par_hp1_sample"]] \
            .rename(columns={'par_hp1_sample': 'sample_id'}) \
            .groupby(['sample_id']).min().reset_index()

        hp2_df = sql_df.loc[sql_df["par_hp2_sample"].notnull(), ["year", "par_hp2_sample"]] \
            .rename(columns={'par_hp2_sample': 'sample_id'}) \
            .groupby(['sample_id']).min().reset_index()

    else:
        hp1_df = sql_df.loc[sql_df["par_hp1_be_bid"].notnull(), ["year", "par_hp1_be_bid"]] \
            .rename(columns={'par_hp1_be_bid': 'be_bid'}) \
            .groupby(['be_bid']).min().reset_index()

        hp2_df = sql_df.loc[sql_df["par_hp2_be_bid"].notnull(), ["year", "par_hp2_be_bid"]] \
            .rename(columns={'par_hp2_be_bid': 'be_bid'}) \
            .groupby(['be_bid']).min().reset_index()

    hp1_df = hp1_df.loc[hp1_df["year"] == int(year),].reset_index(drop=True)
    hp2_df = hp2_df.loc[hp2_df["year"] == int(year),].reset_index(drop=True)

    return hp1_df, hp2_df


def get_het_pools_soy(year, id_type, region_lid):
    with DenodoConnection() as dc:
        sql_df = dc.get_data("""
SELECT
    CAST(rv_bb_ete.year AS integer) AS year,
    rv_bb_ete.be_bid,
    comp_set.sample_id
FROM
    (
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
    ) rv_bb_ete
    INNER JOIN(
        select
            *
        from(
                SELECT
                    "bb_mat"."material_guid",
                    "bb_mat"."be_bid"
                FROM
                    "managed"."rv_bb_material_sdl" "bb_mat"
                --WHERE
                --    "bb_mat"."crop_guid" = '6C9085C2-C442-48C4-ACA7-427C4760B642'
                --    AND "bb_mat"."be_bid" LIKE 'ESY%'
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
    ) "comp_set" on rv_bb_ete.be_bid = comp_set.be_bid""".format(year, region_lid))

        hp_df = sql_df.loc[sql_df["sample_id"].notnull(), ["year", "sample_id"]] \
            .groupby(['sample_id']).min().reset_index()

        hp_df = hp_df.loc[hp_df["year"] == int(year),].reset_index(drop=True)

        return hp_df


def giga_sunflower(be_bid_df, comp_set_be_bid, batch_size):
    df_giga = pd.DataFrame()
    sample_n = len(be_bid_df["be_bid"].drop_duplicates())
    print('sample_n: ', sample_n)
    sample_n_iter = int(math.ceil(sample_n / batch_size))
    print('sample_n_iter: ', sample_n_iter)

    for i in range(sample_n_iter):
        be_bid_list = "('" + be_bid_df.iloc[i * batch_size:min((i + 1) * batch_size - 1, sample_n), :][
            "be_bid"].drop_duplicates().str.cat(sep="', '") + "')"

        comp_set_be_bid_list = "('" + comp_set_be_bid["be_bid"].drop_duplicates().str.cat(sep="', '") + "')"

        with DenodoConnection() as dc:
            print('be_bid_df.shape: ', be_bid_df.shape)
            print('comp_set_be_bid.shape: ', comp_set_be_bid.shape)

            giga_df = dc.get_data("""
            SELECT be_bid_1, be_bid_2, distance_shared_allele  FROM managed.rv_giga_genetic_distance
            WHERE crop_name = 'sunflower'
            AND ((be_bid_1 in {0})
            AND (be_bid_2 in {1}))
            --AND ((be_bid_1 in ('EMF0000000786','EMF0000001245')))
            --AND (be_bid_2 in ('EMF0000000786','EMF0000001245')))
            and be_bid_1 <> be_bid_2 
            """.format(be_bid_list, comp_set_be_bid_list))

        df_giga = pd.concat([df_giga, giga_df], ignore_index=True)

        print('i: ', i, 'df_giga.shape: ', df_giga.shape)

    return df_giga


def get_het_pools_sunflower_init(year):
    with DenodoConnection() as dc:
        sql_df = dc.get_data("""
        SELECT
            DISTINCT min(rv_bb_ete.year) AS year,
            rv_bb_mat.be_bid
        FROM
            managed.rv_bb_experiment_trial_entry_sdl rv_bb_ete
            INNER JOIN managed.rv_bb_material_sdl rv_bb_mat ON rv_bb_ete.planted_material_guid = rv_bb_mat.material_guid
            INNER JOIN managed.rv_bb_location_sdl rv_bb_loc ON rv_bb_ete.location_guid = rv_bb_loc.location_guid
        WHERE
            rv_bb_ete.crop_code = 'SUN'
            AND rv_bb_ete.season = 'SUMR'
            AND rv_bb_loc.region_lid = 'EAME'
            AND rv_bb_ete.year <= {0}
            AND rv_bb_ete.year >= 2014
            AND rv_bb_ete.exclude_from_analysis < 1
            AND LOWER(rv_bb_mat.material_id) NOT IN ('filler', 'purple')
            AND rv_bb_ete.trial_crop_type_lid LIKE 'YG%'
        GROUP BY
            rv_bb_mat.be_bid""".format(year))

        return sql_df


def get_het_pools_sunflower_receiver():
    with DenodoConnection() as dc:
        sql_df = dc.get_data("""    
    select
    rv_laas.be_bid,
    rv_laas.receiver_p,
    bid_sample_query.sample_id as receiver_sample_id
from
    managed.rv_be_bid_ancestry_laas rv_laas
    inner join(
select query_res.be_bid as "be_bid", sample_id
from (
select
            "bb_mat_info"."be_bid","material_info"."sample_id","material_info"."genotype_count"
            from
            (
                select "bb_mat"."material_guid", "bb_mat"."be_bid"
                FROM
                    "managed"."rv_bb_material_sdl" "bb_mat"
                WHERE
                    "bb_mat"."crop_guid" = '824A412A-2EA6-4288-832D-16E165BD033E' AND "bb_mat"."be_bid" LIKE 'ESU%'
            ) "bb_mat_info"
            inner join (
                SELECT
                    "assay_size"."sample_id", "sample_api"."germplasm_guid", "sample_api"."germplasm_id", "assay_size"."genotype_count"
                FROM
                    "managed"."rv_sample_api" "sample_api"
                    INNER JOIN (
                        select *
                        from (
                                select "sample_id" AS "sample_id", "techno_code" AS "techno_code", COUNT("genotype") AS "genotype_count"
                                FROM
                                    "managed"."rv_assay_genotype_with_crops"
                                WHERE
                                    (("techno_code" = 'AX') OR ("techno_code" = 'II')) AND ("crop_id" = 12) AND "top_geno" IN ('A/A', 'C/C', 'G/G', 'T/T')
                                GROUP BY
                                    "sample_id","techno_code"
                            ) "unfiltered_query"
                        where "genotype_count" > 99
                    ) "assay_size" ON "sample_api"."sample_code" = "assay_size"."sample_id" AND "sample_api"."crop_id" = 12
            ) "material_info" on "bb_mat_info"."material_guid" = "material_info"."germplasm_guid"
            ) "query_res"
            inner join(
select be_bid, MAX(genotype_count) as max_genotype_count
from(
select "bb_mat_info"."be_bid","material_info"."sample_id","material_info"."genotype_count"
            from
            (
                select "bb_mat"."material_guid", "bb_mat"."be_bid"
                FROM
                    "managed"."rv_bb_material_sdl" "bb_mat"
                WHERE
                    "bb_mat"."crop_guid" = '824A412A-2EA6-4288-832D-16E165BD033E' AND "bb_mat"."be_bid" LIKE 'ESU%'
            ) "bb_mat_info"
            inner join (
                SELECT
                    "assay_size"."sample_id","sample_api"."germplasm_guid","sample_api"."germplasm_id","assay_size"."genotype_count"
                FROM
                    "managed"."rv_sample_api" "sample_api"
                    INNER JOIN (
                        select *
                        FROM
                            (
                                SELECT
                                    "sample_id" AS "sample_id","techno_code" AS "techno_code",COUNT("genotype") AS "genotype_count"
                                FROM
                                    "managed"."rv_assay_genotype_with_crops"
                                WHERE
                                    (("techno_code" = 'AX') OR ("techno_code" = 'II')) AND ("crop_id" = 12) AND "top_geno" IN ('A/A', 'C/C', 'G/G', 'T/T')
                                GROUP BY
                                    "sample_id", "techno_code"
                            ) "unfiltered_query"
                        where "genotype_count" > 99
                    ) "assay_size" ON "sample_api"."sample_code" = "assay_size"."sample_id" AND "sample_api"."crop_id" = 12
            ) "material_info" on "bb_mat_info"."material_guid" = "material_info"."germplasm_guid"
            ) group by be_bid
            ) "max_res" on "max_res"."max_genotype_count" = "query_res"."genotype_count" and "max_res"."be_bid" = "query_res"."be_bid"
            ) "bid_sample_query" on "rv_laas"."receiver_p" = "bid_sample_query"."be_bid"
""")
        return sql_df


def get_het_pools_sunflower_donor():
    with DenodoConnection() as dc:
        sql_df = dc.get_data("""    
    select
    rv_laas.be_bid,
    rv_laas.donor_p,
    bid_sample_query.sample_id as donor_sample_id
from
    managed.rv_be_bid_ancestry_laas rv_laas
    inner join(
select query_res.be_bid as "be_bid", sample_id
from (
select
            "bb_mat_info"."be_bid","material_info"."sample_id","material_info"."genotype_count"
            from
            (
                select "bb_mat"."material_guid", "bb_mat"."be_bid"
                FROM
                    "managed"."rv_bb_material_sdl" "bb_mat"
                WHERE
                    "bb_mat"."crop_guid" = '824A412A-2EA6-4288-832D-16E165BD033E' AND "bb_mat"."be_bid" LIKE 'ESU%'
            ) "bb_mat_info"
            inner join (
                SELECT
                    "assay_size"."sample_id", "sample_api"."germplasm_guid", "sample_api"."germplasm_id", "assay_size"."genotype_count"
                FROM
                    "managed"."rv_sample_api" "sample_api"
                    INNER JOIN (
                        select *
                        from (
                                select "sample_id" AS "sample_id", "techno_code" AS "techno_code", COUNT("genotype") AS "genotype_count"
                                FROM
                                    "managed"."rv_assay_genotype_with_crops"
                                WHERE
                                    (("techno_code" = 'AX') OR ("techno_code" = 'II')) AND ("crop_id" = 12) AND "top_geno" IN ('A/A', 'C/C', 'G/G', 'T/T')
                                GROUP BY
                                    "sample_id","techno_code"
                            ) "unfiltered_query"
                        where "genotype_count" > 99
                    ) "assay_size" ON "sample_api"."sample_code" = "assay_size"."sample_id" AND "sample_api"."crop_id" = 12
            ) "material_info" on "bb_mat_info"."material_guid" = "material_info"."germplasm_guid"
            ) "query_res"
            inner join(
select be_bid, MAX(genotype_count) as max_genotype_count
from(
select "bb_mat_info"."be_bid","material_info"."sample_id","material_info"."genotype_count"
            from
            (
                select "bb_mat"."material_guid", "bb_mat"."be_bid"
                FROM
                    "managed"."rv_bb_material_sdl" "bb_mat"
                WHERE
                    "bb_mat"."crop_guid" = '824A412A-2EA6-4288-832D-16E165BD033E' AND "bb_mat"."be_bid" LIKE 'ESU%'
            ) "bb_mat_info"
            inner join (
                SELECT
                    "assay_size"."sample_id","sample_api"."germplasm_guid","sample_api"."germplasm_id","assay_size"."genotype_count"
                FROM
                    "managed"."rv_sample_api" "sample_api"
                    INNER JOIN (
                        select *
                        FROM
                            (
                                SELECT
                                    "sample_id" AS "sample_id","techno_code" AS "techno_code",COUNT("genotype") AS "genotype_count"
                                FROM
                                    "managed"."rv_assay_genotype_with_crops"
                                WHERE
                                    (("techno_code" = 'AX') OR ("techno_code" = 'II')) AND ("crop_id" = 12) AND "top_geno" IN ('A/A', 'C/C', 'G/G', 'T/T')
                                GROUP BY
                                    "sample_id", "techno_code"
                            ) "unfiltered_query"
                        where "genotype_count" > 99
                    ) "assay_size" ON "sample_api"."sample_code" = "assay_size"."sample_id" AND "sample_api"."crop_id" = 12
            ) "material_info" on "bb_mat_info"."material_guid" = "material_info"."germplasm_guid"
            ) group by be_bid
            ) "max_res" on "max_res"."max_genotype_count" = "query_res"."genotype_count" and "max_res"."be_bid" = "query_res"."be_bid"
    ) "bid_sample_query" on "rv_laas"."donor_p" = "bid_sample_query"."be_bid"
""")
        return sql_df


def get_sunflower_be_bid_sample():
    with DenodoConnection() as dc:
        sql_df = dc.get_data(""" 
        select query_res.be_bid as "be_bid", sample_id
        from (
        select
            "bb_mat_info"."be_bid","material_info"."sample_id","material_info"."genotype_count"
            from
            (
                select "bb_mat"."material_guid", "bb_mat"."be_bid"
                FROM
                    "managed"."rv_bb_material_sdl" "bb_mat"
                WHERE
                    "bb_mat"."crop_guid" = '824A412A-2EA6-4288-832D-16E165BD033E' AND "bb_mat"."be_bid" LIKE 'ESU%'
            ) "bb_mat_info"
            inner join (
                SELECT
                    "assay_size"."sample_id", "sample_api"."germplasm_guid", "sample_api"."germplasm_id", "assay_size"."genotype_count"
                FROM
                    "managed"."rv_sample_api" "sample_api"
                    INNER JOIN (
                        select *
                        from (
                                select "sample_id" AS "sample_id", "techno_code" AS "techno_code", COUNT("genotype") AS "genotype_count"
                                FROM
                                    "managed"."rv_assay_genotype_with_crops"
                                WHERE
                                    (("techno_code" = 'AX') OR ("techno_code" = 'II')) AND ("crop_id" = 12) AND "top_geno" IN ('A/A', 'C/C', 'G/G', 'T/T')
                                GROUP BY
                                    "sample_id","techno_code"
                            ) "unfiltered_query"
                        where "genotype_count" > 99
                    ) "assay_size" ON "sample_api"."sample_code" = "assay_size"."sample_id" AND "sample_api"."crop_id" = 12
            ) "material_info" on "bb_mat_info"."material_guid" = "material_info"."germplasm_guid"
            ) "query_res"
            inner join(
select be_bid, MAX(genotype_count) as max_genotype_count
from(
select "bb_mat_info"."be_bid","material_info"."sample_id","material_info"."genotype_count"
            from
            (
                select "bb_mat"."material_guid", "bb_mat"."be_bid"
                FROM
                    "managed"."rv_bb_material_sdl" "bb_mat"
                WHERE
                    "bb_mat"."crop_guid" = '824A412A-2EA6-4288-832D-16E165BD033E' AND "bb_mat"."be_bid" LIKE 'ESU%'
            ) "bb_mat_info"
            inner join (
                SELECT
                    "assay_size"."sample_id","sample_api"."germplasm_guid","sample_api"."germplasm_id","assay_size"."genotype_count"
                FROM
                    "managed"."rv_sample_api" "sample_api"
                    INNER JOIN (
                        select *
                        FROM
                            (
                                SELECT
                                    "sample_id" AS "sample_id","techno_code" AS "techno_code",COUNT("genotype") AS "genotype_count"
                                FROM
                                    "managed"."rv_assay_genotype_with_crops"
                                WHERE
                                    (("techno_code" = 'AX') OR ("techno_code" = 'II')) AND ("crop_id" = 12) AND "top_geno" IN ('A/A', 'C/C', 'G/G', 'T/T')
                                GROUP BY
                                    "sample_id", "techno_code"
                            ) "unfiltered_query"
                        where "genotype_count" > 99
                    ) "assay_size" ON "sample_api"."sample_code" = "assay_size"."sample_id" AND "sample_api"."crop_id" = 12
            ) "material_info" on "bb_mat_info"."material_guid" = "material_info"."germplasm_guid"
            ) group by be_bid
            ) "max_res" on "max_res"."max_genotype_count" = "query_res"."genotype_count" and "max_res"."be_bid" = "query_res"."be_bid"
""")
        return sql_df


def get_sunflower_bebids():
    with DenodoConnection() as dc:
        output_df = dc.get_data("""select be_bid, donor_p, receiver_p 
                        from managed.rv_be_bid_ancestry_laas rv_laas
                        where rv_laas."be_bid" LIKE 'ESU%'
                        """)

        output_df1 = output_df.rename(columns={'donor_p': 'par_hp1_be_bid', 'receiver_p': 'par_hp2_be_bid'})

        return output_df1


def get_sunflower_receiver(be_bid_sample_df, batch_size):
    df_be_bid = pd.DataFrame()
    # sample_list = "('" + sample_df["be_bid"].drop_duplicates().str.cat(sep="', '") + "')"
    sample_n = len(be_bid_sample_df["be_bid"].drop_duplicates())
    print('sample_n: ', sample_n)
    sample_n_iter = int(math.ceil(sample_n / batch_size))
    print('sample_n_iter: ', sample_n_iter)

    for i in range(sample_n_iter):
        sample_list = "('" + be_bid_sample_df.iloc[i * batch_size:min((i + 1) * batch_size - 1, sample_n), :][
            "be_bid"].drop_duplicates().str.cat(sep="', '") + "')"

        with DenodoConnection() as dc:
            sql_df = dc.get_data("""    
                select
                rv_laas.be_bid,
                rv_laas.receiver_p
                from managed.rv_be_bid_ancestry_laas rv_laas
                where rv_laas.receiver_p IN {0}
            """.format(sample_list))

        df_be_bid = pd.concat([df_be_bid, sql_df], ignore_index=True)

    return df_be_bid


def get_sunflower_donor(be_bid_sample_df, batch_size):
    df_be_bid = pd.DataFrame()
    # sample_list = "('" + sample_df["be_bid"].drop_duplicates().str.cat(sep="', '") + "')"
    sample_n = len(be_bid_sample_df["be_bid"].drop_duplicates())
    print('sample_n: ', sample_n)
    sample_n_iter = int(math.ceil(sample_n / batch_size))
    print('sample_n_iter: ', sample_n_iter)

    for i in range(sample_n_iter):
        sample_list = "('" + be_bid_sample_df.iloc[i * batch_size:min((i + 1) * batch_size - 1, sample_n), :][
            "be_bid"].drop_duplicates().str.cat(sep="', '") + "')"

        with DenodoConnection() as dc:
            sql_df = dc.get_data("""    
                select
                rv_laas.be_bid,
                rv_laas.donor_p
                from managed.rv_be_bid_ancestry_laas rv_laas
                where rv_laas.donor_p IN {0}
            """.format(sample_list))

        df_be_bid = pd.concat([df_be_bid, sql_df], ignore_index=True)

    return df_be_bid


def giga_corn(be_bid_df, comp_set_be_bid, batch_size):
    df_giga = pd.DataFrame()

    sample_n_i = len(be_bid_df["be_bid"].drop_duplicates())
    print('sample_n_i: ', sample_n_i)
    sample_n_i_iter = int(math.ceil(sample_n_i / batch_size))
    print('sample_n_i_iter: ', sample_n_i_iter)

    sample_n_j = len(comp_set_be_bid["be_bid"].drop_duplicates())
    print('sample_n_j: ', sample_n_j)
    sample_n_j_iter = int(math.ceil(sample_n_j / batch_size))
    print('sample_n_j_iter: ', sample_n_j_iter)

    print('be_bid_df.shape: ', be_bid_df.shape)
    print('comp_set_be_bid.shape: ', comp_set_be_bid.shape)

    for j in range(sample_n_j_iter):

        comp_set_be_bid_list = "('" + comp_set_be_bid.iloc[j * batch_size:min((j + 1) * batch_size - 1, sample_n_j), :]["be_bid"].drop_duplicates().str.cat(sep="', '") + "')"
        df_giga_int = pd.DataFrame()
        for i in range(sample_n_i_iter):
            be_bid_list = "('" + be_bid_df.iloc[i * batch_size:min((i + 1) * batch_size - 1, sample_n_i), :]["be_bid"].drop_duplicates().str.cat(sep="', '") + "')"

            with DenodoConnection() as dc:

                giga_df = dc.get_data("""
                SELECT be_bid_1, be_bid_2, distance_shared_allele  FROM managed.rv_giga_genetic_distance
                WHERE crop_name = 'corn'
                AND ((be_bid_1 in {0})
                AND (be_bid_2 in {1}))
                --AND ((be_bid_1 in ('EMF0000000786','EMF0000001245')))
                --AND (be_bid_2 in ('EMF0000000786','EMF0000001245')))
                and be_bid_1 <> be_bid_2 
                """.format(be_bid_list, comp_set_be_bid_list))

            df_giga_int = pd.concat([df_giga_int, giga_df], ignore_index=True)
            print('i: ', i, 'df_giga_int.shape: ', df_giga_int.shape)

        df_giga = pd.concat([df_giga, df_giga_int], ignore_index=True)
        print('j: ', j, 'df_giga.shape: ', df_giga.shape)

    return df_giga

"""
get_variant_set

Retrieves the marker_id - variant_id mapping for a specified crop_id. This is done because managed.rv_variant_set 
is on Athena (slow) and doesn't join nice with Snowflake tables. Multiple markers (typically across sampling 
technologies) can map to a single variant_id.

Input:
    executor: SQLExecutor2 object specifying connection information for Denodo managed
    crop_id: number identifying crop in table (default 9 = field corn)
    
Output:
    output_df: ["marker_id", "variant_id"] df
    
Author: Keri Rehm, 2023-Jun-05
"""


def get_variant_set(crop_id=9):
    with DenodoConnection() as dc:
        output_df = dc.get_data("""
    SELECT
            marker_id,
            CAST(variant_id AS bigint) AS variant_id
          FROM managed.rv_variant_set
        WHERE CAST(crop_id AS integer) = {0}
          AND marker_id != variant_id
        """.format(int(crop_id)))

    return output_df


"""
get_variants_used()

WARNING: This mixes tables between Athena and Snowflake and is thus EXTREMELY SLOW. Do not use. Here for archival purposes
"""


def get_variants_used(hetpool_df):
    sample_list = pd.concat([hetpool_df['fp_sample'].rename(columns={'fp_sample': 'sample'}),
                             hetpool_df['mp_sample'].rename(columns={'mp_sample': 'sample'})]).drop_duplicates(
        ignore_index=True)

    with DenodoConnection() as dc:
        output_df = dc.get_data("""
    SELECT
        variant_id,
        COUNT(sample) AS sample_count
      FROM(
        SELECT 
            rv_cmta.fp_sample AS sample
        FROM (
            SELECT DISTINCT
                be_bid
              FROM managed.rv_trial_pheno_analytic_dataset
            WHERE ap_data_sector = 'CORN_NA_SUMMER'
            AND trait_measure_code = 'YGSMN'
        ) rv_tpad
        INNER JOIN managed.rv_corn_material_tester_adapt rv_cmta
          ON rv_tpad.be_bid = rv_cmta.be_bid
        WHERE rv_cmta.fp_be_bid IS NOT NULL
          AND rv_cmta.fp_sample IS NOT NULL

        UNION
        SELECT 
            rv_cmta.mp_sample AS sample
        FROM (
            SELECT DISTINCT
                be_bid
              FROM managed.rv_trial_pheno_analytic_dataset
            WHERE ap_data_sector = 'CORN_NA_SUMMER'
            AND trait_measure_code = 'YGSMN'
        ) rv_tpad
        INNER JOIN managed.rv_corn_material_tester_adapt rv_cmta
          ON rv_tpad.be_bid = rv_cmta.be_bid
        WHERE rv_cmta.mp_be_bid IS NOT NULL
          AND rv_cmta.mp_sample IS NOT NULL
    ) be_bid
    INNER JOIN managed.rv_assay_genotype_with_crops rv_agwc
    ON be_bid.sample = rv_agwc.sample_id
    AND CAST(ROUND(rv_agwc.crop_id,0) AS integer) = 9
    AND rv_agwc.top_geno IN ('A/A', 'C/C', 'G/G', 'T/T')
    INNER JOIN(
        SELECT
            marker_id,
            CAST(variant_id AS bigint) AS variant_id
          FROM managed.rv_variant_set
        WHERE CAST(crop_id AS integer) = 9
          AND marker_id != variant_id
    ) rv_vs
    ON rv_agwc.assay_id = rv_vs.marker_id
    GROUP BY
        variant_id
    """)

    return output_df


def get_cmt_bebid(sample_df, batch_size):
    df_cmt = pd.DataFrame()
    # sample_list = "('" + sample_df["be_bid"].drop_duplicates().str.cat(sep="', '") + "')"
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
                be_bid, lbg_bid, fp_sample, mp_sample, fp_be_bid, mp_be_bid, fp_het_pool, mp_het_pool, fp_het_group, mp_het_group
              FROM managed.rv_corn_material_tester_adapt rv_cmt
            WHERE rv_cmt.be_bid IN {0}
            AND rv_cmt.fp_het_pool IS NOT NULL
            AND rv_cmt.mp_het_pool IS NOT NULL""".format(sample_list))

        df_cmt = pd.concat([df_cmt, out_df], ignore_index=True)

        # print("Executing query")
        # print('i: ', i, ' of: ', sample_n_iter)
    return df_cmt


def get_laas_bebid(sample_df, batch_size):
    df_laas = pd.DataFrame()
    # sample_list = "('" + sample_df["be_bid"].drop_duplicates().str.cat(sep="', '") + "')"
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
                    be_bid, receiver_p, donor_p
                FROM managed.rv_be_bid_ancestry_laas rv_anc_laas
                WHERE rv_anc_laas.be_bid IN {0}
                AND rv_anc_laas.receiver_p IS NOT NULL
                AND rv_anc_laas.donor_p IS NOT NULL""".format(sample_list))

        df_laas = pd.concat([df_laas, out_df], ignore_index=True)

        # print("Executing query")
        # print('i: ', i, ' of: ', sample_n_iter)

    return df_laas


def tops_corn_2023(tops_2023_input_data):
    # Read recipe inputs
    # S3_CORN_NOAM_SUMR_2023 = dataiku.Dataset("S3_CORN_NOAM_SUMR_2023")
    # S3_CORN_NOAM_SUMR_2023_df = S3_CORN_NOAM_SUMR_2023.get_dataframe()

    # rv_corn_material_tester = dataiku.Dataset("rv_corn_material_tester_adapt")
    # rv_be_bid_ancestry_laas = dataiku.Dataset("rv_be_bid_ancestry_laas")

    print('tops_2023_input_data: ', tops_2023_input_data)
    fpath = os.path.join(tops_2023_input_data, 'full.parquet')

    S3_CORN_NOAM_SUMR_2023_df = pd.read_parquet(fpath)
    print('S3_CORN_NOAM_SUMR_2023_df.shape: ', S3_CORN_NOAM_SUMR_2023_df.shape)

    query = """
           SELECT
               loc_code,
               AVG(longitude_bak) AS x_longitude_bak,
               AVG(latitude_bak) AS y_latitude_bak
             FROM (
               SELECT DISTINCT
                   loc_code,
                   CASE WHEN country_code IN ('US', 'CA') AND longitude > 0
                       THEN - longitude
                   ELSE longitude
                   END AS longitude_bak,
                   latitude AS latitude_bak
                 FROM S3_CORN_NOAM_SUMR_2023_df
               WHERE longitude IS NOT NULL
               AND latitude IS NOT NULL
               AND ap_data_sector = 'CORN_NOAM_SUMR'
           ) coord_bak_raw
           GROUP BY loc_code
    """

    coord_bak_df = sqldf(query, {**locals(), **globals()})
    print('coord_bak_df.shape: ', coord_bak_df.shape)
    coord_bak_df.head(3)

    with DenodoConnection() as dc:
        rv_fe_query = dc.get_data("""
                SELECT
                    source_id,
                    value
                FROM managed.rv_feature_export
                WHERE feature_name = 'MATURITY_ZONE'
                AND feature_level = 'LOCATION'
                -- AND feature_provider = 'SPIRIT'
                AND market_name = 'CORN_NA_SUMMER'
                AND CAST(year AS integer) = 2023
                """)

    rv_fe = rv_fe_query
    print('rv_fe.shape: ', rv_fe.shape)

    with DenodoConnection() as dc:
        rv_fe_pcrpc_query = dc.get_data("""
                SELECT
                    source_id,
                    value
                FROM managed.rv_feature_export
                WHERE feature_name = 'PCRPC'
                  AND feature_level = 'LOCATION'
                  AND feature_provider = 'SPIRIT'
                  AND market_name = 'CORN_NA_SUMMER'
                  AND CAST(year AS integer) = 2023
                  """)

    rv_fe_pcrpc = rv_fe_pcrpc_query
    print('rv_fe_pcrpc.shape: ', rv_fe_pcrpc.shape)

    results = S3_CORN_NOAM_SUMR_2023_df.merge(coord_bak_df, on='loc_code', how='left').merge(rv_fe,
                                                                                             left_on='loc_selector',
                                                                                             right_on='source_id',
                                                                                             how='left').merge(
        rv_fe_pcrpc, left_on='loc_selector', right_on='source_id', how='left')
    print('results.shape: ', results.shape)

    results_query = """ SELECT *
            FROM results
           WHERE NOT pr_exclude
                  AND NOT psp_exclude
                  AND NOT tr_exclude
                  AND LOWER(material_id) NOT IN ('filler', 'purple')
                  AND crop_type_code LIKE 'YG%'
                  -- AND bg_code IN ('AB','BD','BF','BG','BL','CC','DE','DL','DW','EG','GF','GL','GU','HE','HT','IB','JR',
                   --       'KF','LC','LD','LR','LT','LX','ME','MY','NC','NE','NW','RL','SE','SY','TP','WE','XB','ZY','GY','MB','DB')
                  AND ((trait_measure_code = 'YGSMN' AND result_numeric_value > 0 AND result_numeric_value < 300)
                      OR (trait_measure_code ='GMSTP' AND result_numeric_value >= 4.5 AND result_numeric_value < 35)
                      OR (trait_measure_code ='HAVPN'))
                  AND year = 2023
                  AND ap_data_sector = 'CORN_NOAM_SUMR'
    """

    # results_filtered = sqldf(results_query, {**locals(), **globals()})
    results_filtered = duckdb.query(results_query).to_df()
    print('results_filtered.shape: ', results_filtered.shape)
    results_filtered.head(3)

    # Write recipe outputs
    # S3_CORN_NOAM_SUMR_2023_py = dataiku.Dataset("S3_CORN_NOAM_SUMR_2023_py")
    # S3_CORN_NOAM_SUMR_2023_py.write_with_schema(results_filtered)

    # Read in rv_corn_material_tester, left join on be_bid with be_bid

    cmt_df = get_cmt_bebid(results_filtered, 2000)
    print('cmt_df shape: ', cmt_df.shape)
    laas_df = get_laas_bebid(results_filtered, 2000)
    print('laas_df shape: ', laas_df.shape)

    join_query = """
        SELECT * 
        FROM
         results_filtered rf
        LEFT JOIN cmt_df
              ON rf.be_bid = cmt_df.be_bid
        LEFT JOIN laas_df
              ON rf.be_bid = laas_df.be_bid
              """

    results_joined = results_filtered.merge(cmt_df, on='be_bid', how='left').merge(laas_df, on='be_bid', how='left')

    # results_joined = sqldf(join_query, {**locals(), **globals()})
    # results_joined = duckdb.query(join_query).to_df()
    print('results_joined.shape: ', results_joined.shape)

    print(results_joined.columns)
    final_query = """
       SELECT
                CAST(year AS integer) AS year,
                ap_data_sector,
                COALESCE(value_x,
                         CAST(maturity_group AS varchar),
                         substr(loc_selector,0,1)) AS maturity_group,
                experiment_id,
                trial_id,
                trial_status,
                trial_stage,
                loc_selector,
                plot_barcode,
                plant_date,
                harvest_date,
                CASE WHEN country_code IN ('US', 'CA') AND longitude > 0
                    THEN COALESCE(-longitude, x_longitude_bak)
                ELSE COALESCE(longitude, x_longitude_bak)
                END AS x_longitude,
                COALESCE(latitude, y_latitude_bak) AS y_latitude,
                state_province_code,
                COALESCE(irrigation, 'NONE') AS irrigation,
                CASE
                    WHEN LOWER(value_y) = 'corn' THEN 2
                    WHEN value_y IS NOT NULL THEN 0
                    ELSE 1
                END AS previous_crop_corn,
                material_id,
                be_bid,
                COALESCE(be_bid, be_bid) AS match_be_bid,
                CASE
                    WHEN fp_het_pool != 'pool2' THEN COALESCE(fp_be_bid, receiver_p)
                    WHEN mp_het_pool = 'pool1' THEN COALESCE(mp_be_bid, donor_p)
                    ELSE receiver_p
                END AS par_hp1_be_bid,
                CASE
                    WHEN fp_het_pool != 'pool2' THEN fp_sample
                    WHEN mp_het_pool = 'pool1' THEN mp_sample
                    ELSE NULL
                END AS par_hp1_sample,
                CASE
                    WHEN fp_het_pool = 'pool2' THEN COALESCE(fp_be_bid, receiver_p)
                    WHEN mp_het_pool != 'pool1' THEN COALESCE(mp_be_bid, donor_p)
                    ELSE donor_p
                END AS par_hp2_be_bid,
                CASE
                    WHEN fp_het_pool = 'pool2' THEN fp_sample
                    WHEN mp_het_pool != 'pool1' THEN mp_sample
                    ELSE NULL
                END AS par_hp2_sample,
                fp_het_group,
                mp_het_group,
                CASE WHEN cpifl THEN 1 ELSE 0 END AS cpifl,
                COALESCE(CAST(cperf AS integer), CASE WHEN cpifl THEN 1 ELSE 0 END) AS cperf,
                trait_measure_code AS trait,
                result_numeric_value AS result
         FROM results_joined
        """

    # results_final = sqldf(final_query, {**locals(), **globals()})
    results_final = duckdb.query(final_query).to_df()
    print('results_final.shape: ', results_final.shape)
    results_final.head(3)

    return results_final


def get_highname_soy():
    with DenodoConnection() as dc:
        out_df = dc.get_data("""
        select be_bid, highname
        from managed.rv_trial_pheno_analytic_dataset
        where highname IN ('BC2180031','BC2180033','BC2180022','BW2050720','BW2150031',
            'BW2150018','BW2150032','SSW812394I2X', 'BW2050720','BN2055086','BW2050582','BW2150031','BW2150018',
            'BW2150032','SSW812393I2X','BN2189071','BS218A124','BS2083103','BS2080690','SSS612397I2X',
            'BS2084067','BS218B113','SSS642396I2X','BS2084137','BS2184692','BC2284143','BC2284112',
            'BC2284076','BC2284080','BC2180022','BC2180031','BC2180013','BC2180027','BC2180033','BC2284125')
        """)
    return out_df
