def compute_filled_mtc_final_bid2(spark):
    output_df = spark.sql("""
    SELECT
        `filled_mtc_final_bid`.`be_bid` AS `be_bid`,
        `lbg_bid`,
        `abbr_code`,
        CAST(`family` AS VARCHAR(2048)) AS `family`,
        `gencd`,
        `create_year`,
        `create_year_max`,
        `pedigree`,
        `highname`,
        `gmo`,
        `region_lid`,
        `gna_pedigree`,
        `primary_genetic_family_lid`,
        `secondary_genetic_family_lid`,
        `cpi`,
        `fada_group`,
        `cgenes`,
        `fp_abbr_code`,
        `fp_gencd`,
        substring(`fp_precd`, 1, 254) AS `fp_precd`,
        `fp_highname`,
        `fp_gna_pedigree`,
        `fp_linecode`,
        `fp_create_year`,
        `fp_create_year_max`,
        `fp_line_create_year`,
        `fp_chassis_year`,
        `fp_region_lid`,
        `fp_cpi`,
        `fp_be_bid`,
        `fp_lbg_bid`,
        `fp_fp_be_bid`,
        `fp_mp_be_bid`,
        `fp_het_group`,
        `fp_fada_group`,
        `fp_sample`,
        `mp_abbr_code`,
        `mp_gencd`,
        substring(`mp_precd`, 1, 254) AS `mp_precd`,
        `mp_highname`,
        `mp_gna_pedigree`,
        `mp_linecode`,
        `mp_create_year`,
        `mp_create_year_max`,
        `mp_line_create_year`,
        `mp_chassis_year`,
        `mp_region_lid`,
        `mp_cpi`,
        `mp_be_bid`,
        `mp_lbg_bid`,
        `mp_fp_be_bid`,
        `mp_mp_be_bid`,
        `mp_het_group`,
        `mp_fada_group`,
        `mp_sample`,
        `female`,
        `male`,
        `insect_cgenes`,
        `calc_tester`,
        `fp_tester_ratio`,
        `fp_not_tester_ratio`,
        `fp_make_tester`,
        `mp_tester_ratio`,
        `mp_not_tester_ratio`,
        `mp_make_tester`,
        `rule`,
        `first_pass_tester_role`,
        `rule2`,
        `tester_role`,
        CASE 
            WHEN NOT(`fp_het_pool` = '' OR `fp_het_pool` IS NULL) THEN `fp_het_pool`
            WHEN `fp_stats`.`pool1_vote` > (`fp_stats`.`pool2_vote`*3) AND `mp_stats`.`pool1_vote` <= (`mp_stats`.`pool2_vote`*3) THEN 'pool1'
            WHEN `fp_stats`.`pool2_vote` > (`fp_stats`.`pool1_vote`*3) AND `mp_stats`.`pool2_vote` <= (`mp_stats`.`pool1_vote`*3) THEN 'pool2'
            WHEN `fp_stats`.`pool1_vote` <= (`fp_stats`.`pool2_vote`*3) AND `mp_stats`.`pool1_vote` > (`mp_stats`.`pool2_vote`*3) THEN 'pool2'
            WHEN `fp_stats`.`pool2_vote` <= (`fp_stats`.`pool1_vote`*3) AND `mp_stats`.`pool2_vote` > (`mp_stats`.`pool1_vote`*3) THEN 'pool1'
            ELSE NULL
        END AS `fp_het_pool`,
        CASE 
            WHEN NOT(`mp_het_pool` = '' OR `mp_het_pool` IS NULL) THEN `mp_het_pool`
            WHEN `fp_stats`.`pool1_vote` > (`fp_stats`.`pool2_vote`*3) AND `mp_stats`.`pool1_vote` <= (`mp_stats`.`pool2_vote`*3) THEN 'pool2'
            WHEN `fp_stats`.`pool2_vote` > (`fp_stats`.`pool1_vote`*3) AND `mp_stats`.`pool2_vote` <= (`mp_stats`.`pool1_vote`*3) THEN 'pool1'
            WHEN `fp_stats`.`pool1_vote` <= (`fp_stats`.`pool2_vote`*3) AND `mp_stats`.`pool1_vote` > (`mp_stats`.`pool2_vote`*3) THEN 'pool1'
            WHEN `fp_stats`.`pool2_vote` <= (`fp_stats`.`pool1_vote`*3) AND `mp_stats`.`pool2_vote` > (`mp_stats`.`pool1_vote`*3) THEN 'pool2'
            ELSE NULL
        END AS `mp_het_pool`,
        CASE 
            WHEN `fp_het_pool` = 'pool1' THEN `fp_be_bid`
            WHEN `mp_het_pool` = 'pool1' THEN `mp_be_bid`
            WHEN `fp_stats`.`pool1_vote` > (`fp_stats`.`pool2_vote`*3) AND `mp_stats`.`pool1_vote` <= (`mp_stats`.`pool2_vote`*3) THEN `fp_be_bid`
            WHEN `fp_stats`.`pool2_vote` > (`fp_stats`.`pool1_vote`*3) AND `mp_stats`.`pool2_vote` <= (`mp_stats`.`pool1_vote`*3) THEN `mp_be_bid`
            WHEN `fp_stats`.`pool1_vote` <= (`fp_stats`.`pool2_vote`*3) AND `mp_stats`.`pool1_vote` > (`mp_stats`.`pool2_vote`*3) THEN `mp_be_bid`
            WHEN `fp_stats`.`pool2_vote` <= (`fp_stats`.`pool1_vote`*3) AND `mp_stats`.`pool2_vote` > (`mp_stats`.`pool1_vote`*3) THEN `fp_be_bid`
            ELSE NULL
        END AS `pool1_be_bid`,
        CASE 
            WHEN `mp_het_pool` = 'pool2' THEN `mp_be_bid`
            WHEN `fp_het_pool` = 'pool2' THEN `fp_be_bid`
            WHEN `fp_stats`.`pool1_vote` > (`fp_stats`.`pool2_vote`*3) AND `mp_stats`.`pool1_vote` <= (`mp_stats`.`pool2_vote`*3) THEN `mp_be_bid`
            WHEN `fp_stats`.`pool2_vote` > (`fp_stats`.`pool1_vote`*3) AND `mp_stats`.`pool2_vote` <= (`mp_stats`.`pool1_vote`*3) THEN `fp_be_bid`
            WHEN `fp_stats`.`pool1_vote` <= (`fp_stats`.`pool2_vote`*3) AND `mp_stats`.`pool1_vote` > (`mp_stats`.`pool2_vote`*3) THEN `fp_be_bid`
            WHEN `fp_stats`.`pool2_vote` <= (`fp_stats`.`pool1_vote`*3) AND `mp_stats`.`pool2_vote` > (`mp_stats`.`pool1_vote`*3) THEN `mp_be_bid`
            ELSE NULL
        END AS `pool2_be_bid`
      FROM `filled_mtc_final_bid`
    LEFT JOIN `hetpool_review` `fp_stats`
      ON `filled_mtc_final_bid`.`fp_be_bid` = `fp_stats`.`be_bid`
    LEFT JOIN `hetpool_review` `mp_stats`
      ON `filled_mtc_final_bid`.`mp_be_bid` = `mp_stats`.`be_bid`
    WHERE `filled_mtc_final_bid`.`fp_be_bid` IS NOT NULL 
      OR `filled_mtc_final_bid`.`mp_be_bid` IS NOT NULL
    """)

    return output_df


def compute_hetpool_review(spark):
    output_df = spark.sql("""
   SELECT
        `be_bid`,
        SUM(`pool1_vote`) AS `pool1_vote`,
        SUM(`pool2_vote`) AS `pool2_vote`,
        SUM(`poolnull_vote`) AS `poolnull_vote`,
        SUM(`ss_vote`) AS `ss_vote`,
        SUM(`nss_vote`) AS `nss_vote`,
        SUM(`hetgrpnull_vote`) AS `hetgrpnull_vote`
      FROM(
        SELECT
            `fp_be_bid` AS `be_bid`,
            SUM(CASE WHEN `fp_het_pool` = 'pool1' THEN 1 ELSE 0 END) AS `pool1_vote`,
            SUM(CASE WHEN `fp_het_pool` = 'pool2' THEN 1 ELSE 0 END) AS `pool2_vote`,
            SUM(CASE WHEN `fp_het_pool` = '' OR `fp_het_pool` IS NULL THEN 1 ELSE 0 END) AS `poolnull_vote`,
            SUM(CASE WHEN `fp_het_group` = 'ss' THEN 1 ELSE 0 END) AS `ss_vote`,
            SUM(CASE WHEN `fp_het_group` = 'nss' THEN 1 else 0 END) AS `nss_vote`,
            SUM(CASE WHEN `fp_het_group` = '' OR `fp_het_group` IS NULL THEN 1 ELSE 0 END) AS `hetgrpnull_vote`
          FROM `filled_mtc_final_bid`
          GROUP BY `fp_be_bid`

        UNION ALL
        SELECT
            `mp_be_bid` AS `be_bid`,
            SUM(CASE WHEN `mp_het_pool` = 'pool1' THEN 1 ELSE 0 END) AS `pool1_vote`,
            SUM(CASE WHEN `mp_het_pool` = 'pool2' THEN 1 ELSE 0 END) AS `pool2_vote`,
            SUM(CASE WHEN `mp_het_pool` = '' OR `mp_het_pool` IS NULL THEN 1 ELSE 0 END) AS `poolnull_vote`,
            SUM(CASE WHEN `mp_het_group` = 'ss' THEN 1 ELSE 0 END) AS `ss_vote`,
            SUM(CASE WHEN `mp_het_group` = 'nss' THEN 1 else 0 END) AS `nss_vote`,
            SUM(CASE WHEN `mp_het_group` = '' OR `mp_het_group` IS NULL THEN 1 ELSE 0 END) AS `hetgrpnull_vote`
          FROM `filled_mtc_final_bid`
          GROUP BY `mp_be_bid`
    ) `be_bid_stats`
    GROUP BY `be_bid` 
    """)

    return output_df


def compute_filled_mtc_final_bid(spark):
    output_df = spark.sql("""
    SELECT
        `be_bid`,
        `lbg_bid`,
        `abbr_code`,
        IF(ISNULL(`family`), IF(`tester_role` = 'M', `fp_family`, IF(`tester_role` = 'F', `mp_family`, '' )), `family`) AS `family`,
        `gencd`,
        `create_year`,
        `create_year_max`,
        `pedigree`,
        `highname`,
        `gmo`,
        `region_lid`,
        `gna_pedigree`,
        IF(ISNULL(`primary_genetic_family_lid`), 
           IF(`tester_role` = 'M', `fp_primary_genetic_family_lid`, 
              IF(`tester_role` = 'F', `mp_primary_genetic_family_lid`, '' )),
           `primary_genetic_family_lid`
        ) AS `primary_genetic_family_lid`,
        IF(ISNULL(`secondary_genetic_family_lid`), 
           IF(`tester_role` = 'M', `fp_secondary_genetic_family_lid`, 
              IF(`tester_role` = 'F', `mp_secondary_genetic_family_lid`, '' )),
           `secondary_genetic_family_lid`
        ) AS `secondary_genetic_family_lid`,
        `cpi`,
        `fada_group`,
        `cgenes`,
        `fp_abbr_code`,
        `fp_gencd`,
        `fp_precd`,
        `fp_highname`,
        `fp_gna_pedigree`,
        `fp_linecode`,
        `fp_create_year`,
        `fp_create_year_max`,
        `fp_line_create_year`,
        `fp_chassis_year`,
        `fp_region_lid`,
        `fp_cpi`,
        `fp_be_bid`,
        `fp_lbg_bid`,
        `fp_fp_be_bid`,
        `fp_mp_be_bid`,
        `fp_het_group`,
        `fp_fada_group`,
        `fp_sample`,
        `mp_abbr_code`,
        `mp_gencd`,
        `mp_precd`,
        `mp_highname`,
        `mp_gna_pedigree`,
        `mp_linecode`,
        `mp_create_year`,
        `mp_create_year_max`,
        `mp_line_create_year`,
        `mp_chassis_year`,
        `mp_region_lid`,
        `mp_cpi`,
        `mp_be_bid`,
        `mp_lbg_bid`,
        `mp_fp_be_bid`,
        `mp_mp_be_bid`,
        `mp_het_group`,
        `mp_fada_group`,
        `mp_sample`,
        `female`,
        `male`,
        `insect_cgenes`,
        IF(`tester_role` = 'F', `fp_linecode`, IF(`tester_role` = 'M', `mp_linecode`, '' )) AS `calc_tester`,
        `fp_tester_ratio`,
        `fp_not_tester_ratio`,
        `fp_make_tester`,
        `mp_tester_ratio`,
        `mp_not_tester_ratio`,
        `mp_make_tester`,
        `rule`,
        `first_pass_tester_role`,
        `rule2`,
        `tester_role`,
        CASE
            WHEN `fp_base_pool` = 'pool1' AND (`mp_base_pool` = 'pool2' OR `mp_base_pool` IS NULL OR `mp_base_pool` = '') THEN 'pool1'
            WHEN `fp_base_pool` = 'pool2' AND (`mp_base_pool` = 'pool1' OR `mp_base_pool` IS NULL OR `mp_base_pool` = '') THEN 'pool2'
            WHEN `mp_base_pool` = 'pool1' AND (`fp_base_pool` = 'pool2' OR `fp_base_pool` IS NULL OR `fp_base_pool` = '') THEN 'pool2'
            WHEN `mp_base_pool` = 'pool2' AND (`fp_base_pool` = 'pool1' OR `fp_base_pool` IS NULL OR `fp_base_pool` = '') THEN 'pool1'
            WHEN `fp_base_pool` = `mp_base_pool` AND `fp_base_pool` = 'pool1' AND `tester_role` = 'M' THEN 'pool1'
            WHEN `fp_base_pool` = `mp_base_pool` AND `fp_base_pool` = 'pool1' AND `tester_role` = 'F' THEN 'pool2'
            WHEN `fp_base_pool` = `mp_base_pool` AND `fp_base_pool` = 'pool2' AND `tester_role` = 'M' THEN 'pool2'
            WHEN `fp_base_pool` = `mp_base_pool` AND `fp_base_pool` = 'pool2' AND `tester_role` = 'F' THEN 'pool1'
            WHEN `fp_base_pool` = `mp_base_pool` AND `fp_base_pool` = 'pool1' AND `fp_base_pool_rank` < `mp_base_pool_rank` THEN 'pool1'
            WHEN `fp_base_pool` = `mp_base_pool` AND `fp_base_pool` = 'pool2' AND `fp_base_pool_rank` < `mp_base_pool_rank` THEN 'pool2'
            WHEN `fp_base_pool` = `mp_base_pool` AND `fp_base_pool` = 'pool1' AND `fp_base_pool_rank` > `mp_base_pool_rank` THEN 'pool2'
            WHEN `fp_base_pool` = `mp_base_pool` AND `fp_base_pool` = 'pool2' AND `fp_base_pool_rank` > `mp_base_pool_rank` THEN 'pool1'
            ELSE NULL
        END AS `fp_het_pool`,
        CASE
            WHEN `fp_base_pool` = 'pool1' AND (`mp_base_pool` = 'pool2' OR `mp_base_pool` IS NULL OR `mp_base_pool` = '') THEN 'pool2'
            WHEN `fp_base_pool` = 'pool2' AND (`mp_base_pool` = 'pool1' OR `mp_base_pool` IS NULL OR `mp_base_pool` = '') THEN 'pool1'
            WHEN `mp_base_pool` = 'pool1' AND (`fp_base_pool` = 'pool2' OR `fp_base_pool` IS NULL OR `fp_base_pool` = '') THEN 'pool1'
            WHEN `mp_base_pool` = 'pool2' AND (`fp_base_pool` = 'pool1' OR `fp_base_pool` IS NULL OR `fp_base_pool` = '') THEN 'pool2'
            WHEN `fp_base_pool` = `mp_base_pool` AND `fp_base_pool` = 'pool1' AND `tester_role` = 'M' THEN 'pool2'
            WHEN `fp_base_pool` = `mp_base_pool` AND `fp_base_pool` = 'pool1' AND `tester_role` = 'F' THEN 'pool1'
            WHEN `fp_base_pool` = `mp_base_pool` AND `fp_base_pool` = 'pool2' AND `tester_role` = 'M' THEN 'pool1'
            WHEN `fp_base_pool` = `mp_base_pool` AND `fp_base_pool` = 'pool2' AND `tester_role` = 'F' THEN 'pool2'
            WHEN `fp_base_pool` = `mp_base_pool` AND `fp_base_pool` = 'pool1' AND `fp_base_pool_rank` < `mp_base_pool_rank` THEN 'pool2'
            WHEN `fp_base_pool` = `mp_base_pool` AND `fp_base_pool` = 'pool2' AND `fp_base_pool_rank` < `mp_base_pool_rank` THEN 'pool1'
            WHEN `fp_base_pool` = `mp_base_pool` AND `fp_base_pool` = 'pool1' AND `fp_base_pool_rank` > `mp_base_pool_rank` THEN 'pool1'
            WHEN `fp_base_pool` = `mp_base_pool` AND `fp_base_pool` = 'pool2' AND `fp_base_pool_rank` > `mp_base_pool_rank` THEN 'pool2'
            ELSE NULL
        END AS `mp_het_pool`
    FROM(
        SELECT 
            `filled_material_tester_calculated_rough_bebid`.`abbr_code` AS `abbr_code`,
            `filled_material_tester_calculated_rough_bebid`.`family` AS `family`,
            `filled_material_tester_calculated_rough_bebid`.`gencd` AS `gencd`,
            `filled_material_tester_calculated_rough_bebid`.`create_year` AS `create_year`,
            `filled_material_tester_calculated_rough_bebid`.`create_year_max` AS `create_year_max`,
            `filled_material_tester_calculated_rough_bebid`.`pedigree` AS `pedigree`,
            `filled_material_tester_calculated_rough_bebid`.`highname` AS `highname`,
            `filled_material_tester_calculated_rough_bebid`.`gmo` AS `gmo`,
            `filled_material_tester_calculated_rough_bebid`.`region_lid` AS `region_lid`,
            `filled_material_tester_calculated_rough_bebid`.`gna_pedigree` AS `gna_pedigree`,
            `filled_material_tester_calculated_rough_bebid`.`primary_genetic_family_lid` AS `primary_genetic_family_lid`,
            `filled_material_tester_calculated_rough_bebid`.`secondary_genetic_family_lid` AS `secondary_genetic_family_lid`,
            `filled_material_tester_calculated_rough_bebid`.`cpi` AS `cpi`,
            `filled_material_tester_calculated_rough_bebid`.`be_bid` AS `be_bid`,
            `filled_material_tester_calculated_rough_bebid`.`lbg_bid` AS `lbg_bid`,
            `filled_material_tester_calculated_rough_bebid`.`fada_group` AS `fada_group`,
            `filled_material_tester_calculated_rough_bebid`.`cgenes` AS `cgenes`,
            `filled_material_tester_calculated_rough_bebid`.`fp_abbr_code` AS `fp_abbr_code`,
            `filled_material_tester_calculated_rough_bebid`.`fp_family` AS `fp_family`,
            `filled_material_tester_calculated_rough_bebid`.`fp_gencd` AS `fp_gencd`,
            `filled_material_tester_calculated_rough_bebid`.`fp_create_year` AS `fp_create_year`,
            `filled_material_tester_calculated_rough_bebid`.`fp_create_year_max` AS `fp_create_year_max`,
            `filled_material_tester_calculated_rough_bebid`.`fp_region_lid` AS `fp_region_lid`,
            `filled_material_tester_calculated_rough_bebid`.`fp_precd` AS `fp_precd`,
            `filled_material_tester_calculated_rough_bebid`.`fp_highname` AS `fp_highname`,
            `filled_material_tester_calculated_rough_bebid`.`fp_gna_pedigree` AS `fp_gna_pedigree`,
            `filled_material_tester_calculated_rough_bebid`.`fp_linecode` AS `fp_linecode`,
            `filled_material_tester_calculated_rough_bebid`.`fp_line_create_year` AS `fp_line_create_year`,
            `filled_material_tester_calculated_rough_bebid`.`fp_chassis_year` AS `fp_chassis_year`,
            `filled_material_tester_calculated_rough_bebid`.`fp_primary_genetic_family_lid` AS `fp_primary_genetic_family_lid`,
            `filled_material_tester_calculated_rough_bebid`.`fp_secondary_genetic_family_lid` AS `fp_secondary_genetic_family_lid`,
            `filled_material_tester_calculated_rough_bebid`.`fp_cpi` AS `fp_cpi`,
            `filled_material_tester_calculated_rough_bebid`.`fp_be_bid` AS `fp_be_bid`,
            `filled_material_tester_calculated_rough_bebid`.`fp_lbg_bid` AS `fp_lbg_bid`,
            `filled_material_tester_calculated_rough_bebid`.`fp_fp_be_bid` AS `fp_fp_be_bid`,
            `filled_material_tester_calculated_rough_bebid`.`fp_mp_be_bid` AS `fp_mp_be_bid`,
            `filled_material_tester_calculated_rough_bebid`.`fp_het_group` AS `fp_het_group`,
            `filled_material_tester_calculated_rough_bebid`.`fp_fada_group` AS `fp_fada_group`,
            `filled_material_tester_calculated_rough_bebid`.`fp_base_pool` AS `fp_base_pool`,
            `filled_material_tester_calculated_rough_bebid`.`fp_base_pool_rank` AS `fp_base_pool_rank`,
            `filled_material_tester_calculated_rough_bebid`.`fp_sample` AS `fp_sample`,
            `filled_material_tester_calculated_rough_bebid`.`mp_abbr_code` AS `mp_abbr_code`,
            `filled_material_tester_calculated_rough_bebid`.`mp_family` AS `mp_family`,
            `filled_material_tester_calculated_rough_bebid`.`mp_gencd` AS `mp_gencd`,
            `filled_material_tester_calculated_rough_bebid`.`mp_create_year` AS `mp_create_year`,
            `filled_material_tester_calculated_rough_bebid`.`mp_create_year_max` AS `mp_create_year_max`,
            `filled_material_tester_calculated_rough_bebid`.`mp_region_lid` AS `mp_region_lid`,
            `filled_material_tester_calculated_rough_bebid`.`mp_precd` AS `mp_precd`,
            `filled_material_tester_calculated_rough_bebid`.`mp_highname` AS `mp_highname`,
            `filled_material_tester_calculated_rough_bebid`.`mp_gna_pedigree` AS `mp_gna_pedigree`,
            `filled_material_tester_calculated_rough_bebid`.`mp_linecode` AS `mp_linecode`,
            `filled_material_tester_calculated_rough_bebid`.`mp_line_create_year` AS `mp_line_create_year`,
            `filled_material_tester_calculated_rough_bebid`.`mp_chassis_year` AS `mp_chassis_year`,
            `filled_material_tester_calculated_rough_bebid`.`mp_primary_genetic_family_lid` AS `mp_primary_genetic_family_lid`,
            `filled_material_tester_calculated_rough_bebid`.`mp_secondary_genetic_family_lid` AS `mp_secondary_genetic_family_lid`,
            `filled_material_tester_calculated_rough_bebid`.`mp_cpi` AS `mp_cpi`,
            `filled_material_tester_calculated_rough_bebid`.`mp_be_bid` AS `mp_be_bid`,
            `filled_material_tester_calculated_rough_bebid`.`mp_lbg_bid` AS `mp_lbg_bid`,
            `filled_material_tester_calculated_rough_bebid`.`mp_fp_be_bid` AS `mp_fp_be_bid`,
            `filled_material_tester_calculated_rough_bebid`.`mp_mp_be_bid` AS `mp_mp_be_bid`,
            `filled_material_tester_calculated_rough_bebid`.`mp_het_group` AS `mp_het_group`,
            `filled_material_tester_calculated_rough_bebid`.`mp_fada_group` AS `mp_fada_group`,
            `filled_material_tester_calculated_rough_bebid`.`mp_base_pool` AS `mp_base_pool`,
            `filled_material_tester_calculated_rough_bebid`.`mp_base_pool_rank` AS `mp_base_pool_rank`,
            `filled_material_tester_calculated_rough_bebid`.`mp_sample` AS `mp_sample`,
            `filled_material_tester_calculated_rough_bebid`.`female` AS `female`,
            `filled_material_tester_calculated_rough_bebid`.`male` AS `male`,
            `filled_material_tester_calculated_rough_bebid`.`insect_cgenes` AS `insect_cgenes`,
            CASE 
                WHEN `bebid_tester_stats`.`make_tester` = 1 
                    AND !ISNULL(`filled_material_tester_calculated_rough_bebid`.`fp_linecode`) 
                    AND (`bebid_tester_stats_2`.`make_tester` = 0)
                    THEN 'F'
                WHEN `bebid_tester_stats_2`.`make_tester` = 1 
                    AND !ISNULL(`filled_material_tester_calculated_rough_bebid`.`mp_linecode`) 
                    AND (`bebid_tester_stats`.`make_tester` = 0)
                    THEN 'M'
                WHEN `bebid_tester_stats`.`make_tester` = 1 
                    AND !ISNULL(`filled_material_tester_calculated_rough_bebid`.`fp_linecode`) 
                    AND (`bebid_tester_stats_2`.`make_tester` != 1 OR ISNULL(`bebid_tester_stats_2`.`make_tester`) OR `filled_material_tester_calculated_rough_bebid`.`rule` = 12 OR `filled_material_tester_calculated_rough_bebid`.`rule`=13)
                    AND (`filled_material_tester_calculated_rough_bebid`.`tester_role` IS NULL OR `filled_material_tester_calculated_rough_bebid`.`tester_role` = '')
                    THEN 'F'
                WHEN `bebid_tester_stats_2`.`make_tester` = 1 
                    AND !ISNULL(`filled_material_tester_calculated_rough_bebid`.`mp_linecode`) 
                    AND (`bebid_tester_stats`.`make_tester` != 1 OR ISNULL(`bebid_tester_stats`.`make_tester`) OR `filled_material_tester_calculated_rough_bebid`.`rule` = 12 OR `filled_material_tester_calculated_rough_bebid`.`rule`=13)
                    AND (`filled_material_tester_calculated_rough_bebid`.`tester_role` IS NULL OR `filled_material_tester_calculated_rough_bebid`.`tester_role` = '')
                    THEN 'M'
                WHEN `bebid_tester_stats_2`.`make_tester` = 0 AND (`bebid_tester_stats`.`not_tester_ratio` < `bebid_tester_stats`.`tester_ratio` - 0.1) AND ISNULL(`bebid_tester_stats`.`make_tester`)
                    THEN 'F'
                WHEN `bebid_tester_stats`.`make_tester` = 0 AND (`bebid_tester_stats_2`.`not_tester_ratio` < `bebid_tester_stats_2`.`tester_ratio` - 0.1) AND ISNULL(`bebid_tester_stats_2`.`make_tester`)
                    THEN 'M'
                WHEN `filled_material_tester_calculated_rough_bebid`.`cpi` = 1 
                    AND (`filled_material_tester_calculated_rough_bebid`.`fp_line_create_year` < `filled_material_tester_calculated_rough_bebid`.`mp_line_create_year`)
                    AND (`filled_material_tester_calculated_rough_bebid`.`tester_role` IS NULL OR `filled_material_tester_calculated_rough_bebid`.`tester_role` = '')
                    THEN 'F'
                WHEN `filled_material_tester_calculated_rough_bebid`.`cpi` = 1 
                    AND (`filled_material_tester_calculated_rough_bebid`.`mp_line_create_year` < `filled_material_tester_calculated_rough_bebid`.`fp_line_create_year`)
                    AND (`filled_material_tester_calculated_rough_bebid`.`tester_role` IS NULL OR `filled_material_tester_calculated_rough_bebid`.`tester_role` = '')
                    THEN 'M'
                WHEN `bebid_tester_stats`.`tester_ratio`> `bebid_tester_stats_2`.`tester_ratio` 
                    AND (`filled_material_tester_calculated_rough_bebid`.`tester_role` IS NULL OR `filled_material_tester_calculated_rough_bebid`.`tester_role` = '')
                    THEN 'F'
                WHEN `bebid_tester_stats_2`.`tester_ratio`> `bebid_tester_stats`.`tester_ratio`
                    AND (`filled_material_tester_calculated_rough_bebid`.`tester_role` IS NULL OR `filled_material_tester_calculated_rough_bebid`.`tester_role` = '')
                    THEN 'M'
                ELSE `filled_material_tester_calculated_rough_bebid`.`tester_role`
            END AS `tester_role`,
            CASE 
                WHEN `bebid_tester_stats`.`make_tester` = 1 
                    AND !ISNULL(`filled_material_tester_calculated_rough_bebid`.`fp_linecode`) 
                    AND (`bebid_tester_stats_2`.`make_tester` = 0)
                    THEN 1
                WHEN `bebid_tester_stats_2`.`make_tester` = 1 
                    AND !ISNULL(`filled_material_tester_calculated_rough_bebid`.`mp_linecode`) 
                    AND (`bebid_tester_stats`.`make_tester` = 0)
                    THEN 2
                WHEN `bebid_tester_stats`.`make_tester` = 1 
                    AND !ISNULL(`filled_material_tester_calculated_rough_bebid`.`fp_linecode`) 
                    AND (`bebid_tester_stats_2`.`make_tester` != 1 OR ISNULL(`bebid_tester_stats_2`.`make_tester`))
                    AND (`filled_material_tester_calculated_rough_bebid`.`tester_role` IS NULL OR `filled_material_tester_calculated_rough_bebid`.`tester_role` = '')
                    THEN 3
                WHEN `bebid_tester_stats_2`.`make_tester` = 1 
                    AND !ISNULL(`filled_material_tester_calculated_rough_bebid`.`mp_linecode`) 
                    AND (`bebid_tester_stats`.`make_tester` != 1 OR ISNULL(`bebid_tester_stats`.`make_tester`))
                    AND (`filled_material_tester_calculated_rough_bebid`.`tester_role` IS NULL OR `filled_material_tester_calculated_rough_bebid`.`tester_role` = '')
                    THEN 4
                WHEN `bebid_tester_stats_2`.`make_tester` = 0 AND (`bebid_tester_stats`.`not_tester_ratio` < `bebid_tester_stats`.`tester_ratio` - 0.1) AND ISNULL(`bebid_tester_stats`.`make_tester`)
                    THEN 5
                WHEN `bebid_tester_stats`.`make_tester` = 0 AND (`bebid_tester_stats_2`.`not_tester_ratio` < `bebid_tester_stats_2`.`tester_ratio` - 0.1) AND ISNULL(`bebid_tester_stats_2`.`make_tester`)
                    THEN 6
                WHEN `filled_material_tester_calculated_rough_bebid`.`cpi` = 1 
                    AND (`filled_material_tester_calculated_rough_bebid`.`fp_line_create_year` < `filled_material_tester_calculated_rough_bebid`.`mp_line_create_year`)
                    AND (`filled_material_tester_calculated_rough_bebid`.`tester_role` IS NULL OR `filled_material_tester_calculated_rough_bebid`.`tester_role` = '')
                    THEN 7
                WHEN `filled_material_tester_calculated_rough_bebid`.`cpi` = 1 
                    AND (`filled_material_tester_calculated_rough_bebid`.`mp_line_create_year` < `filled_material_tester_calculated_rough_bebid`.`fp_line_create_year`)
                    AND (`filled_material_tester_calculated_rough_bebid`.`tester_role` IS NULL OR `filled_material_tester_calculated_rough_bebid`.`tester_role` = '')
                    THEN 8
                WHEN `bebid_tester_stats`.`tester_ratio`> `bebid_tester_stats_2`.`tester_ratio` 
                    AND (`filled_material_tester_calculated_rough_bebid`.`tester_role` IS NULL OR `filled_material_tester_calculated_rough_bebid`.`tester_role` = '')
                    THEN 9
                WHEN `bebid_tester_stats_2`.`tester_ratio`> `bebid_tester_stats`.`tester_ratio`
                    AND (`filled_material_tester_calculated_rough_bebid`.`tester_role` IS NULL OR `filled_material_tester_calculated_rough_bebid`.`tester_role` = '')
                    THEN 10
                ELSE 0
            END AS `rule2`,
            `filled_material_tester_calculated_rough_bebid`.`tester_role` AS `first_pass_tester_role`,
            `filled_material_tester_calculated_rough_bebid`.`rule` AS `rule`,
            `bebid_tester_stats`.`not_tester_ratio` AS `fp_not_tester_ratio`,
            `bebid_tester_stats`.`tester_ratio` AS `fp_tester_ratio`,
            `bebid_tester_stats`.`make_tester` AS `fp_make_tester`,
            `bebid_tester_stats_2`.`not_tester_ratio` AS `mp_not_tester_ratio`,
            `bebid_tester_stats_2`.`tester_ratio` AS `mp_tester_ratio`,
            `bebid_tester_stats_2`.`make_tester` AS `mp_make_tester`
          FROM `filled_material_tester_calculated_rough_by_be_bid_with_parents` `filled_material_tester_calculated_rough_bebid`
          LEFT JOIN (
              SELECT
                  `be_bid`,
                  COALESCE(`fp_tester_ratio`, `tester_ratio`) AS `tester_ratio`,
                  COALESCE(`fp_not_tester_ratio`, `not_tester_ratio`) AS `not_tester_ratio`,
                  IF(`fp_tester_ratio` IS NOT NULL, `fp_make_tester`, `make_tester`) AS `make_tester`
                FROM `bebid_tester_stats`
          ) `bebid_tester_stats`
            ON `filled_material_tester_calculated_rough_bebid`.`fp_be_bid` = `bebid_tester_stats`.`be_bid`
          LEFT JOIN (
              SELECT
                  `be_bid`,
                  COALESCE(`mp_tester_ratio`, `tester_ratio`) AS `tester_ratio`,
                  COALESCE(`mp_not_tester_ratio`, `not_tester_ratio`) AS `not_tester_ratio`,
                  IF(`mp_tester_ratio` IS NOT NULL, `mp_make_tester`, `make_tester`) AS `make_tester`
                FROM `bebid_tester_stats`
          ) `bebid_tester_stats_2`
            ON `filled_material_tester_calculated_rough_bebid`.`mp_be_bid` = `bebid_tester_stats_2`.`be_bid`
    ) `filled_mtc_pregroup_joined`
    """)

    return output_df


def compute_bebid_tester_stats(spark):
    output_df = spark.sql("""
    SELECT
        `be_bid`,
        `mp_count`,
        `fp_count`,
        `count`,
        `not_tester_ratio`,
        `tester_ratio`,
        if(`tester_ratio` >= `not_tester_ratio` +0.4 , 1, 
            if(`not_tester_ratio` >= `tester_ratio` + 0.3, 0, NULL)) AS `make_tester`,
        `fp_tester_ratio`,
        `mp_tester_ratio`,
        `fp_not_tester_ratio`,
        `mp_not_tester_ratio`,
        if(`fp_tester_ratio` >= `fp_not_tester_ratio` +0.4 , 1, 
            if(`fp_not_tester_ratio` >= `fp_tester_ratio` + 0.3, 0, NULL)) AS `fp_make_tester`,
        if(`mp_tester_ratio` >= `mp_not_tester_ratio` +0.4 , 1, 
            if(`mp_not_tester_ratio` >= `mp_tester_ratio` + 0.3, 0, NULL)) AS `mp_make_tester`
      FROM(
        SELECT 
            COALESCE(`fp_matid_tester_stats`.`fp_be_bid`,`mp_matid_tester_stats`.`mp_be_bid`) AS `be_bid`,
            `mp_matid_tester_stats`.`count` AS `mp_count`,
            `fp_matid_tester_stats`.`count` AS `fp_count`,
            `mp_matid_tester_stats`.`is_tester_sum`/`mp_matid_tester_stats`.`count` AS `mp_tester_ratio`,
            `fp_matid_tester_stats`.`is_tester_sum`/`fp_matid_tester_stats`.`count` AS `fp_tester_ratio`,
            `mp_matid_tester_stats`.`is_not_tester_sum`/`mp_matid_tester_stats`.`count` AS `mp_not_tester_ratio`,
            `fp_matid_tester_stats`.`is_not_tester_sum`/`fp_matid_tester_stats`.`count` AS `fp_not_tester_ratio`,
            ( IF(ISNULL(`mp_matid_tester_stats`.`is_tester_sum`),0,`mp_matid_tester_stats`.`is_tester_sum`) + 
                IF(ISNULL(`fp_matid_tester_stats`.`is_tester_sum`),0,`fp_matid_tester_stats`.`is_tester_sum`)) 
              /( IF(ISNULL(`mp_matid_tester_stats`.`count`),0,`mp_matid_tester_stats`.`count`) + 
                IF(ISNULL(`fp_matid_tester_stats`.`count`),0,`fp_matid_tester_stats`.`count`))
            AS `tester_ratio`,
            ( IF(ISNULL(`mp_matid_tester_stats`.`is_not_tester_sum`),0,`mp_matid_tester_stats`.`is_not_tester_sum`) + 
                IF(ISNULL(`fp_matid_tester_stats`.`is_not_tester_sum`),0,`fp_matid_tester_stats`.`is_not_tester_sum`)) 
              /( IF(ISNULL(`mp_matid_tester_stats`.`count`),0,`mp_matid_tester_stats`.`count`) + 
                IF(ISNULL(`fp_matid_tester_stats`.`count`),0,`fp_matid_tester_stats`.`count`))
            AS `not_tester_ratio`,
            IF(ISNULL(`mp_matid_tester_stats`.`count`),0,`mp_matid_tester_stats`.`count`) + IF(ISNULL(`fp_matid_tester_stats`.`count`),0,`fp_matid_tester_stats`.`count`) AS `count`
          FROM (
              SELECT 
                `mp_be_bid` AS `mp_be_bid`,
                SUM(`is_tester`) AS `is_tester_sum`,
                SUM(`is_not_tester`) AS `is_not_tester_sum`,
                COUNT(*) AS `count`
              FROM (
                SELECT 
                    `mp_be_bid` AS `mp_be_bid`,
                    `tester_role` AS `tester_role`,
                    CASE WHEN `tester_role` = 'M' THEN 1 ELSE 0 END AS `is_tester`,
                    CASE WHEN `tester_role` = 'F' THEN 1 ELSE 0 END AS `is_not_tester`
                  FROM `filled_material_tester_calculated_rough_by_be_bid_with_parents`
                  WHERE `mp_be_bid` != '' AND `mp_be_bid` IS NOT NULL AND `mp_linecode` != '' AND `mp_linecode` IS NOT NULL AND (`rule` > 6 OR `rule` = 0)
                ) `dku__beforegrouping`
              GROUP BY `mp_be_bid`
          ) `mp_matid_tester_stats`
          FULL OUTER JOIN (
              SELECT 
                `fp_be_bid` AS `fp_be_bid`,
                SUM(`is_tester`) AS `is_tester_sum`,
                SUM(`is_not_tester`) AS `is_not_tester_sum`,
                COUNT(*) AS `count`
              FROM (
                SELECT 
                    `fp_be_bid` AS `fp_be_bid`,
                    `tester_role` AS `tester_role`,
                    CASE WHEN `tester_role` = 'F' THEN 1 ELSE 0 END AS `is_tester`,
                    CASE WHEN `tester_role` = 'M' THEN 1 ELSE 0 END AS `is_not_tester`
                  FROM `filled_material_tester_calculated_rough_by_be_bid_with_parents`
                  WHERE `fp_be_bid` != '' AND `fp_be_bid` IS NOT NULL AND `fp_linecode` != '' AND `fp_linecode` IS NOT NULL AND (`rule` > 6 OR `rule` = 0)
                ) `dku__beforegrouping`
              GROUP BY `fp_be_bid`
          ) `fp_matid_tester_stats`
            ON `mp_matid_tester_stats`.`mp_be_bid` = `fp_matid_tester_stats`.`fp_be_bid`
      ) `matid_tester_stats_raw`
    """)
    return output_df


def compute_filled_material_tester_calculated_rough_by_be_bid_with_parents(spark):
    output_df = spark.sql("""
    SELECT 
        `abbr_code` AS `abbr_code`,
        `family` AS `family`,
        `gencd` AS `gencd`,
        `create_year` AS `create_year`,
        `create_year_max` AS `create_year_max`,
        `pedigree` AS `pedigree`,
        `gmo` AS `gmo`,
        `region_lid` AS `region_lid`,
        `precd` AS `precd`,
        `highname` AS `highname`,
        `gna_pedigree` AS `gna_pedigree`,
        `primary_genetic_family_lid` AS `primary_genetic_family_lid`,
        `secondary_genetic_family_lid` AS `secondary_genetic_family_lid`,
        `cpi` AS `cpi`,
        `be_bid` AS `be_bid`,
        `lbg_bid` AS `lbg_bid`,
        `fada_group`,
        `cgenes` AS `cgenes`,
        `fp_abbr_code` AS `fp_abbr_code`,
        `fp_family` AS `fp_family`,
        `fp_gencd` AS `fp_gencd`,
        `fp_create_year` AS `fp_create_year`,
        `fp_create_year_max` AS `fp_create_year_max`,
        `fp_region_lid` AS `fp_region_lid`,
        `fp_precd` AS `fp_precd`,
        `fp_highname` AS `fp_highname`,
        `fp_gna_pedigree` AS `fp_gna_pedigree`,
        `fp_linecode` AS `fp_linecode`,
        `fp_line_create_year` AS `fp_line_create_year`,
        `fp_chassis_year` AS `fp_chassis_year`,
        `fp_primary_genetic_family_lid` AS `fp_primary_genetic_family_lid`,
        `fp_secondary_genetic_family_lid` AS `fp_secondary_genetic_family_lid`,
        `fp_cpi` AS `fp_cpi`,
        COALESCE(`fp_be_bid1`, `fp_be_bid2`) AS `fp_be_bid`,
        `fp_lbg_bid` AS `fp_lbg_bid`,
        COALESCE(`fp_fp_be_bid1`, `fp_fp_be_bid2`) AS `fp_fp_be_bid`,
        COALESCE(`fp_mp_be_bid1`, `fp_mp_be_bid2`) AS `fp_mp_be_bid`,
        `fp_fp_fp_be_bid`,
        `fp_fp_mp_be_bid`,
        `fp_mp_fp_be_bid`,
        `fp_mp_mp_be_bid`,
        `fp_het_group` AS `fp_het_group`,
        `fp_het_group_pct` AS `fp_het_group_pct`,
        `fp_base_pool`,
        `fp_base_pool_rank`,
        `fp_fada_group`,
        `fp_sample` AS `fp_sample`,
        `mp_abbr_code` AS `mp_abbr_code`,
        `mp_family` AS `mp_family`,
        `mp_gencd` AS `mp_gencd`,
        `mp_create_year` AS `mp_create_year`,
        `mp_create_year_max` AS `mp_create_year_max`,
        `mp_region_lid` AS `mp_region_lid`,
        `mp_precd` AS `mp_precd`,
        `mp_highname` AS `mp_highname`,
        `mp_gna_pedigree` AS `mp_gna_pedigree`,
        `mp_linecode` AS `mp_linecode`,
        `mp_line_create_year` AS `mp_line_create_year`,
        `mp_chassis_year` AS `mp_chassis_year`,
        `mp_primary_genetic_family_lid` AS `mp_primary_genetic_family_lid`,
        `mp_secondary_genetic_family_lid` AS `mp_secondary_genetic_family_lid`,
        `mp_cpi` AS `mp_cpi`,
        COALESCE(`mp_be_bid1`,`mp_be_bid2`) AS `mp_be_bid`,
        `mp_lbg_bid` AS `mp_lbg_bid`,
        `mp_het_group` AS `mp_het_group`,
        COALESCE(`mp_fp_be_bid1`, `mp_fp_be_bid2`) AS `mp_fp_be_bid`,
        COALESCE(`mp_mp_be_bid1`, `mp_mp_be_bid2`) AS `mp_mp_be_bid`,
        `mp_fp_fp_be_bid`,
        `mp_fp_mp_be_bid`,
        `mp_mp_fp_be_bid`,
        `mp_mp_mp_be_bid`,
        `mp_het_group_pct` AS `mp_het_group_pct`,
        `mp_base_pool`,
        `mp_base_pool_rank`,
        `mp_fada_group`,
        `mp_sample` AS `mp_sample`,
        coalesce(fp_linecode,fp_precd,fp_abbr_code) AS `female`,
        coalesce(mp_linecode,mp_precd,mp_abbr_code) AS `male`,
        CASE 
            WHEN `cgenes` LIKE CONCAT('%', REGEXP_REPLACE('Bt11', '([%_])', '\\\\$1'), '%') 
              OR `cgenes` LIKE CONCAT('%', REGEXP_REPLACE('TC1507', '([%_])', '\\\\$1'), '%')
              OR `cgenes` LIKE CONCAT('%', REGEXP_REPLACE('MON89034', '([%_])', '\\\\$1'), '%') 
              OR `cgenes` LIKE CONCAT('%', REGEXP_REPLACE('MIR162', '([%_])', '\\\\$1'), '%') 
              OR `cgenes` LIKE CONCAT('%', REGEXP_REPLACE('DP-004114-3', '([%_])', '\\\\$1'), '%') 
              OR `cgenes` LIKE CONCAT('%', REGEXP_REPLACE('5307', '([%_])', '\\\\$1'), '%') 
              OR `cgenes` LIKE CONCAT('%', REGEXP_REPLACE('MIR604', '([%_])', '\\\\$1'), '%') 
              OR `cgenes` LIKE CONCAT('%', REGEXP_REPLACE('DAS-59122-7', '([%_])', '\\\\$1'), '%') 
              OR `cgenes` LIKE CONCAT('%', REGEXP_REPLACE('DP-4114', '([%_])', '\\\\$1'), '%') 
                THEN 1 
            ELSE 0 
        END AS `insect_cgenes`,
        CASE
            WHEN INSTR(REVERSE(`pedigree`),'/')<INSTR(REVERSE(`pedigree`),')') AND INSTR(REVERSE(`pedigree`),'/')>0 AND ISNOTNULL(`mp_linecode`) AND `mp_linecode` != '' THEN 'M'
            WHEN INSTR(REVERSE(`pedigree`),"/")>INSTR(REVERSE(`pedigree`),'(') AND INSTR(REVERSE(`pedigree`),'(')>0 AND ISNOTNULL(`fp_linecode`) AND `fp_linecode` != '' THEN 'F'
            WHEN (ISNULL(`fp_linecode`) OR `fp_linecode` = '') AND ISNOTNULL(`mp_linecode`) AND `mp_linecode` != '' THEN 'M'
            WHEN (ISNULL(`mp_linecode`) OR `mp_linecode` = '')  AND ISNOTNULL(`fp_linecode`) AND `fp_linecode` != '' THEN 'F'
            WHEN (INSTR(`fp_abbr_code`,')')>0) AND !(INSTR(`mp_abbr_code`,')')>0) THEN 'M'
            WHEN !(INSTR(`fp_abbr_code`,')')>0) AND (INSTR(`mp_abbr_code`,')')>0) THEN 'F'
            WHEN (`fp_chassis_year`> `create_year_max` - 3) 
                AND (`fp_region_lid` != `region_lid` AND `region_lid` IS NOT NULL AND `region_lid` != '') THEN 'M'
            WHEN (`mp_chassis_year`> `create_year_max` - 3) 
                AND (`mp_region_lid` != `region_lid` AND `region_lid` IS NOT NULL AND `region_lid` != '') THEN 'F'
            WHEN `fp_chassis_year` >= `create_year_max` AND `mp_chassis_year` < `create_year_max` THEN 'M'
            WHEN `fp_chassis_year` < `create_year_max` AND `mp_chassis_year` >= `create_year_max` THEN 'F'
            WHEN (`create_year_max` - `create_year` > 3) THEN ''
    --        WHEN (`create_year_max` = `create_year` AND `fp_line_create_year` = `fp_chassis_year` AND `mp_line_create_year` = `mp_chassis_year`) THEN ''
            WHEN `cpi` = 1 THEN ''
            WHEN (LEAST(`mp_chassis_year`,`mp_line_create_year`) < LEAST(`fp_chassis_year`,`fp_line_create_year`)) AND ((`mp_region_lid` = `region_lid`) OR (`mp_region_lid` = `fp_region_lid`) OR ISNULL(`mp_region_lid`) OR `mp_region_lid` = '') AND ISNOTNULL(`mp_linecode`) AND `mp_linecode` != '' THEN 'M'
            WHEN (LEAST(`mp_chassis_year`,`mp_line_create_year`) > LEAST(`fp_chassis_year`,`fp_line_create_year`)) AND ((`fp_region_lid` = `region_lid`) OR (`mp_region_lid` = `fp_region_lid`) OR ISNULL(`fp_region_lid`) OR `fp_region_lid` = '') AND ISNOTNULL(`fp_linecode`) AND `fp_linecode` != '' THEN 'F'
            WHEN (`fp_het_group` = `mp_het_group`) AND ISNOTNULL(`fp_het_group`) AND `fp_het_group` != '' THEN ''
            WHEN LENGTH(`fp_family`)>2 AND LENGTH(`mp_family`)<3 AND ISNOTNULL(`mp_linecode`) AND `mp_linecode` != '' THEN 'M'
            WHEN LENGTH(`fp_family`)<3 AND LENGTH(`mp_family`)>2 AND ISNOTNULL(`fp_linecode`) AND `fp_linecode` != '' THEN 'F'
            WHEN LENGTH(`family`)>2 AND INSTR(`fp_abbr_code`,`family`)>0 AND ISNOTNULL(`mp_linecode`) AND `mp_linecode` != '' THEN 'M'
            WHEN LENGTH(`family`)>2 AND INSTR(`mp_abbr_code`,`family`)>0 AND ISNOTNULL(`fp_linecode`) AND `fp_linecode` != '' THEN 'F'
    --        WHEN LENGTH(`nontester_str1`)>2 AND INSTR(`fp_abbr_code`,`nontester_str1`)>0 AND ISNOTNULL(`mp_linecode`) AND `mp_linecode` != '' THEN 'M'
    --        WHEN LENGTH(`nontester_str1`)>2 AND INSTR(`mp_abbr_code`,`nontester_str1`)>0 AND ISNOTNULL(`fp_linecode`) AND `fp_linecode` != '' THEN 'F'
            WHEN ISNOTNULL(`mp_line_create_year`) AND ISNULL(`fp_line_create_year`) AND ISNOTNULL(`mp_linecode`) AND `mp_linecode` != '' THEN 'M'
            WHEN ISNULL(`mp_line_create_year`) AND ISNOTNULL(`fp_line_create_year`) AND ISNOTNULL(`fp_linecode`) AND `fp_linecode` != '' THEN 'F'
            WHEN ((`fp_region_lid` != `region_lid`) AND ISNOTNULL(`fp_region_lid`) AND `fp_region_lid` != '') AND (`mp_region_lid` == `region_lid`) AND ISNOTNULL(`mp_linecode`) AND `mp_linecode` != '' THEN 'M'
            WHEN (`fp_region_lid` == `region_lid`) AND((`mp_region_lid` != `region_lid`) AND ISNOTNULL(`mp_region_lid`) AND `mp_region_lid` != '') AND ISNOTNULL(`fp_linecode`) AND `fp_linecode` != '' THEN 'F'
            WHEN !(INSTR(`fp_precd`,'_') > 0) AND (INSTR(`mp_precd`,'_') > 0) AND (`fp_line_create_year` = `mp_line_create_year`) THEN 'M'
            WHEN (INSTR(`fp_precd`,'_') > 0) AND !(INSTR(`mp_precd`,'_') > 0) AND (`fp_line_create_year` = `mp_line_create_year`) THEN 'F'
            WHEN ((`create_year` <= (`mp_create_year`+1))) AND !((`create_year` <= (`fp_create_year`+1))) AND ISNOTNULL(`fp_linecode`) AND `fp_linecode` != '' THEN 'F'
            WHEN ((`create_year` <= (`fp_create_year`+1))) AND !((`create_year` <= (`mp_create_year`+1))) AND ISNOTNULL(`mp_linecode`) AND `mp_linecode` != '' THEN 'M'
            WHEN (`create_year` <= `mp_line_create_year`) AND (`create_year` > (`fp_line_create_year`+2)) AND (`fp_region_lid` = `region_lid`) THEN 'F'
            WHEN (`create_year` <= `fp_line_create_year`) AND (`create_year` > (`mp_line_create_year`+2)) AND (`mp_region_lid` = `region_lid`) THEN 'M'
            WHEN INSTR(LOWER(`pedigree`),'ver')>0 AND INSTR( LOWER( `fp_precd`), 'ver')>0 AND ISNOTNULL(`mp_linecode`) AND `mp_linecode` != '' THEN 'M'
            WHEN INSTR(LOWER(`pedigree`),'ver')>0 AND INSTR( LOWER( `mp_precd`), 'ver')>0 AND ISNOTNULL(`fp_linecode`) AND `fp_linecode` != '' THEN 'F'
            WHEN (`mp_chassis_year` = `fp_chassis_year`) AND (`mp_line_create_year` < `fp_line_create_year`) THEN 'M'
            WHEN (`mp_chassis_year` = `fp_chassis_year`) AND (`mp_line_create_year` > `fp_line_create_year`) THEN 'F'
        END AS `tester_role`,
        CASE
            WHEN INSTR(REVERSE(`pedigree`),'/')<INSTR(REVERSE(`pedigree`),')') AND INSTR(REVERSE(`pedigree`),'/')>0 AND ISNOTNULL(`mp_linecode`) AND `mp_linecode` != '' THEN 1
            WHEN INSTR(REVERSE(`pedigree`),"/")>INSTR(REVERSE(`pedigree`),'(') AND INSTR(REVERSE(`pedigree`),'(')>0 AND ISNOTNULL(`fp_linecode`) AND `fp_linecode` != '' THEN 2
            WHEN (ISNULL(`fp_linecode`) OR `fp_linecode` = '') AND ISNOTNULL(`mp_linecode`) AND `mp_linecode` != '' THEN 3
            WHEN (ISNULL(`mp_linecode`) OR `mp_linecode` = '')  AND ISNOTNULL(`fp_linecode`) AND `fp_linecode` != '' THEN 4
            WHEN (INSTR(`fp_abbr_code`,')')>0) AND !(INSTR(`mp_abbr_code`,')')>0) THEN 5
            WHEN !(INSTR(`fp_abbr_code`,')')>0) AND (INSTR(`mp_abbr_code`,')')>0) THEN 6
            WHEN (`fp_chassis_year`> `create_year_max` - 3) 
                AND (`fp_region_lid` != `region_lid` AND `region_lid` IS NOT NULL AND `region_lid` != '') THEN 7
            WHEN (`mp_chassis_year`> `create_year_max` - 3) 
                AND (`mp_region_lid` != `region_lid` AND `region_lid` IS NOT NULL AND `region_lid` != '') THEN 8
            WHEN `fp_chassis_year` >= `create_year_max` AND `mp_chassis_year` < `create_year_max` THEN 9
            WHEN `fp_chassis_year` < `create_year_max` AND `mp_chassis_year` >= `create_year_max` THEN 10
            WHEN (`create_year_max` - `create_year` > 3) THEN 11
    --        WHEN (`create_year_max` = `create_year` AND `fp_line_create_year` = `fp_chassis_year` AND `mp_line_create_year` = `mp_chassis_year`) THEN 10
            WHEN `cpi` = 1 THEN 12
    --        WHEN () AND ((`fp_region_lid` != `region_lid`) OR (`region_lid` IS NULL OR `region_lid` = '' AND `fp_region_lid` != `mp_region_lid` AND `fp_region_lid` IS NOT NULL AND `fp_region_lid` != ''))
            WHEN (LEAST(`mp_chassis_year`,`mp_line_create_year`) < LEAST(`fp_chassis_year`,`fp_line_create_year`)) AND ((`mp_region_lid` = `region_lid`) OR (`mp_region_lid` = `fp_region_lid`) OR ISNULL(`mp_region_lid`) OR `mp_region_lid` = '') AND ISNOTNULL(`mp_linecode`) AND `mp_linecode` != '' THEN 13
            WHEN (LEAST(`mp_chassis_year`,`mp_line_create_year`) > LEAST(`fp_chassis_year`,`fp_line_create_year`)) AND ((`fp_region_lid` = `region_lid`) OR (`mp_region_lid` = `fp_region_lid`) OR ISNULL(`fp_region_lid`) OR `fp_region_lid` = '') AND ISNOTNULL(`fp_linecode`) AND `fp_linecode` != '' THEN 14
            WHEN (`fp_het_group` = `mp_het_group`) AND ISNOTNULL(`fp_het_group`) AND `fp_het_group` != '' THEN 15
            WHEN LENGTH(`fp_family`)>2 AND LENGTH(`mp_family`)<3 AND ISNOTNULL(`mp_linecode`) AND `mp_linecode` != '' THEN 16
            WHEN LENGTH(`fp_family`)<3 AND LENGTH(`mp_family`)>2 AND ISNOTNULL(`fp_linecode`) AND `fp_linecode` != '' THEN 17
            WHEN LENGTH(`family`)>2 AND INSTR(`fp_abbr_code`,`family`)>0 AND ISNOTNULL(`mp_linecode`) AND `mp_linecode` != '' THEN 18
            WHEN LENGTH(`family`)>2 AND INSTR(`mp_abbr_code`,`family`)>0 AND ISNOTNULL(`fp_linecode`) AND `fp_linecode` != '' THEN 19
    --        WHEN LENGTH(`nontester_str1`)>2 AND INSTR(`fp_abbr_code`,`nontester_str1`)>0 AND ISNOTNULL(`mp_linecode`) AND `mp_linecode` != '' THEN 20
    --        WHEN LENGTH(`nontester_str1`)>2 AND INSTR(`mp_abbr_code`,`nontester_str1`)>0 AND ISNOTNULL(`fp_linecode`) AND `fp_linecode` != '' THEN 21
            WHEN ISNOTNULL(`mp_line_create_year`) AND ISNULL(`fp_line_create_year`) AND ISNOTNULL(`mp_linecode`) AND `mp_linecode` != '' THEN 22
            WHEN ISNULL(`mp_line_create_year`) AND ISNOTNULL(`fp_line_create_year`) AND ISNOTNULL(`fp_linecode`) AND `fp_linecode` != '' THEN 23
            WHEN ((`fp_region_lid` != `region_lid`) AND ISNOTNULL(`fp_region_lid`) AND `fp_region_lid` != '') AND (`mp_region_lid` == `region_lid`) AND ISNOTNULL(`mp_linecode`) AND `mp_linecode` != '' THEN 24
            WHEN (`fp_region_lid` == `region_lid`) AND((`mp_region_lid` != `region_lid`) AND ISNOTNULL(`mp_region_lid`) AND `mp_region_lid` != '') AND ISNOTNULL(`fp_linecode`) AND `fp_linecode` != '' THEN 25
            WHEN ((`create_year` <= (`mp_create_year`+1))) AND !((`create_year` <= (`fp_create_year`+1))) AND ISNOTNULL(`fp_linecode`) AND `fp_linecode` != '' THEN 26
            WHEN ((`create_year` <= (`fp_create_year`+1))) AND !((`create_year` <= (`mp_create_year`+1))) AND ISNOTNULL(`mp_linecode`) AND `mp_linecode` != '' THEN 27
            WHEN (`create_year` <= `mp_line_create_year`) AND (`create_year` > (`fp_line_create_year`+2)) AND (`fp_region_lid` = `region_lid`) THEN 28
            WHEN (`create_year` <= `fp_line_create_year`) AND (`create_year` > (`mp_line_create_year`+2)) AND (`mp_region_lid` = `region_lid`) THEN 29
            WHEN !(INSTR(`fp_precd`,'_') > 0) AND (INSTR(`mp_precd`,'_') > 0) AND (`fp_line_create_year` = `mp_line_create_year`) THEN 30
            WHEN (INSTR(`fp_precd`,'_') > 0) AND !(INSTR(`mp_precd`,'_') > 0) AND (`fp_line_create_year` = `mp_line_create_year`) THEN 31
            WHEN INSTR(LOWER(`pedigree`),'ver')>0 AND INSTR( LOWER( `fp_precd`), 'ver')>0 AND ISNOTNULL(`mp_linecode`) AND `mp_linecode` != '' THEN 32
            WHEN INSTR(LOWER(`pedigree`),'ver')>0 AND INSTR( LOWER( `mp_precd`), 'ver')>0 AND ISNOTNULL(`fp_linecode`) AND `fp_linecode` != '' THEN 33
            WHEN (`mp_chassis_year` = `fp_chassis_year`) AND (`mp_line_create_year` < `fp_line_create_year`) THEN 34
            WHEN (`mp_chassis_year` = `fp_chassis_year`) AND (`mp_line_create_year` > `fp_line_create_year`) THEN 35
            ELSE 0
        END AS `rule`
      FROM (
        SELECT 
            `filled_material_tester_calculated_rough_by_be_bid`.`abbr_code` AS `abbr_code`,
            `filled_material_tester_calculated_rough_by_be_bid`.`family` AS `family`,
            `filled_material_tester_calculated_rough_by_be_bid`.`gencd` AS `gencd`,
            `filled_material_tester_calculated_rough_by_be_bid`.`create_year` AS `create_year`,
            `filled_material_tester_calculated_rough_by_be_bid`.`create_year_max` AS `create_year_max`,
            `filled_material_tester_calculated_rough_by_be_bid`.`pedigree` AS `pedigree`,
            `filled_material_tester_calculated_rough_by_be_bid`.`gmo` AS `gmo`,
            `filled_material_tester_calculated_rough_by_be_bid`.`region_lid` AS `region_lid`,
            `filled_material_tester_calculated_rough_by_be_bid`.`precd` AS `precd`,
            `filled_material_tester_calculated_rough_by_be_bid`.`highname` AS `highname`,
            `filled_material_tester_calculated_rough_by_be_bid`.`gna_pedigree` AS `gna_pedigree`,
            `filled_material_tester_calculated_rough_by_be_bid`.`primary_genetic_family_lid` AS `primary_genetic_family_lid`,
            `filled_material_tester_calculated_rough_by_be_bid`.`secondary_genetic_family_lid` AS `secondary_genetic_family_lid`,
            `filled_material_tester_calculated_rough_by_be_bid`.`cpi` AS `cpi`,
            `filled_material_tester_calculated_rough_by_be_bid`.`be_bid` AS `be_bid`,
            `filled_material_tester_calculated_rough_by_be_bid`.`lbg_bid` AS `lbg_bid`,
            `filled_material_tester_calculated_rough_by_be_bid`.`fp_be_bid` AS `fp_be_bid1`,
            `filled_material_tester_calculated_rough_by_be_bid`.`mp_be_bid` AS `mp_be_bid1`,
            `filled_material_tester_calculated_rough_by_be_bid`.`fp_fp_be_bid` AS `fp_fp_be_bid1`,
            `filled_material_tester_calculated_rough_by_be_bid`.`fp_mp_be_bid` AS `fp_mp_be_bid1`,
            `filled_material_tester_calculated_rough_by_be_bid`.`mp_fp_be_bid` AS `mp_fp_be_bid1`,
            `filled_material_tester_calculated_rough_by_be_bid`.`mp_mp_be_bid` AS `mp_mp_be_bid1`,
            `filled_material_tester_calculated_rough_by_be_bid`.`genetic_group_name` AS `fada_group`,
            COALESCE(`filled_material_tester_calculated_rough_by_be_bid`.`cgenes`,
                     `fmtc_fp`.`cgenes`,
                     `fmtc_mp`.`cgenes`) AS `cgenes`,
            `fmtc_fp`.`abbr_code` AS `fp_abbr_code`,
            `fmtc_fp`.`family` AS `fp_family`,
            `fmtc_fp`.`gencd` AS `fp_gencd`,
            `fmtc_fp`.`create_year` AS `fp_create_year`,
            `fmtc_fp`.`create_year_max` AS `fp_create_year_max`,
            `fmtc_fp`.`region_lid` AS `fp_region_lid`,
            `fmtc_fp`.`precd` AS `fp_precd`,
            `fmtc_fp`.`highname` AS `fp_highname`,
            `fmtc_fp`.`gna_pedigree` AS `fp_gna_pedigree`,
            `fmtc_fp`.`linecode` AS `fp_linecode`,
            `fmtc_fp`.`line_create_year` AS `fp_line_create_year`,
            `fmtc_fp`.`chassis_year` AS `fp_chassis_year`,
            `fmtc_fp`.`primary_genetic_family_lid` AS `fp_primary_genetic_family_lid`,
            `fmtc_fp`.`secondary_genetic_family_lid` AS `fp_secondary_genetic_family_lid`,
            `fmtc_fp`.`cpi` AS `fp_cpi`,
            `fmtc_fp`.`be_bid` AS `fp_be_bid2`,
            `fmtc_fp`.`lbg_bid` AS `fp_lbg_bid`,
            `fmtc_fp`.`fp_be_bid` AS `fp_fp_be_bid2`,
            `fmtc_fp`.`mp_be_bid` AS `fp_mp_be_bid2`,
            `fmtc_fp`.`fp_fp_be_bid` AS `fp_fp_fp_be_bid`,
            `fmtc_fp`.`fp_mp_be_bid` AS `fp_fp_mp_be_bid`,
            `fmtc_fp`.`mp_fp_be_bid` AS `fp_mp_fp_be_bid`,
            `fmtc_fp`.`mp_mp_be_bid` AS `fp_mp_mp_be_bid`,
            `fmtc_fp`.`het_group` AS `fp_het_group`,
            `fmtc_fp`.`het_group_pct` AS `fp_het_group_pct`,
            `fmtc_fp`.`base_pool` AS `fp_base_pool`,
            `fmtc_fp`.`base_pool_rank` AS `fp_base_pool_rank`,
            `fmtc_fp`.`genetic_group_name` AS `fp_fada_group`,
            `fmtc_fp`.`sample` AS `fp_sample`,
            `fmtc_mp`.`abbr_code` AS `mp_abbr_code`,
            `fmtc_mp`.`family` AS `mp_family`,
            `fmtc_mp`.`gencd` AS `mp_gencd`,
            `fmtc_mp`.`create_year` AS `mp_create_year`,
            `fmtc_mp`.`create_year_max` AS `mp_create_year_max`,
            `fmtc_mp`.`region_lid` AS `mp_region_lid`,
            `fmtc_mp`.`precd` AS `mp_precd`,
            `fmtc_mp`.`highname` AS `mp_highname`,
            `fmtc_mp`.`gna_pedigree` AS `mp_gna_pedigree`,
            `fmtc_mp`.`linecode` AS `mp_linecode`,
            `fmtc_mp`.`line_create_year` AS `mp_line_create_year`,
            `fmtc_mp`.`chassis_year` AS `mp_chassis_year`,
            `fmtc_mp`.`primary_genetic_family_lid` AS `mp_primary_genetic_family_lid`,
            `fmtc_mp`.`secondary_genetic_family_lid` AS `mp_secondary_genetic_family_lid`,
            `fmtc_mp`.`cpi` AS `mp_cpi`,
            `fmtc_mp`.`be_bid` AS `mp_be_bid2`,
            `fmtc_mp`.`lbg_bid` AS `mp_lbg_bid`,
            `fmtc_mp`.`fp_be_bid` AS `mp_fp_be_bid2`,
            `fmtc_mp`.`mp_be_bid` AS `mp_mp_be_bid2`,
            `fmtc_mp`.`fp_fp_be_bid` AS `mp_fp_fp_be_bid`,
            `fmtc_mp`.`fp_mp_be_bid` AS `mp_fp_mp_be_bid`,
            `fmtc_mp`.`mp_fp_be_bid` AS `mp_mp_fp_be_bid`,
            `fmtc_mp`.`mp_mp_be_bid` AS `mp_mp_mp_be_bid`,
            `fmtc_mp`.`het_group` AS `mp_het_group`,
            `fmtc_mp`.`het_group_pct` AS `mp_het_group_pct`,
            `fmtc_mp`.`base_pool` AS `mp_base_pool`,
            `fmtc_mp`.`base_pool_rank` AS `mp_base_pool_rank`,
            `fmtc_mp`.`genetic_group_name` AS `mp_fada_group`,
            `fmtc_mp`.`sample` AS `mp_sample`
          FROM `filled_material_tester_calculated_rough_by_be_bid` `filled_material_tester_calculated_rough_by_be_bid`
          LEFT JOIN `filled_material_tester_calculated_rough_by_be_bid` `fmtc_fp`
            ON `filled_material_tester_calculated_rough_by_be_bid`.`fp_be_bid` = `fmtc_fp`.`be_bid`
          LEFT JOIN `filled_material_tester_calculated_rough_by_be_bid` `fmtc_mp`
            ON `filled_material_tester_calculated_rough_by_be_bid`.`mp_be_bid` = `fmtc_mp`.`be_bid`
        ) `withoutcomputedcols_query`
    """)
    return output_df

def compute_fmtc_rough_34(spark):
    output_df = spark.sql("""
    SELECT 
        be_bid AS be_bid,
        lbg_bid AS lbg_bid,
        MODE(abbr_code) AS abbr_code,
        MODE(family) AS family,
        MODE(gencd) AS gencd,
        MIN(create_year) AS create_year,
        MAX(create_year_max) AS create_year_max,
        MODE(pedigree) AS pedigree,
        CAST( (MAX(CAST( (gmo) AS INT))) AS BOOLEAN) AS gmo,
        MODE(region_lid) AS region_lid,
        MODE(precd) AS precd,
        MODE(highname) AS highname,
        MODE(gna_pedigree) AS gna_pedigree,
        MODE(linecode) AS linecode,
        MODE(chassis) AS chassis,
        MIN(line_create_year) AS line_create_year,
        MIN(chassis_year) AS chassis_year,
        MODE(primary_genetic_family_lid) AS primary_genetic_family_lid,
        MODE(secondary_genetic_family_lid) AS secondary_genetic_family_lid,
        MAX(cpi) AS cpi,
        MAX(cgenes) AS cgenes,
        MODE(fp_be_bid) AS fp_be_bid,
        MODE(mp_be_bid) AS mp_be_bid,
        MODE(fp_fp_be_bid) AS fp_fp_be_bid,
        MODE(fp_mp_be_bid) AS fp_mp_be_bid,
        MODE(mp_fp_be_bid) AS mp_fp_be_bid,
        MODE(mp_mp_be_bid) AS mp_mp_be_bid,
        MODE(het_group) AS het_group,
        MODE(het_group_pct) AS het_group_pct,
        MODE(genetic_group_name) AS genetic_group_name,
        MODE(base_pool) AS base_pool,
        MODE(base_pool_rank) AS base_pool_rank,
        MODE(sample) AS sample,
        MODE(sample_source) AS sample_source,
        COUNT(*) AS count
      FROM (
        SELECT
            mat.abbr_code,
            COALESCE(mat.family,mat.gas_pedigree_stem) AS family,
            mat.generation_code AS gencd,
            mat.pollination_type_lid,
            mat.create_year,
            mat.create_year_max,
            mat.pedigree,
            mat.gmo_lid AS gmo,
            mat.region_lid,
            mat.pre_line_code AS precd,
            mat.highname,
            mat.stack_guid,
            mat.gas_pedigree_stem AS gna_pedigree,
            mat.line_code AS linecode,
            mat.chassis,
            mat.line_create_year,
            mat.chassis_year,
            mat.primary_genetic_family_lid,
            mat.secondary_genetic_family_lid,
            COALESCE(be_bid_cpi.cpi,mat.cpi) AS cpi,
            mat.be_bid,
            mat.lbg_bid,
            mat.fp_be_bid,
            mat.mp_be_bid,
            mat.fp_fp_be_bid,
            mat.fp_mp_be_bid,
            mat.mp_fp_be_bid,
            mat.mp_mp_be_bid,
            mat.cgenes,
            COALESCE(fada_by_bebid.ss_nss, fada_by_precd.ss_nss, 
                     fada_by_lincd.ss_nss, fada_by_chassis.ss_nss, 
                     fada_by_family.ss_nss, fada_by_lbg_bid.ss_nss) AS het_group,
            COALESCE(fada_by_bebid.membership_pct, fada_by_precd.membership_pct, 
                     fada_by_lincd.membership_pct, fada_by_chassis.membership_pct, 
                     fada_by_family.membership_pct_max, fada_by_lbg_bid.membership_pct) AS het_group_pct,
            COALESCE(fada_by_bebid.genetic_group_name, fada_by_precd.genetic_group_name, 
                     fada_by_lincd.genetic_group_name, fada_by_chassis.genetic_group_name, 
                     fada_by_family.genetic_group_name, fada_by_lbg_bid.genetic_group_name) AS genetic_group_name,
            CASE
                WHEN ((fada_by_bebid.base_pool != fada_by_lbg_bid.base_pool AND fada_by_bebid.base_pool IS NOT NULL AND fada_by_lbg_bid.base_pool IS NOT NULL))
                    THEN NULL
                ELSE
                    COALESCE(fada_by_bebid.base_pool, fada_by_precd.base_pool, 
                     fada_by_lincd.base_pool, fada_by_chassis.base_pool, 
                     fada_by_lbg_bid.base_pool)
            END AS base_pool, 
            CASE
                WHEN ((fada_by_bebid.base_pool != fada_by_lbg_bid.base_pool AND fada_by_bebid.base_pool IS NOT NULL AND fada_by_lbg_bid.base_pool IS NOT NULL))
                    THEN NULL
                ELSE
                    COALESCE(fada_by_bebid.base_pool_rank, fada_by_precd.base_pool_rank, 
                     fada_by_lincd.base_pool_rank, fada_by_chassis.base_pool_rank, 
                     fada_by_lbg_bid.base_pool_rank)
            END AS base_pool_rank,
            COALESCE(sample_bebid.sample_id, 
                     IF(sample_precode.genotype_count > sample_linecode.genotype_count, 
                        sample_precode.sample_id, sample_linecode.sample_id),
                     sample_linecode.sample_id, sample_precode.sample_id,
                     sample_chassis.sample_id) AS sample,
            CASE
                WHEN ISNOTNULL(sample_bebid.sample_id) THEN 'bebid'
                WHEN sample_precode.genotype_count > sample_linecode.genotype_count THEN 'precode'
                WHEN ISNOTNULL(sample_linecode.genotype_count) THEN 'linecode'
                WHEN ISNOTNULL(sample_precode.genotype_count) THEN 'precode'
                WHEN ISNOTNULL(sample_chassis.sample_id) THEN 'cha'
                ELSE 'none'
            END AS sample_source,
            CASE
                WHEN ISNOTNULL(sample_bebid.sample_id) THEN sample_bebid.genotype_count
                WHEN sample_precode.genotype_count > sample_linecode.genotype_count THEN sample_precode.genotype_count
                ELSE COALESCE(sample_linecode.genotype_count,
                              sample_precode.genotype_count,
                              sample_chassis.genotype_count)
            END AS genotype_count
          FROM material_prepared_joined mat
          LEFT JOIN sample_precode sample_precode
            ON mat.pre_line_code = sample_precode.pre_line_code
          LEFT JOIN sample_linecode sample_linecode
            ON mat.line_code = sample_linecode.line_code
          LEFT JOIN fada_by_precd fada_by_precd
            ON mat.pre_line_code = fada_by_precd.pre_line_code
          LEFT JOIN fada_by_lincd fada_by_lincd
            ON mat.line_code = fada_by_lincd.line_code
          LEFT JOIN fada_by_chassis fada_by_chassis
            ON mat.chassis = fada_by_chassis.chassis
          LEFT JOIN sample_chassis sample_chassis
            ON mat.chassis = sample_chassis.chassis
          LEFT JOIN fada_by_family fada_by_family
            ON mat.family = fada_by_family.family
          LEFT JOIN sample_bebid sample_bebid
            ON mat.be_bid = sample_bebid.be_bid
          LEFT JOIN fada_by_bebid fada_by_bebid
            ON mat.be_bid = fada_by_bebid.be_bid
          LEFT JOIN fada_by_lbg_bid fada_by_lbg_bid
            ON mat.lbg_bid = fada_by_lbg_bid.lbg_bid
          LEFT JOIN be_bid_cpi
            ON mat.be_bid = be_bid_cpi.be_bid
    ) joined_table
    WHERE fp_be_bid IS NOT NULL OR mp_be_bid IS NOT NULL OR sample IS NOT NULL
    GROUP BY be_bid, lbg_bid
    """)
    
    return output_df

def compute_bebid_cpi(spark):
  df = spark.sql("""
    SELECT
        be_bid,
        MAX(cpi) AS cpi
      FROM (
        SELECT
            be_bid,
            MAX(cpi) AS cpi
          FROM material_prepared_joined
        WHERE cpi IS NOT NULL
          AND be_bid IS NOT NULL
        GROUP BY be_bid

        UNION ALL
        SELECT
            fp_be_bid AS be_bid,
            MAX(cpi) AS cpi
          FROM material_prepared_joined
        WHERE cpi IS NOT NULL
          AND fp_be_bid IS NOT NULL
        GROUP BY fp_be_bid


        UNION ALL
        SELECT
            mp_be_bid AS be_bid,
            MAX(cpi) AS cpi
          FROM material_prepared_joined
        WHERE cpi IS NOT NULL
          AND mp_be_bid IS NOT NULL
        GROUP BY mp_be_bid
    ) be_bid_cpi
    GROUP BY be_bid
  """)

  return df

def compute_fmtc_rough(spark):
    output_df = spark.sql("""
    SELECT 
        be_bid,
        lbg_bid,
        FIRST(abbr_code) AS abbr_code,
        FIRST(family) AS family,
        FIRST(gencd) AS gencd,
        MIN(create_year) AS create_year,
        MAX(create_year_max) AS create_year_max,
        FIRST(pedigree) AS pedigree,
        CAST( (MAX(CAST( (gmo) AS INT))) AS BOOLEAN) AS gmo,
        FIRST(region_lid) AS region_lid,
        FIRST(precd) AS precd,
        FIRST(highname) AS highname,
        FIRST(gna_pedigree) AS gna_pedigree,
        FIRST(linecode) AS linecode,
        FIRST(chassis) AS chassis,
        MIN(line_create_year) AS line_create_year,
        MIN(chassis_year) AS chassis_year,
        FIRST(primary_genetic_family_lid) AS primary_genetic_family_lid,
        FIRST(secondary_genetic_family_lid) AS secondary_genetic_family_lid,
        MAX(cpi) AS cpi,
        MAX(cgenes) AS cgenes,
        FIRST(fp_be_bid) AS fp_be_bid,
        FIRST(mp_be_bid) AS mp_be_bid,
        FIRST(fp_fp_be_bid) AS fp_fp_be_bid,
        FIRST(fp_mp_be_bid) AS fp_mp_be_bid,
        FIRST(mp_fp_be_bid) AS mp_fp_be_bid,
        FIRST(mp_mp_be_bid) AS mp_mp_be_bid,
        FIRST(het_group) AS het_group,
        FIRST(het_group_pct) AS het_group_pct,
        FIRST(genetic_group_name) AS genetic_group_name,
        FIRST(base_pool) AS base_pool,
        FIRST(base_pool_rank) AS base_pool_rank,
        FIRST(sample) AS sample,
        FIRST(sample_source) AS sample_source,
        COUNT(*) AS count
      FROM (
        SELECT
            mat.material_id AS matid,
            mat.abbr_code,
            COALESCE(mat.family,mat.gas_pedigree_stem) AS family,
            mat.generation_code AS gencd,
            mat.pollination_type_lid,
            mat.create_year,
            mat.create_year_max,
            mat.pedigree,
            mat.gmo_lid AS gmo,
            mat.region_lid,
            mat.pre_line_code AS precd,
            mat.highname,
            mat.stack_guid,
            mat.gas_pedigree_stem AS gna_pedigree,
            mat.line_code AS linecode,
            mat.chassis,
            mat.line_create_year,
            mat.chassis_year,
            mat.primary_genetic_family_lid,
            mat.secondary_genetic_family_lid,
            COALESCE(be_bid_cpi.cpi,mat.cpi) AS cpi,
            mat.be_bid,
            mat.lbg_bid,
            mat.fp_be_bid,
            mat.mp_be_bid,
            mat.fp_fp_be_bid,
            mat.fp_mp_be_bid,
            mat.mp_fp_be_bid,
            mat.mp_mp_be_bid,
            mat.cgenes,
            COALESCE(fada_by_bebid.ss_nss, fada_by_precd.ss_nss, 
                     fada_by_lincd.ss_nss, fada_by_chassis.ss_nss, 
                     fada_by_family.ss_nss, fada_by_lbg_bid.ss_nss) AS het_group,
            COALESCE(fada_by_bebid.membership_pct, fada_by_precd.membership_pct, 
                     fada_by_lincd.membership_pct, fada_by_chassis.membership_pct, 
                     fada_by_family.membership_pct_max, fada_by_lbg_bid.membership_pct) AS het_group_pct,
            COALESCE(fada_by_bebid.genetic_group_name, fada_by_precd.genetic_group_name, 
                     fada_by_lincd.genetic_group_name, fada_by_chassis.genetic_group_name, 
                     fada_by_family.genetic_group_name, fada_by_lbg_bid.genetic_group_name) AS genetic_group_name,
            CASE
                WHEN ((fada_by_bebid.base_pool != fada_by_lbg_bid.base_pool AND fada_by_bebid.base_pool IS NOT NULL AND fada_by_lbg_bid.base_pool IS NOT NULL))
                    THEN NULL
                ELSE
                    COALESCE(fada_by_bebid.base_pool, fada_by_precd.base_pool, 
                     fada_by_lincd.base_pool, fada_by_chassis.base_pool, 
                     fada_by_lbg_bid.base_pool)
            END AS base_pool, 
            CASE
                WHEN ((fada_by_bebid.base_pool != fada_by_lbg_bid.base_pool AND fada_by_bebid.base_pool IS NOT NULL AND fada_by_lbg_bid.base_pool IS NOT NULL))
                    THEN NULL
                ELSE
                    COALESCE(fada_by_bebid.base_pool_rank, fada_by_precd.base_pool_rank, 
                     fada_by_lincd.base_pool_rank, fada_by_chassis.base_pool_rank, 
                     fada_by_lbg_bid.base_pool_rank)
            END AS base_pool_rank,
            COALESCE(sample_bebid.sample_id, 
                     IF(sample_precode.genotype_count > sample_linecode.genotype_count, 
                        sample_precode.sample_id, sample_linecode.sample_id),
                     sample_linecode.sample_id, sample_precode.sample_id,
                     sample_chassis.sample_id) AS sample,
            CASE
                WHEN ISNOTNULL(sample_bebid.sample_id) THEN 'bebid'
                WHEN sample_precode.genotype_count > sample_linecode.genotype_count THEN 'precode'
                WHEN ISNOTNULL(sample_linecode.genotype_count) THEN 'linecode'
                WHEN ISNOTNULL(sample_precode.genotype_count) THEN 'precode'
                WHEN ISNOTNULL(sample_chassis.sample_id) THEN 'cha'
                ELSE 'none'
            END AS sample_source,
            CASE
                WHEN ISNOTNULL(sample_bebid.sample_id) THEN sample_bebid.genotype_count
                WHEN sample_precode.genotype_count > sample_linecode.genotype_count THEN sample_precode.genotype_count
                ELSE COALESCE(sample_linecode.genotype_count,
                              sample_precode.genotype_count,
                              sample_chassis.genotype_count)
            END AS genotype_count
          FROM material_prepared_joined mat
          LEFT JOIN sample_precode sample_precode
            ON mat.pre_line_code = sample_precode.pre_line_code
          LEFT JOIN sample_linecode sample_linecode
            ON mat.line_code = sample_linecode.line_code
          LEFT JOIN fada_by_precd fada_by_precd
            ON mat.pre_line_code = fada_by_precd.pre_line_code
          LEFT JOIN fada_by_lincd fada_by_lincd
            ON mat.line_code = fada_by_lincd.line_code
          LEFT JOIN fada_by_chassis fada_by_chassis
            ON mat.chassis = fada_by_chassis.chassis
          LEFT JOIN sample_chassis sample_chassis
            ON mat.chassis = sample_chassis.chassis
          LEFT JOIN fada_by_family fada_by_family
            ON mat.family = fada_by_family.family
          LEFT JOIN sample_bebid sample_bebid
            ON mat.be_bid = sample_bebid.be_bid
          LEFT JOIN fada_by_bebid fada_by_bebid
            ON mat.be_bid = fada_by_bebid.be_bid
          LEFT JOIN fada_by_lbg_bid fada_by_lbg_bid
            ON mat.lbg_bid = fada_by_lbg_bid.lbg_bid
          LEFT JOIN(
            SELECT
                be_bid,
                MAX(cpi) AS cpi
              FROM (
                SELECT
                    be_bid,
                    MAX(cpi) AS cpi
                  FROM material_prepared_joined
                WHERE cpi IS NOT NULL
                  AND be_bid IS NOT NULL
                GROUP BY be_bid

                UNION ALL
                SELECT
                    fp_be_bid AS be_bid,
                    MAX(cpi) AS cpi
                  FROM material_prepared_joined
                WHERE cpi IS NOT NULL
                  AND fp_be_bid IS NOT NULL
                GROUP BY fp_be_bid


                UNION ALL
                SELECT
                    mp_be_bid AS be_bid,
                    MAX(cpi) AS cpi
                  FROM material_prepared_joined
                WHERE cpi IS NOT NULL
                  AND mp_be_bid IS NOT NULL
                GROUP BY mp_be_bid
            ) be_bid_cpi
            GROUP BY be_bid
          ) be_bid_cpi
            ON mat.be_bid = be_bid_cpi.be_bid
          ORDER BY mat.material_id
    ) joined_table
    WHERE fp_be_bid IS NOT NULL OR mp_be_bid IS NOT NULL OR sample IS NOT NULL
    GROUP BY be_bid, lbg_bid
    """)
    
    return output_df


def compute_material_prepared_joined(spark):
    output_df = spark.sql("""
        SELECT 
            `material_prepared`.`material_guid` AS `material_guid`,
            COALESCE(`material_prepared`.`line_guid`,`material_prepared_by_pre_line_code`.`line_guid_first`,`material_prepared_by_bebid`.`line_guid_first`, `material_prepared_by_line_code`.`line_guid_first`) AS `line_guid`,
            `material_prepared`.`material_id` AS `material_id`,
            `material_prepared`.`abbr_code` AS `abbr_code`,
            COALESCE(`material_prepared`.`family`,`material_prepared_by_pre_line_code`.`family_first`,`material_prepared_by_bebid`.`family_first`) AS `family`,
            `material_prepared`.`highname` AS `highname`,
            `material_prepared`.`generation_code` AS `generation_code`,
            `material_prepared`.`pollination_type_lid` AS `pollination_type_lid`,
            LEAST(`material_prepared`.`create_year`,`material_prepared_by_pre_line_code`.`create_year_min`,`material_prepared_by_bebid`.`create_year_min`) AS `create_year`,
            GREATEST(`material_prepared`.`create_year`,`material_prepared_by_pre_line_code`.`create_year_max`,`material_prepared_by_bebid`.`create_year_max`) AS `create_year_max`,
            `material_prepared`.`pedigree` AS `pedigree`,
            `material_prepared`.`gmo_lid` AS `gmo_lid`,
            COALESCE(`material_prepared`.`region_lid`,`material_prepared_by_pre_line_code`.`region_lid_first`, `material_prepared_by_bebid`.`region_lid_first`, `material_prepared_by_line_code`.`region_lid_first`) AS `region_lid`,
            COALESCE(`material_prepared`.`pre_line_code`, `material_prepared_by_bebid`.`pre_line_code_first`, `material_prepared_by_line_code`.`pre_line_code`) AS `pre_line_code`,
            `material_prepared`.`stack_guid` AS `stack_guid`,
            COALESCE(`material_prepared`.`gas_pedigree_stem`,`material_prepared_by_pre_line_code`.`gas_pedigree_stem_first`,`material_prepared_by_bebid`.`gas_pedigree_stem_first`) AS `gas_pedigree_stem`,
            COALESCE(`material_prepared`.`line_code`,`material_prepared_by_pre_line_code`.`line_code_first`, `material_prepared_by_bebid`.`line_code_first`, `material_prepared_by_line_code`.`line_code_first`) AS `line_code`,
            COALESCE(`material_prepared`.`chassis`,`material_prepared_by_pre_line_code`.`chassis_first`, `material_prepared_by_bebid`.`chassis_first`, `material_prepared_by_line_code`.`chassis_first`) AS `chassis`,
            COALESCE(LEAST(`material_prepared`.`line_create_year`,`material_prepared_by_pre_line_code`.`line_create_year_min`),`material_prepared_by_bebid`.`line_create_year_min`, `material_prepared_by_line_code`.`line_create_year_min`) AS `line_create_year`,
            COALESCE(LEAST(`material_prepared`.`chassis_year`,`material_prepared_by_pre_line_code`.`chassis_year_min`),`material_prepared_by_bebid`.`chassis_year_min`, `material_prepared_by_line_code`.`chassis_year_min`) AS `chassis_year`,
            COALESCE(`material_prepared`.`primary_genetic_family_lid`,`material_prepared_by_pre_line_code`.`primary_genetic_family_lid_first`, `material_prepared_by_bebid`.`primary_genetic_family_lid_first`,`material_prepared_by_line_code`.`primary_genetic_family_lid_first`) AS `primary_genetic_family_lid`,
            COALESCE(`material_prepared`.`secondary_genetic_family_lid`,`material_prepared_by_pre_line_code`.`secondary_genetic_family_lid_first`, `material_prepared_by_bebid`.`secondary_genetic_family_lid_first`,`material_prepared_by_line_code`.`secondary_genetic_family_lid_first`) AS `secondary_genetic_family_lid`,
            GREATEST(`material_prepared`.`cpi`,`material_prepared_by_pre_line_code`.`cpi_max`,`material_prepared_by_bebid`.`cpi_max`) AS `cpi`,
            COALESCE(`material_prepared`.`be_bid`,`material_prepared_by_pre_line_code`.`be_bid_first`, `material_prepared_by_line_code`.`be_bid_first`) AS `be_bid`,
            `material_prepared`.`lbg_bid` AS `lbg_bid`,
            COALESCE(`material_prepared`.`female_be_bid`, `material_prepared_by_bebid`.`female_be_bid_first`,`material_prepared_by_pre_line_code`.`female_be_bid_first`, `material_prepared_by_line_code`.`female_be_bid_first`) AS `fp_be_bid`,
            COALESCE(`material_prepared`.`male_be_bid`, `material_prepared_by_bebid`.`male_be_bid_first`, `material_prepared_by_pre_line_code`.`male_be_bid_first`, `material_prepared_by_line_code`.`male_be_bid_first`) AS `mp_be_bid`,
            COALESCE(`material_prepared`.`fp_fp_be_bid`,`material_prepared_by_pre_line_code`.`fp_fp_be_bid_first`, `material_prepared_by_line_code`.`fp_fp_be_bid_first`) AS `fp_fp_be_bid`,
            COALESCE(`material_prepared`.`fp_mp_be_bid`,`material_prepared_by_pre_line_code`.`fp_mp_be_bid_first`, `material_prepared_by_line_code`.`fp_mp_be_bid_first`) AS `fp_mp_be_bid`,
            COALESCE(`material_prepared`.`mp_fp_be_bid`,`material_prepared_by_pre_line_code`.`mp_fp_be_bid_first`, `material_prepared_by_line_code`.`mp_fp_be_bid_first`) AS `mp_fp_be_bid`,
            COALESCE(`material_prepared`.`mp_mp_be_bid`,`material_prepared_by_pre_line_code`.`mp_mp_be_bid_first`, `material_prepared_by_line_code`.`mp_mp_be_bid_first`) AS `mp_mp_be_bid`,
            GREATEST(`material_prepared`.`check_line_to_beat_f`, `material_prepared_by_bebid`.`check_line_to_beat_f_max`, `material_prepared_by_pre_line_code`.`check_line_to_beat_f_max`, `material_prepared_by_line_code`.`check_line_to_beat_f_max`) AS `check_line_to_beat_f`,
            GREATEST(`material_prepared`.`check_line_to_beat_m`, `material_prepared_by_bebid`.`check_line_to_beat_m_max`, `material_prepared_by_pre_line_code`.`check_line_to_beat_m_max`, `material_prepared_by_line_code`.`check_line_to_beat_m_max`) AS `check_line_to_beat_m`,
            `material_prepared`.`cgenes` AS `cgenes`
          FROM `material_info_sp` `material_prepared`
          LEFT JOIN `material_prepared_by_pre_line_code` `material_prepared_by_pre_line_code`
            ON `material_prepared`.`pre_line_code` = `material_prepared_by_pre_line_code`.`pre_line_code`
          LEFT JOIN `material_prepared_by_bebid` `material_prepared_by_bebid`
            ON `material_prepared`.`be_bid` = `material_prepared_by_bebid`.`be_bid`
          LEFT JOIN `material_prepared_by_line_code` `material_prepared_by_line_code`
            ON `material_prepared`.`line_guid` = `material_prepared_by_line_code`.`line_guid_first`
    """)

    return output_df


def compute_material_prepared_by_line_code(spark):
    output_df = spark.sql("""
        SELECT 
            `__topn_btmm`.`pre_line_code` AS `pre_line_code`,
            `__topn_btmm`.`cpi_max` AS `cpi_max`,
            `__topn_btmm`.`check_line_to_beat_f_max` AS `check_line_to_beat_f_max`,
            `__topn_btmm`.`check_line_to_beat_m_max` AS `check_line_to_beat_m_max`,
            `__topn_btmm`.`be_bid_first` AS `be_bid_first`,
            `__topn_btmm`.`lbg_bid_first` AS `lbg_bid_first`,
            `__topn_btmm`.`female_be_bid_first` AS `female_be_bid_first`,
            `__topn_btmm`.`male_be_bid_first` AS `male_be_bid_first`,
            `__topn_btmm`.`mp_mp_be_bid_first` AS `mp_mp_be_bid_first`,
            `__topn_btmm`.`mp_fp_be_bid_first` AS `mp_fp_be_bid_first`,
            `__topn_btmm`.`fp_mp_be_bid_first` AS `fp_mp_be_bid_first`,
            `__topn_btmm`.`fp_fp_be_bid_first` AS `fp_fp_be_bid_first`,
            `__topn_btmm`.`line_guid_first` AS `line_guid_first`,
            `__topn_btmm`.`create_year_min` AS `create_year_min`,
            `__topn_btmm`.`create_year_max` AS `create_year_max`,
            `__topn_btmm`.`region_lid_first` AS `region_lid_first`,
            `__topn_btmm`.`gas_pedigree_stem_first` AS `gas_pedigree_stem_first`,
            `__topn_btmm`.`family_first` AS `family_first`,
            `__topn_btmm`.`line_code_first` AS `line_code_first`,
            `__topn_btmm`.`chassis_first` AS `chassis_first`,
            `__topn_btmm`.`chassis_year_min` AS `chassis_year_min`,
            `__topn_btmm`.`line_create_year_min` AS `line_create_year_min`,
            `__topn_btmm`.`primary_genetic_family_lid_first` AS `primary_genetic_family_lid_first`,
            `__topn_btmm`.`secondary_genetic_family_lid_first` AS `secondary_genetic_family_lid_first`,
            `__topn_btmm`.`count` AS `count`
          FROM (
            SELECT 
                `__origin_table`.`pre_line_code` AS `pre_line_code`,
                `__origin_table`.`cpi_max` AS `cpi_max`,
                `__origin_table`.`check_line_to_beat_f_max` AS `check_line_to_beat_f_max`,
                `__origin_table`.`check_line_to_beat_m_max` AS `check_line_to_beat_m_max`,
                `__origin_table`.`be_bid_distinct` AS `be_bid_distinct`,
                `__origin_table`.`be_bid_first` AS `be_bid_first`,
                `__origin_table`.`lbg_bid_distinct` AS `lbg_bid_distinct`,
                `__origin_table`.`lbg_bid_first` AS `lbg_bid_first`,
                `__origin_table`.`female_be_bid_first` AS `female_be_bid_first`,
                `__origin_table`.`male_be_bid_first` AS `male_be_bid_first`,
                `__origin_table`.`mp_mp_be_bid_distinct` AS `mp_mp_be_bid_distinct`,
                `__origin_table`.`mp_mp_be_bid_first` AS `mp_mp_be_bid_first`,
                `__origin_table`.`mp_fp_be_bid_distinct` AS `mp_fp_be_bid_distinct`,
                `__origin_table`.`mp_fp_be_bid_first` AS `mp_fp_be_bid_first`,
                `__origin_table`.`fp_mp_be_bid_distinct` AS `fp_mp_be_bid_distinct`,
                `__origin_table`.`fp_mp_be_bid_first` AS `fp_mp_be_bid_first`,
                `__origin_table`.`fp_fp_be_bid_distinct` AS `fp_fp_be_bid_distinct`,
                `__origin_table`.`fp_fp_be_bid_first` AS `fp_fp_be_bid_first`,
                `__origin_table`.`line_guid_distinct` AS `line_guid_distinct`,
                `__origin_table`.`line_guid_first` AS `line_guid_first`,
                `__origin_table`.`create_year_min` AS `create_year_min`,
                `__origin_table`.`create_year_max` AS `create_year_max`,
                `__origin_table`.`region_lid_distinct` AS `region_lid_distinct`,
                `__origin_table`.`region_lid_first` AS `region_lid_first`,
                `__origin_table`.`gas_pedigree_stem_distinct` AS `gas_pedigree_stem_distinct`,
                `__origin_table`.`gas_pedigree_stem_first` AS `gas_pedigree_stem_first`,
                `__origin_table`.`family_first` AS `family_first`,
                `__origin_table`.`line_code_distinct` AS `line_code_distinct`,
                `__origin_table`.`line_code_first` AS `line_code_first`,
                `__origin_table`.`chassis_distinct` AS `chassis_distinct`,
                `__origin_table`.`chassis_first` AS `chassis_first`,
                `__origin_table`.`chassis_year_min` AS `chassis_year_min`,
                `__origin_table`.`chassis_year_distinct` AS `chassis_year_distinct`,
                `__origin_table`.`line_create_year_min` AS `line_create_year_min`,
                `__origin_table`.`line_create_year_distinct` AS `line_create_year_distinct`,
                `__origin_table`.`primary_genetic_family_lid_distinct` AS `primary_genetic_family_lid_distinct`,
                `__origin_table`.`primary_genetic_family_lid_first` AS `primary_genetic_family_lid_first`,
                `__origin_table`.`secondary_genetic_family_lid_distinct` AS `secondary_genetic_family_lid_distinct`,
                `__origin_table`.`secondary_genetic_family_lid_first` AS `secondary_genetic_family_lid_first`,
                `__origin_table`.`count` AS `count`,
                ROW_NUMBER() OVER (PARTITION BY `__origin_table`.`line_guid_first` ORDER BY `__origin_table`.`count` DESC) AS `_row_number`
              FROM (
                SELECT *
                  FROM `material_prepared_by_pre_line_code` `__origin_table`
                  WHERE (`line_guid_first` != '' OR `line_guid_first` IS NULL AND '' IS NOT NULL OR `line_guid_first` IS NOT NULL AND '' IS NULL) AND `line_guid_first` IS NOT NULL
                ) `__origin_table`
            ) `__topn_btmm`
          WHERE `_row_number` <= (1)
    """)
    return output_df


def compute_material_prepared_by_bebid(spark):
    output_df = spark.sql("""
        SELECT 
            `be_bid` AS `be_bid`,
            COUNT(DISTINCT `pre_line_code`) AS `pre_line_code_distinct`,
            MIN(`pre_line_code_dku_fst`) AS `pre_line_code_first`,
            MIN(`cgenes_dku_fst`) AS `cgenes_first`,
            MIN(`pollination_type_lid_dku_fst`) AS `pollination_type_lid_first`,
            MAX(`cpi`) AS `cpi_max`,
            MAX(`check_line_to_beat_f`) AS `check_line_to_beat_f_max`,
            MAX(`check_line_to_beat_m`) AS `check_line_to_beat_m_max`,
            MIN(`lbg_bid_dku_fst`) AS `lbg_bid_first`,
            MIN(`female_be_bid_dku_fst`) AS `female_be_bid_first`,
            MIN(`male_be_bid_dku_fst`) AS `male_be_bid_first`,
            MIN(`mp_mp_be_bid_dku_fst`) AS `mp_mp_be_bid_first`,
            MIN(`mp_fp_be_bid_dku_fst`) AS `mp_fp_be_bid_first`,
            MIN(`fp_mp_be_bid_dku_fst`) AS `fp_mp_be_bid_first`,
            MIN(`fp_fp_be_bid_dku_fst`) AS `fp_fp_be_bid_first`,
            COUNT(DISTINCT `line_guid`) AS `line_guid_distinct`,
            MIN(`line_guid_dku_fst`) AS `line_guid_first`,
            MIN(`create_year`) AS `create_year_min`,
            MAX(`create_year`) AS `create_year_max`,
            COUNT(DISTINCT `region_lid`) AS `region_lid_distinct`,
            MIN(`region_lid_dku_fst`) AS `region_lid_first`,
            COUNT(DISTINCT `gas_pedigree_stem`) AS `gas_pedigree_stem_distinct`,
            MIN(`gas_pedigree_stem_dku_fst`) AS `gas_pedigree_stem_first`,
            MIN(`family_dku_fst`) AS `family_first`,
            COUNT(DISTINCT `line_code`) AS `line_code_distinct`,
            MIN(`line_code_dku_fst`) AS `line_code_first`,
            COUNT(DISTINCT `chassis`) AS `chassis_distinct`,
            MIN(`chassis_dku_fst`) AS `chassis_first`,
            MIN(`chassis_year`) AS `chassis_year_min`,
            COUNT(DISTINCT `chassis_year`) AS `chassis_year_distinct`,
            MIN(`line_create_year`) AS `line_create_year_min`,
            COUNT(DISTINCT `line_create_year`) AS `line_create_year_distinct`,
            COUNT(DISTINCT `primary_genetic_family_lid`) AS `primary_genetic_family_lid_distinct`,
            MIN(`primary_genetic_family_lid_dku_fst`) AS `primary_genetic_family_lid_first`,
            COUNT(DISTINCT `secondary_genetic_family_lid`) AS `secondary_genetic_family_lid_distinct`,
            MIN(`secondary_genetic_family_lid_dku_fst`) AS `secondary_genetic_family_lid_first`,
            COUNT(*) AS `count`
          FROM (
            SELECT 
                `material_guid`,
                `material_id`,
                `pre_line_code`,
                `abbr_code`,
                `highname`,
                `generation_code`,
                `gmo_lid`,
                `cgenes`,
                `pedigree`,
                `pollination_type_lid`,
                `material_type_lid`,
                `cpi`,
                `check_line_to_beat_f`,
                `check_line_to_beat_m`,
                `be_bid`,
                `lbg_bid`,
                `female_be_bid`,
                `male_be_bid`,
                `mp_mp_be_bid`,
                `mp_fp_be_bid`,
                `fp_mp_be_bid`,
                `fp_fp_be_bid`,
                `line_guid`,
                `create_year`,
                `region_lid`,
                `stack_guid`,
                `gas_pedigree_stem`,
                `family`,
                `line_code`,
                `chassis`,
                `chassis_year`,
                `line_create_year`,
                `primary_genetic_family_lid`,
                `secondary_genetic_family_lid`,
                FIRST_VALUE(`pre_line_code`, true) OVER (PARTITION BY `be_bid` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `pre_line_code_dku_fst`,
                FIRST_VALUE(`cgenes`, true) OVER (PARTITION BY `be_bid` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `cgenes_dku_fst`,
                FIRST_VALUE(`pollination_type_lid`, true) OVER (PARTITION BY `be_bid` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `pollination_type_lid_dku_fst`,
                FIRST_VALUE(`lbg_bid`, true) OVER (PARTITION BY `be_bid` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `lbg_bid_dku_fst`,
                FIRST_VALUE(`female_be_bid`, true) OVER (PARTITION BY `be_bid` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `female_be_bid_dku_fst`,
                FIRST_VALUE(`male_be_bid`, true) OVER (PARTITION BY `be_bid` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `male_be_bid_dku_fst`,
                FIRST_VALUE(`mp_mp_be_bid`, true) OVER (PARTITION BY `be_bid` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `mp_mp_be_bid_dku_fst`,
                FIRST_VALUE(`mp_fp_be_bid`, true) OVER (PARTITION BY `be_bid` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `mp_fp_be_bid_dku_fst`,
                FIRST_VALUE(`fp_mp_be_bid`, true) OVER (PARTITION BY `be_bid` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `fp_mp_be_bid_dku_fst`,
                FIRST_VALUE(`fp_fp_be_bid`, true) OVER (PARTITION BY `be_bid` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `fp_fp_be_bid_dku_fst`,
                FIRST_VALUE(`line_guid`, true) OVER (PARTITION BY `be_bid` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `line_guid_dku_fst`,
                FIRST_VALUE(`region_lid`, true) OVER (PARTITION BY `be_bid` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `region_lid_dku_fst`,
                FIRST_VALUE(`gas_pedigree_stem`, true) OVER (PARTITION BY `be_bid` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `gas_pedigree_stem_dku_fst`,
                FIRST_VALUE(`family`, true) OVER (PARTITION BY `be_bid` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `family_dku_fst`,
                FIRST_VALUE(`line_code`, true) OVER (PARTITION BY `be_bid` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `line_code_dku_fst`,
                FIRST_VALUE(`chassis`, true) OVER (PARTITION BY `be_bid` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `chassis_dku_fst`,
                FIRST_VALUE(`primary_genetic_family_lid`, true) OVER (PARTITION BY `be_bid` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `primary_genetic_family_lid_dku_fst`,
                FIRST_VALUE(`secondary_genetic_family_lid`, true) OVER (PARTITION BY `be_bid` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `secondary_genetic_family_lid_dku_fst`
              FROM `material_info_sp`
              WHERE (`be_bid` != '' OR `be_bid` IS NULL AND '' IS NOT NULL OR `be_bid` IS NOT NULL AND '' IS NULL) AND `be_bid` IS NOT NULL
            ) `dku__beforegrouping`
          GROUP BY `be_bid`
    """)

    return output_df


def compute_material_prepared_by_pre_line_code(spark):
    output_df = spark.sql("""
        SELECT 
            `pre_line_code` AS `pre_line_code`,
            MAX(`cpi`) AS `cpi_max`,
            MAX(`check_line_to_beat_f`) AS `check_line_to_beat_f_max`,
            MAX(`check_line_to_beat_m`) AS `check_line_to_beat_m_max`,
            COUNT(DISTINCT `be_bid`) AS `be_bid_distinct`,
            MIN(`be_bid_dku_fst`) AS `be_bid_first`,
            COUNT(DISTINCT `lbg_bid`) AS `lbg_bid_distinct`,
            MIN(`lbg_bid_dku_fst`) AS `lbg_bid_first`,
            MIN(`female_be_bid_dku_fst`) AS `female_be_bid_first`,
            MIN(`male_be_bid_dku_fst`) AS `male_be_bid_first`,
            COUNT(DISTINCT `mp_mp_be_bid`) AS `mp_mp_be_bid_distinct`,
            MIN(`mp_mp_be_bid_dku_fst`) AS `mp_mp_be_bid_first`,
            COUNT(DISTINCT `mp_fp_be_bid`) AS `mp_fp_be_bid_distinct`,
            MIN(`mp_fp_be_bid_dku_fst`) AS `mp_fp_be_bid_first`,
            COUNT(DISTINCT `fp_mp_be_bid`) AS `fp_mp_be_bid_distinct`,
            MIN(`fp_mp_be_bid_dku_fst`) AS `fp_mp_be_bid_first`,
            COUNT(DISTINCT `fp_fp_be_bid`) AS `fp_fp_be_bid_distinct`,
            MIN(`fp_fp_be_bid_dku_fst`) AS `fp_fp_be_bid_first`,
            COUNT(DISTINCT `line_guid`) AS `line_guid_distinct`,
            MIN(`line_guid_dku_fst`) AS `line_guid_first`,
            MIN(`create_year`) AS `create_year_min`,
            MAX(`create_year`) AS `create_year_max`,
            COUNT(DISTINCT `region_lid`) AS `region_lid_distinct`,
            MIN(`region_lid_dku_fst`) AS `region_lid_first`,
            COUNT(DISTINCT `gas_pedigree_stem`) AS `gas_pedigree_stem_distinct`,
            MIN(`gas_pedigree_stem_dku_fst`) AS `gas_pedigree_stem_first`,
            MIN(`family_dku_fst`) AS `family_first`,
            COUNT(DISTINCT `line_code`) AS `line_code_distinct`,
            MIN(`line_code_dku_fst`) AS `line_code_first`,
            COUNT(DISTINCT `chassis`) AS `chassis_distinct`,
            MIN(`chassis_dku_fst`) AS `chassis_first`,
            MIN(`chassis_year`) AS `chassis_year_min`,
            COUNT(DISTINCT `chassis_year`) AS `chassis_year_distinct`,
            MIN(`line_create_year`) AS `line_create_year_min`,
            COUNT(DISTINCT `line_create_year`) AS `line_create_year_distinct`,
            COUNT(DISTINCT `primary_genetic_family_lid`) AS `primary_genetic_family_lid_distinct`,
            MIN(`primary_genetic_family_lid_dku_fst`) AS `primary_genetic_family_lid_first`,
            COUNT(DISTINCT `secondary_genetic_family_lid`) AS `secondary_genetic_family_lid_distinct`,
            MIN(`secondary_genetic_family_lid_dku_fst`) AS `secondary_genetic_family_lid_first`,
            COUNT(*) AS `count`
          FROM (
            SELECT 
                `material_guid`,
                `material_id`,
                `pre_line_code`,
                `abbr_code`,
                `highname`,
                `generation_code`,
                `gmo_lid`,
                `cgenes`,
                `pedigree`,
                `pollination_type_lid`,
                `material_type_lid`,
                `cpi`,
                `check_line_to_beat_f`,
                `check_line_to_beat_m`,
                `be_bid`,
                `lbg_bid`,
                `female_be_bid`,
                `male_be_bid`,
                `mp_mp_be_bid`,
                `mp_fp_be_bid`,
                `fp_mp_be_bid`,
                `fp_fp_be_bid`,
                `line_guid`,
                `create_year`,
                `region_lid`,
                `stack_guid`,
                `gas_pedigree_stem`,
                `family`,
                `line_code`,
                `chassis`,
                `chassis_year`,
                `line_create_year`,
                `primary_genetic_family_lid`,
                `secondary_genetic_family_lid`,
                FIRST_VALUE(`be_bid`) OVER (PARTITION BY `pre_line_code` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `be_bid_dku_fst`,
                FIRST_VALUE(`lbg_bid`, true) OVER (PARTITION BY `pre_line_code` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `lbg_bid_dku_fst`,
                FIRST_VALUE(`female_be_bid`, true) OVER (PARTITION BY `pre_line_code` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `female_be_bid_dku_fst`,
                FIRST_VALUE(`male_be_bid`, true) OVER (PARTITION BY `pre_line_code` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `male_be_bid_dku_fst`,
                FIRST_VALUE(`mp_mp_be_bid`, true) OVER (PARTITION BY `pre_line_code` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `mp_mp_be_bid_dku_fst`,
                FIRST_VALUE(`mp_fp_be_bid`, true) OVER (PARTITION BY `pre_line_code` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `mp_fp_be_bid_dku_fst`,
                FIRST_VALUE(`fp_mp_be_bid`, true) OVER (PARTITION BY `pre_line_code` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `fp_mp_be_bid_dku_fst`,
                FIRST_VALUE(`fp_fp_be_bid`, true) OVER (PARTITION BY `pre_line_code` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `fp_fp_be_bid_dku_fst`,
                FIRST_VALUE(`line_guid`) OVER (PARTITION BY `pre_line_code` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `line_guid_dku_fst`,
                FIRST_VALUE(`region_lid`, true) OVER (PARTITION BY `pre_line_code` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `region_lid_dku_fst`,
                FIRST_VALUE(`gas_pedigree_stem`) OVER (PARTITION BY `pre_line_code` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `gas_pedigree_stem_dku_fst`,
                FIRST_VALUE(`family`) OVER (PARTITION BY `pre_line_code` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `family_dku_fst`,
                FIRST_VALUE(`line_code`, true) OVER (PARTITION BY `pre_line_code` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `line_code_dku_fst`,
                FIRST_VALUE(`chassis`) OVER (PARTITION BY `pre_line_code` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `chassis_dku_fst`,
                FIRST_VALUE(`primary_genetic_family_lid`) OVER (PARTITION BY `pre_line_code` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `primary_genetic_family_lid_dku_fst`,
                FIRST_VALUE(`secondary_genetic_family_lid`) OVER (PARTITION BY `pre_line_code` ORDER BY `material_guid` ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `secondary_genetic_family_lid_dku_fst`
              FROM `material_info_sp`
              WHERE (`pre_line_code` != '' OR `pre_line_code` IS NULL AND '' IS NOT NULL OR `pre_line_code` IS NOT NULL AND '' IS NULL) AND `pre_line_code` IS NOT NULL
            ) `dku__beforegrouping`
          GROUP BY `pre_line_code`
    """)
    return output_df


def compute_fada_groups_joined(spark):
    output_df = spark.sql("""
    SELECT *
          FROM (
            SELECT 
                `material_guid` AS `material_guid`,
                `line_guid` AS `line_guid`,
                `material_id` AS `material_id`,
                `family` AS `family`,
                `pre_line_code` AS `pre_line_code`,
                `line_code` AS `line_code`,
                `chassis` AS `chassis`,
                `be_bid` AS `be_bid`,
                `lbg_bid` AS `lbg_bid`,
                `matguid_material_id` AS `matguid_material_id`,
                `matguid_membership_pct` AS `matguid_membership_pct`,
                `matguid_genetic_group_name` AS `matguid_genetic_group_name`,
                `matguid_ss_nss` AS `matguid_ss_nss`,
                `matguid_base_pool` AS `matguid_base_pool`,
                `matguid_base_pool_rank` AS `matguid_base_pool_rank`,
                `matid_membership_pct` AS `matid_membership_pct`,
                `matid_genetic_group_name` AS `matid_genetic_group_name`,
                `matid_ss_nss` AS `matid_ss_nss`,
                `matid_base_pool` AS `matid_base_pool`,
                `matid_base_pool_rank` AS `matid_base_pool_rank`,
                `precd_membership_pct` AS `precd_membership_pct`,
                `precd_genetic_group_name` AS `precd_genetic_group_name`,
                `precd_ss_nss` AS `precd_ss_nss`,
                `precd_base_pool` AS `precd_base_pool`,
                `precd_base_pool_rank` AS `precd_base_pool_rank`,
                `linguid_material_id` AS `linguid_material_id`,
                `linguid_membership_pct` AS `linguid_membership_pct`,
                `linguid_genetic_group_name` AS `linguid_genetic_group_name`,
                `linguid_ss_nss` AS `linguid_ss_nss`,
                `linguid_base_pool` AS `linguid_base_pool`,
                `linguid_base_pool_rank` AS `linguid_base_pool_rank`,
                `lincd_membership_pct` AS `lincd_membership_pct`,
                `lincd_genetic_group_name` AS `lincd_genetic_group_name`,
                `lincd_ss_nss` AS `lincd_ss_nss`,
                `lincd_base_pool` AS `lincd_base_pool`,
                `lincd_base_pool_rank` AS `lincd_base_pool_rank`,
                `cha_membership_pct` AS `cha_membership_pct`,
                `cha_genetic_group_name` AS `cha_genetic_group_name`,
                `cha_ss_nss` AS `cha_ss_nss`,
                `cha_base_pool` AS `cha_base_pool`,
                `cha_base_pool_rank` AS `cha_base_pool_rank`,
                COALESCE(`linguid_membership_pct`, `lincd_membership_pct`, 
                 `matguid_membership_pct`, `matid_membership_pct`,
                 `precd_membership_pct`, `cha_membership_pct`) AS `membership_pct`,
                COALESCE(`linguid_ss_nss`, `lincd_ss_nss`, `matguid_ss_nss`, 
                 `matid_ss_nss`, `precd_ss_nss`, `cha_ss_nss`) AS `ss_nss`,
                IF(`matguid_ss_nss` = 'ss' OR `matid_ss_nss` = 'ss', 1, 0) +
        IF(`precd_ss_nss` = 'ss', 1, 0) +
        IF(`linguid_ss_nss` = 'ss' OR `lincd_ss_nss` = 'ss', 1, 0) +
        IF(`cha_ss_nss` = 'ss', 0.5, 0) AS `is_ss_sum`,
                IF(`matguid_ss_nss` = 'nss' OR `matid_ss_nss` = 'nss', 1, 0) +
        IF(`precd_ss_nss` = 'nss', 1, 0) +
        IF(`linguid_ss_nss` = 'nss' OR `lincd_ss_nss` = 'nss', 1, 0) +
        IF(`cha_ss_nss` = 'nss', 0.5, 0) AS `is_nss_sum`,
                COALESCE(`linguid_genetic_group_name`, `lincd_genetic_group_name`, `matguid_genetic_group_name`, 
                 `matid_genetic_group_name`, `precd_genetic_group_name`, `cha_genetic_group_name`) AS `genetic_group_name`,
                COALESCE(`linguid_base_pool`, `lincd_base_pool`, `matguid_base_pool`, 
                 `matid_base_pool`, `precd_base_pool`, `cha_base_pool`) AS `base_pool`,
                COALESCE(`linguid_base_pool_rank`, `lincd_base_pool_rank`, `matguid_base_pool_rank`, 
                 `matid_base_pool_rank`, `precd_base_pool_rank`, `cha_base_pool_rank`) AS `base_pool_rank`,
                CASE
            WHEN `linguid_genetic_group_name` IS NOT NULL THEN 'linguid'
            WHEN `lincd_genetic_group_name` IS NOT NULL THEN 'lincd'
            WHEN `matguid_genetic_group_name` IS NOT NULL THEN 'matguid'
            WHEN `matid_genetic_group_name` IS NOT NULL THEN 'matid'
            WHEN `precd_genetic_group_name` IS NOT NULL THEN 'precd'
            WHEN `cha_genetic_group_name` IS NOT NULL THEN 'cha'
            ELSE 'none'
        END AS `fada_classif_source`
              FROM (
                SELECT 
                    `material_prepared_joined`.`material_guid` AS `material_guid`,
                    `material_prepared_joined`.`line_guid` AS `line_guid`,
                    `material_prepared_joined`.`material_id` AS `material_id`,
                    `material_prepared_joined`.`family` AS `family`,
                    `material_prepared_joined`.`pre_line_code` AS `pre_line_code`,
                    `material_prepared_joined`.`line_code` AS `line_code`,
                    `material_prepared_joined`.`chassis` AS `chassis`,
                    `material_prepared_joined`.`be_bid` AS `be_bid`,
                    `material_prepared_joined`.`lbg_bid` AS `lbg_bid`,
                    `fada_groups`.`material_id` AS `matguid_material_id`,
                    `fada_groups`.`membership_pct` AS `matguid_membership_pct`,
                    `fada_groups`.`genetic_group_name` AS `matguid_genetic_group_name`,
                    `fada_groups`.`ss_nss` AS `matguid_ss_nss`,
                    `fada_groups`.`base_pool` AS `matguid_base_pool`,
                    `fada_groups`.`base_pool_rank` AS `matguid_base_pool_rank`,
                    `fada_groups_by_id`.`membership_pct` AS `matid_membership_pct`,
                    `fada_groups_by_id`.`genetic_group_name` AS `matid_genetic_group_name`,
                    `fada_groups_by_id`.`ss_nss` AS `matid_ss_nss`,
                    `fada_groups_by_id`.`base_pool` AS `matid_base_pool`,
                    `fada_groups_by_id`.`base_pool_rank` AS `matid_base_pool_rank`,
                    `fada_groups_by_id_2`.`membership_pct` AS `precd_membership_pct`,
                    `fada_groups_by_id_2`.`genetic_group_name` AS `precd_genetic_group_name`,
                    `fada_groups_by_id_2`.`ss_nss` AS `precd_ss_nss`,
                    `fada_groups_by_id_2`.`base_pool` AS `precd_base_pool`,
                    `fada_groups_by_id_2`.`base_pool_rank` AS `precd_base_pool_rank`,
                    `fada_groups_2`.`material_id` AS `linguid_material_id`,
                    `fada_groups_2`.`membership_pct` AS `linguid_membership_pct`,
                    `fada_groups_2`.`genetic_group_name` AS `linguid_genetic_group_name`,
                    `fada_groups_2`.`ss_nss` AS `linguid_ss_nss`,
                    `fada_groups_2`.`base_pool` AS `linguid_base_pool`,
                    `fada_groups_2`.`base_pool_rank` AS `linguid_base_pool_rank`,
                    `fada_groups_by_id_3`.`membership_pct` AS `lincd_membership_pct`,
                    `fada_groups_by_id_3`.`genetic_group_name` AS `lincd_genetic_group_name`,
                    `fada_groups_by_id_3`.`ss_nss` AS `lincd_ss_nss`,
                    `fada_groups_by_id_3`.`base_pool` AS `lincd_base_pool`,
                    `fada_groups_by_id_3`.`base_pool_rank` AS `lincd_base_pool_rank`,
                    `fada_groups_by_id_4`.`membership_pct` AS `cha_membership_pct`,
                    `fada_groups_by_id_4`.`genetic_group_name` AS `cha_genetic_group_name`,
                    `fada_groups_by_id_4`.`ss_nss` AS `cha_ss_nss`,
                    `fada_groups_by_id_4`.`base_pool` AS `cha_base_pool`,
                    `fada_groups_by_id_4`.`base_pool_rank` AS `cha_base_pool_rank`
                  FROM `material_prepared_joined` `material_prepared_joined`
                  LEFT JOIN (
                    SELECT `fada_groups`.*
                      FROM `fada_groups` `fada_groups`
                      WHERE LOWER(`source_type`) = LOWER('batch') OR LOWER(`source_type`) IS NULL AND LOWER('batch') IS NULL
                    ) `fada_groups`
                    ON `material_prepared_joined`.`material_guid` = `fada_groups`.`material_guid`
                  LEFT JOIN (
                    SELECT `fada_groups_by_id`.*
                      FROM `fada_groups_by_id` `fada_groups_by_id`
                      WHERE LOWER(`source_type`) = LOWER('batch') OR LOWER(`source_type`) IS NULL AND LOWER('batch') IS NULL
                    ) `fada_groups_by_id`
                    ON `material_prepared_joined`.`material_id` = `fada_groups_by_id`.`fada_id`
                  LEFT JOIN `fada_groups_by_id` `fada_groups_by_id_2`
                    ON `material_prepared_joined`.`pre_line_code` = `fada_groups_by_id_2`.`fada_id`
                  LEFT JOIN (
                    SELECT `fada_groups_2`.*
                      FROM `fada_groups` `fada_groups_2`
                      WHERE LOWER(`source_type`) = LOWER('pbm') OR LOWER(`source_type`) IS NULL AND LOWER('pbm') IS NULL
                    ) `fada_groups_2`
                    ON `material_prepared_joined`.`line_guid` = `fada_groups_2`.`material_guid`
                  LEFT JOIN (
                    SELECT `fada_groups_by_id_3`.*
                      FROM `fada_groups_by_id` `fada_groups_by_id_3`
                      WHERE `source_type` LIKE CONCAT('%', REGEXP_REPLACE('pbm', '([%_])', '\\\\$1'), '%')
                    ) `fada_groups_by_id_3`
                    ON `material_prepared_joined`.`line_code` = `fada_groups_by_id_3`.`fada_id`
                  LEFT JOIN (
                    SELECT `fada_groups_by_id_4`.*
                      FROM `fada_groups_by_id` `fada_groups_by_id_4`
                      WHERE `source_type` LIKE CONCAT('%', REGEXP_REPLACE('pbm', '([%_])', '\\\\$1'), '%')
                    ) `fada_groups_by_id_4`
                    ON `material_prepared_joined`.`chassis` = `fada_groups_by_id_4`.`fada_id`
                ) `withoutcomputedcols_query`
            ) `unfiltered_query`
          WHERE (`matguid_ss_nss` != '' OR `matguid_ss_nss` IS NULL AND '' IS NOT NULL OR `matguid_ss_nss` IS NOT NULL AND '' IS NULL) AND `matguid_ss_nss` IS NOT NULL OR (`matid_ss_nss` != '' OR `matid_ss_nss` IS NULL AND '' IS NOT NULL OR `matid_ss_nss` IS NOT NULL AND '' IS NULL) AND `matid_ss_nss` IS NOT NULL OR (`precd_ss_nss` != '' OR `precd_ss_nss` IS NULL AND '' IS NOT NULL OR `precd_ss_nss` IS NOT NULL AND '' IS NULL) AND `precd_ss_nss` IS NOT NULL OR (`linguid_ss_nss` != '' OR `linguid_ss_nss` IS NULL AND '' IS NOT NULL OR `linguid_ss_nss` IS NOT NULL AND '' IS NULL) AND `linguid_ss_nss` IS NOT NULL OR (`lincd_ss_nss` != '' OR `lincd_ss_nss` IS NULL AND '' IS NOT NULL OR `lincd_ss_nss` IS NOT NULL AND '' IS NULL) AND `lincd_ss_nss` IS NOT NULL OR (`cha_ss_nss` != '' OR `cha_ss_nss` IS NULL AND '' IS NOT NULL OR `cha_ss_nss` IS NOT NULL AND '' IS NULL) AND `cha_ss_nss` IS NOT NULL
    """)

    return output_df


def compute_fada_groups_by_id(spark):
    output_df = spark.sql("""
        SELECT
            `fada_id`,
            `source_type`,
            MAX(`membership_pct`) AS `membership_pct`,
            `genetic_group_name`,
            `ss_nss`,
            `base_pool`,
            `base_pool_rank`
        FROM(
            SELECT DISTINCT
                `material_id` AS `fada_id`,
                `source_type`,
                `membership_pct`,
                `genetic_group_name`,
                `ss_nss`,
                `base_pool`,
                `base_pool_rank`
            FROM `fada_groups`

            UNION
            SELECT
                `fada_id`,
                `source_type`,
                `membership_pct`,
                `genetic_group_name`,
                `ss_nss`,
                `base_pool`,
                `base_pool_rank`
            FROM (
                SELECT
                    `fada_id`,
                    `source_type`,
                    `membership_pct`,
                    `ss_nss`,
                    `genetic_group_name`,
                    `base_pool`,
                    `base_pool_rank`,
                    ROW_NUMBER() OVER (PARTITION BY `fada_id` ORDER BY `membership_pct` DESC) AS `_row_number`
                FROM (
                    SELECT
                        REGEXP_EXTRACT(`material_id`,'^([A-Z]{3}\\d{4})',1) AS `fada_id`,
                        `source_type`,
                        `membership_pct`,
                        `ss_nss`,
                        `genetic_group_name`,
                        `base_pool`,
                        `base_pool_rank`,
                        COUNT(*) OVER (PARTITION BY REGEXP_EXTRACT(`material_id`,'^([A-Z]{3}\\d{4})',1), `source_type`) AS `group_count`
                    FROM `fada_groups`
                    WHERE REGEXP_EXTRACT(`material_id`,'^([A-Z]{3}\\d{4})',1) != ''
                        and `source_type` = 'PBM'
                ) `chassis_ranked`
            ) `chassis_curated`
            WHERE `_row_number` <= 1
        ) `fg`
        GROUP BY
            `fada_id`,
            `source_type`,
            `genetic_group_name`,
            `ss_nss`,
            `base_pool`,
            `base_pool_rank`
    """)
    return output_df


def compute_fada_groups(spark):
    output_df = spark.sql("""
        SELECT 
            `__topn_btmm`.`material_guid` AS `material_guid`,
            `__topn_btmm`.`material_id` AS `material_id`,
            `__topn_btmm`.`source_type` AS `source_type`,
            LEAST(`__topn_btmm`.`membership_pct`, 100) AS `membership_pct`,
            `__topn_btmm`.`genetic_group_name` AS `genetic_group_name`,
            `__topn_btmm`.`ss_nss` AS `ss_nss`,
            `__topn_btmm`.`base_pool` AS `base_pool`,
            `__topn_btmm`.`base_pool_rank` AS `base_pool_rank`
          FROM (
            SELECT 
                `rv_genetic_groups`.`nbr_spid` AS `material_guid`,
                `rv_genetic_groups`.`nbr_code` AS `material_id`,
                `rv_genetic_groups`.`nbr_type` AS `source_type`,
                float(`rv_genetic_groups`.`membership_percentage`) AS `membership_pct`,
                `rv_genetic_groups`.`genetic_group_name`,
                `fada_ss_nss`.`ss_nss` AS `ss_nss`,
                `fada_ss_nss`.`base_pool` AS `base_pool`,
                `fada_ss_nss`.`base_pool_rank` AS `base_pool_rank`,
                ROW_NUMBER() OVER (PARTITION BY `rv_genetic_groups`.`nbr_spid`, `rv_genetic_groups`.`nbr_code` ORDER BY `rv_genetic_groups`.`membership_percentage` DESC) AS `row_number`
              FROM `rv_v_p_genetic_group_classif_fada` `rv_genetic_groups`
              INNER JOIN `fada_ss_nss` `fada_ss_nss`
                  ON LOWER(`rv_genetic_groups`.`genetic_group_name`) = LOWER(`fada_ss_nss`.`fada_group`)
            ) `__topn_btmm`
          WHERE `row_number` <= (1)
    """)
    return output_df


def compute_geno_sample_info(spark):
    output_df = spark.sql("""
    SELECT 
        `material_prepared_joined`.`material_guid` AS `material_guid`,
        `material_prepared_joined`.`material_id` AS `material_id`,
        `material_prepared_joined`.`pre_line_code` AS `pre_line_code`,
        `material_prepared_joined`.`line_code` AS `line_code`,
        `material_prepared_joined`.`chassis` AS `chassis`,
        `material_prepared_joined`.`be_bid` AS `be_bid`,
        `material_prepared_joined`.`lbg_bid` AS `lbg_bid`,
        `geno_sample_list`.`sample_id` AS `sample_id`,
        `geno_sample_list`.`germplasm_id` AS `germplasm_id`,
        `geno_sample_list`.`genotype_count` AS `genotype_count`
      FROM `material_prepared_joined` `material_prepared_joined`
      INNER JOIN `geno_sample_list` `geno_sample_list`
        ON `material_prepared_joined`.`material_guid` = `geno_sample_list`.`germplasm_guid`
    """)
    return output_df

def compute_fada_by_family(spark):
    output_df = spark.sql("""
    SELECT
        `family`,
        `membership_pct_max`,
        `is_ss_sum_sum`,
        `is_nss_sum_sum`,
        IF(`is_ss_sum_sum` >= 2*`is_nss_sum_sum`, "ss",
                IF(`is_nss_sum_sum` >= 2*`is_ss_sum_sum`,"nss","")) AS `ss_nss`,
        `genetic_group_name`,
        `base_pool`,
        `base_pool_rank`,
        `count`
      FROM(
        SELECT 
            `family` AS `family`,
            MAX(`membership_pct`) AS `membership_pct_max`,
            SUM(`is_ss_sum`) AS `is_ss_sum_sum`,
            SUM(`is_nss_sum`) AS `is_nss_sum_sum`,
            FIRST_VALUE(`genetic_group_name`) AS `genetic_group_name`,
            FIRST_VALUE(`base_pool`) AS `base_pool`,
            FIRST_VALUE(`base_pool_rank`) AS `base_pool_rank`,
            COUNT(*) AS `count`
          FROM `fada_groups_joined`
        WHERE `family` != '' AND `family` IS NOT NULL AND `membership_pct` >= 33
        GROUP BY `family`
        ) `fada_family_stats`
    WHERE IF(`is_ss_sum_sum` >= 2*`is_nss_sum_sum`, "ss",
                IF(`is_nss_sum_sum` >= 2*`is_ss_sum_sum`,"nss","")) IS NOT NULL
    """)
    return output_df

# compute_sample()
# Selects the sample with the most markers for the given aggregation level.
# Returns a spark df
def compute_sample(spark, level = 'be_bid'):
    output_df = spark.sql("""
    SELECT 
        {0},
        sample_id,
        genotype_count
      FROM (
        SELECT 
            {0},
            sample_id,
            genotype_count,
            ROW_NUMBER() OVER (PARTITION BY {0} ORDER BY genotype_count DESC, sample_id ASC) AS row_number
          FROM geno_sample_info 
          WHERE {0} != '' AND {0} IS NOT NULL
        ) topn_btmm
      WHERE row_number <= (1)
    """.format(level))
    return output_df


def compute_fada(spark, level = "be_bid"):
    output_df = spark.sql("""
    SELECT 
        {0},
        membership_pct,
        ss_nss,
        is_ss_sum,
        is_nss_sum,
        genetic_group_name,
        base_pool,
        base_pool_rank
      FROM (
        SELECT 
            {0},
            membership_pct,
            ss_nss,
            is_ss_sum,
            is_nss_sum,
            genetic_group_name,
            base_pool,
            base_pool_rank,
            fada_classif_source,
            ROW_NUMBER() OVER (PARTITION BY {0} ORDER BY membership_pct DESC) AS row_number
          FROM `fada_groups_joined`
          WHERE {0} != '' AND {0} IS NOT NULL
        ) topn_btmm
      WHERE row_number <= (1)
    """.format(level))
    return output_df

def compute_material_by_group(spark, level="be_bid"):
    output_df = spark.sql("""
        SELECT 
            {0},
			FIRST(`be_bid`, true) AS `be_bid2`,
            FIRST(`pre_line_code`, true) AS `pre_line_code2`,
			FIRST(`line_guid`, true) AS `line_guid2`,
            FIRST(`cgenes`, true) AS `cgenes`,
            FIRST(`pollination_type_lid`, true) AS `pollination_type_lid`,
            MAX(`cpi`) AS `cpi_max`,
            MAX(`check_line_to_beat_f`) AS `check_line_to_beat_f_max`,
            MAX(`check_line_to_beat_m`) AS `check_line_to_beat_m_max`,
            FIRST(`lbg_bid`, true) AS `lbg_bid`,
            FIRST(`female_be_bid`, true) AS `female_be_bid`,
            FIRST(`male_be_bid`, true) AS `male_be_bid`,
            FIRST(`mp_mp_be_bid`, true) AS `mp_mp_be_bid`,
            FIRST(`mp_fp_be_bid`, true) AS `mp_fp_be_bid`,
            FIRST(`fp_mp_be_bid`, true) AS `fp_mp_be_bid`,
            FIRST(`fp_fp_be_bid`, true) AS `fp_fp_be_bid`,
            MIN(`create_year`) AS `create_year_min`,
            MAX(`create_year`) AS `create_year_max`,
            FIRST(`region_lid`, true) AS `region_lid`,
            FIRST(`gas_pedigree_stem`, true) AS `gas_pedigree_stem`,
            FIRST(`family`, true) AS `family`,
            FIRST(`line_code`, true) AS `line_code`,
            FIRST(`chassis`, true) AS `chassis`,
            MIN(`chassis_year`) AS `chassis_year_min`,
			MIN(`line_create_year`) AS `line_create_year_min`,
            FIRST(`primary_genetic_family_lid`, true) AS `primary_genetic_family_lid`,
            FIRST(`secondary_genetic_family_lid`, true) AS `secondary_genetic_family_lid`,
            COUNT(*) AS `count`
          FROM `material_info_sp`
        WHERE {0} != '' AND {0} IS NOT NULL
        GROUP BY {0}
    """.format(level))

    return output_df
