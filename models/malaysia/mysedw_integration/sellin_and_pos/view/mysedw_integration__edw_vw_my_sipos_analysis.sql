with edw_vw_os_curr_dim as
( 
  select * from DEV_DNA_CORE.AAGRAW03_WORKSPACE.EDW_VW_MY_CURR_DIM --pending
),
edw_vw_my_pos_sales_fact as
(
  select * from DEV_DNA_CORE.SNENAV01_WORKSPACE.edw_vw_my_pos_sales_fact --matching
),
edw_vw_os_time_dim as
(
  select * from DEV_DNA_CORE.SNENAV01_WORKSPACE.EDW_VW_OS_TIME_DIM --matching
),
edw_vw_my_material_dim as 
(
  select * from DEV_DNA_CORE.NNaras01_workspace.edw_vw_my_material_dim --matching
),
edw_vw_my_customer_dim as 
(
  select * from DEV_DNA_CORE.AAGRAW03_WORKSPACE.EDW_VW_MY_CUSTOMER_DIM --matching
),
edw_vw_my_pos_customer_dim as 
(
  select * from DEV_DNA_CORE.ASING012_WORKSPACE.edw_vw_my_pos_customer_dim --matching
),
edw_vw_os_pos_material_dim as 
(
  select * from DEV_DNA_CORE.ASING012_WORKSPACE.EDW_VW_MY_POS_MATERIAL_DIM --matching
),
edw_vw_my_billing_fact AS
(
  select * from DEV_DNA_CORE.SM05_WORKSPACE.EDW_VW_MY_BILLING_FACT --matching
),
itg_my_pos_cust_mstr AS
(
  select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_MY_POS_CUST_MSTR
),
itg_my_material_dim AS
(
  select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_MY_MATERIAL_DIM
),
itg_my_customer_dim AS
(
  select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_MY_CUSTOMER_DIM
),
itg_my_dstrbtrr_dim AS
(
  select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_MY_DSTRBTRR_DIM
),
final as (
SELECT
  'POS' AS data_src,
  veposf."year" AS jj_year,
  CAST((
    veposf.qrtr
  ) AS VARCHAR) AS jj_qtr,
  CAST((
    veposf.mnth_id
  ) AS VARCHAR) AS jj_mnth_id,
  veposf.mnth_no AS jj_mnth_no,
  veposf.jj_yr_week_no AS jj_year_wk_no,
  veposf.cntry_nm,
  vopcd.cust_cd,
  vopcd.brnch_cd AS cust_brnch_cd,
  vopcd.brnch_nm AS mt_cust_brnch_nm,
  vopcd.dept_cd AS cust_dept_cd,
  vopcd.dept_nm AS mt_cust_dept_nm,
  vopcd.region_cd,
  vopcd.region_nm,
  vopmd.item_cd,
  CAST((
    vopmd.item_nm
  ) AS VARCHAR) AS mt_item_nm,
  CAST((
    UPPER(TRIM(CAST((
      vopcd.sold_to
    ) AS TEXT)))
  ) AS VARCHAR) AS sold_to,
  veocd.sap_cust_nm AS sold_to_nm,
  'MODERN TRADE' AS trade_type,
  imcd.dstrbtr_grp_cd,
  imcd.dstrbtr_grp_nm,
  veocd.sap_state_cd,
  veocd.sap_sls_org,
  veocd.sap_cmp_id,
  veocd.sap_cntry_cd,
  veocd.sap_cntry_nm,
  veocd.sap_addr,
  veocd.sap_region,
  veocd.sap_city,
  veocd.sap_post_cd,
  veocd.sap_chnl_cd,
  veocd.sap_chnl_desc,
  veocd.sap_sls_office_cd,
  veocd.sap_sls_office_desc,
  veocd.sap_sls_grp_cd,
  veocd.sap_sls_grp_desc,
  veocd.sap_curr_cd,
  veocd.gch_region,
  veocd.gch_cluster,
  veocd.gch_subcluster,
  veocd.gch_market,
  veocd.gch_retail_banner,
  CAST((
    LTRIM(CAST((
      veomd.sap_matl_num
    ) AS TEXT), CAST((
      CAST('0' AS VARCHAR)
    ) AS TEXT))
  ) AS VARCHAR) AS sku,
  immd.frnchse_desc,
  immd.brnd_desc,
  immd.vrnt_desc,
  immd.putup_desc,
  immd.item_desc2,
  veomd.sap_mat_desc AS sku_desc,
  veomd.sap_mat_type_cd,
  veomd.sap_mat_type_desc,
  veomd.sap_base_uom_cd,
  veomd.sap_prchse_uom_cd,
  veomd.sap_prod_sgmt_cd,
  veomd.sap_prod_sgmt_desc,
  veomd.sap_base_prod_cd,
  veomd.sap_base_prod_desc,
  veomd.sap_mega_brnd_cd,
  veomd.sap_mega_brnd_desc,
  veomd.sap_brnd_cd,
  veomd.sap_brnd_desc,
  veomd.sap_vrnt_cd,
  veomd.sap_vrnt_desc,
  veomd.sap_put_up_cd,
  veomd.sap_put_up_desc,
  veomd.sap_grp_frnchse_cd,
  veomd.sap_grp_frnchse_desc,
  veomd.sap_frnchse_cd,
  veomd.sap_frnchse_desc,
  veomd.sap_prod_frnchse_cd,
  veomd.sap_prod_frnchse_desc,
  veomd.sap_prod_mjr_cd,
  veomd.sap_prod_mjr_desc,
  veomd.sap_prod_mnr_cd,
  veomd.sap_prod_mnr_desc,
  veomd.sap_prod_hier_cd,
  veomd.sap_prod_hier_desc,
  veomd.gph_region AS global_mat_region,
  veomd.gph_prod_frnchse AS global_prod_franchise,
  veomd.gph_prod_brnd AS global_prod_brand,
  veomd.gph_prod_vrnt AS global_prod_variant,
  veomd.gph_prod_put_up_cd AS global_prod_put_up_cd,
  veomd.gph_prod_put_up_desc AS global_put_up_desc,
  veomd.gph_prod_sub_brnd AS global_prod_sub_brand,
  veomd.gph_prod_needstate AS global_prod_need_state,
  veomd.gph_prod_ctgry AS global_prod_category,
  veomd.gph_prod_subctgry AS global_prod_subcategory,
  veomd.gph_prod_sgmnt AS global_prod_segment,
  veomd.gph_prod_subsgmnt AS global_prod_subsegment,
  veomd.gph_prod_size AS global_prod_size,
  veomd.gph_prod_size_uom AS global_prod_size_uom,
  veocurd.from_ccy,
  veocurd.to_ccy,
  veocurd.exch_rate,
  NULL AS bill_type,
  0 AS bill_qty_pc,
  0 AS billing_grs_trd_sls,
  0 AS billing_subtot2,
  0 AS billing_subtot3,
  0 AS billing_subtot4,
  0 AS billing_net_amt,
  0 AS billing_est_nts,
  0 AS billing_invoice_val,
  0 AS billing_gross_val,
  veposf.pos_qty,
  (
    veposf.pos_gts * CAST((
      veocurd.exch_rate
    ) AS DOUBLE)
  ) AS pos_gts,
  (
    veposf.pos_item_prc * CAST((
      veocurd.exch_rate
    ) AS DOUBLE)
  ) AS pos_item_prc,
  veposf.pos_tax,
  (
    veposf.pos_nts * CAST((
      veocurd.exch_rate
    ) AS DOUBLE)
  ) AS pos_nts,
  veposf.conv_factor,
  veposf.jj_qty_pc AS jj_pos_qty_pc,
  (
    veposf.jj_item_prc_per_pc * veocurd.exch_rate
  ) AS jj_pos_item_prc_per_pc,
  (
    veposf.jj_gts * veocurd.exch_rate
  ) AS jj_pos_gts,
  veposf.jj_vat_amt AS jj_pos_vat_amt,
  (
    veposf.jj_nts * veocurd.exch_rate
  ) AS jj_pos_nts,
  immd.npi_ind AS is_npi,
  immd.npi_strt_period AS npi_str_period,
  NULL AS npi_end_period,
  NULL AS is_reg,
  immd.promo_reg_ind AS is_promo,
  NULL AS promo_strt_period,
  NULL AS promo_end_period,
  NULL AS is_mcl,
  immd.hero_ind AS is_hero
FROM (
  SELECT
    d.cntry_key,
    d.cntry_nm,
    d.rate_type,
    d.from_ccy,
    d.to_ccy,
    d.valid_date,
    d.jj_year,
    d.start_period,
    CASE
      WHEN (
        d.end_mnth_id = b.max_period
      )
      THEN CAST((
        CAST('209912' AS VARCHAR)
      ) AS TEXT)
      ELSE d.end_mnth_id
    END AS end_period,
    d.exch_rate
  FROM (
    SELECT
      a.cntry_key,
      a.cntry_nm,
      a.rate_type,
      a.from_ccy,
      a.to_ccy,
      a.valid_date,
      a.jj_year,
      MIN(CAST((
        a.jj_mnth_id
      ) AS TEXT)) AS start_period,
      MAX(CAST((
        a.jj_mnth_id
      ) AS TEXT)) AS end_mnth_id,
      a.exch_rate
    FROM edw_vw_os_curr_dim AS a
    WHERE
      (
        CAST((
          a.cntry_key
        ) AS TEXT) = CAST((
          CAST('MY' AS VARCHAR)
        ) AS TEXT)
      )
    GROUP BY
      a.cntry_key,
      a.cntry_nm,
      a.rate_type,
      a.from_ccy,
      a.to_ccy,
      a.valid_date,
      a.jj_year,
      a.exch_rate
  ) AS d, (
    SELECT
      MAX(CAST((
        a.jj_mnth_id
      ) AS TEXT)) AS max_period
    FROM edw_vw_os_curr_dim AS a
    WHERE
      (
        CAST((
          a.cntry_key
        ) AS TEXT) = CAST((
          CAST('MY' AS VARCHAR)
        ) AS TEXT)
      )
  ) AS b
) AS veocurd, (
  (
    (
      (
        (
          (
            (
              SELECT
                CASE
                  WHEN (
                    (
                      NOT a.jj_yr_week_no IS NULL
                    )
                    OR (
                      CAST((
                        a.jj_yr_week_no
                      ) AS TEXT) <> CAST((
                        CAST('' AS VARCHAR)
                      ) AS TEXT)
                    )
                  )
                  THEN b1."year"
                  ELSE b2."year"
                END AS "year",
                CASE
                  WHEN (
                    (
                      NOT a.jj_yr_week_no IS NULL
                    )
                    OR (
                      CAST((
                        a.jj_yr_week_no
                      ) AS TEXT) <> CAST((
                        CAST('' AS VARCHAR)
                      ) AS TEXT)
                    )
                  )
                  THEN b1.qrtr
                  ELSE b2.qrtr
                END AS qrtr,
                CASE
                  WHEN (
                    (
                      NOT a.jj_yr_week_no IS NULL
                    )
                    OR (
                      CAST((
                        a.jj_yr_week_no
                      ) AS TEXT) <> CAST((
                        CAST('' AS VARCHAR)
                      ) AS TEXT)
                    )
                  )
                  THEN b1.mnth_id
                  ELSE b2.mnth_id
                END AS mnth_id,
                CASE
                  WHEN (
                    (
                      NOT a.jj_yr_week_no IS NULL
                    )
                    OR (
                      CAST((
                        a.jj_yr_week_no
                      ) AS TEXT) <> CAST((
                        CAST('' AS VARCHAR)
                      ) AS TEXT)
                    )
                  )
                  THEN b1.mnth_no
                  ELSE b2.mnth_no
                END AS mnth_no,
                a.jj_yr_week_no,
                a.cntry_nm,
                a.cust_cd,
                a.cust_brnch_cd,
                a.item_cd,
                a.sap_matl_num,
                a.pos_qty,
                a.pos_gts,
                a.pos_item_prc,
                a.pos_tax,
                a.pos_nts,
                a.conv_factor,
                a.jj_qty_pc,
                a.jj_item_prc_per_pc,
                a.jj_gts,
                a.jj_vat_amt,
                a.jj_nts
              FROM (
                (
                  edw_vw_my_pos_sales_fact AS a
                    LEFT JOIN (
                      SELECT DISTINCT
                        (
                          CAST((
                            CAST((
                              edw_vw_os_time_dim."year"
                            ) AS VARCHAR)
                          ) AS TEXT) || RIGHT(
                            (
                              CAST((
                                CAST('00' AS VARCHAR)
                              ) AS TEXT) + CAST((
                                CAST((
                                  edw_vw_os_time_dim.wk
                                ) AS VARCHAR)
                              ) AS TEXT)
                            ),
                            2
                          )
                        ) AS yr_wk,
                        edw_vw_os_time_dim.wk,
                        edw_vw_os_time_dim.mnth_id,
                        edw_vw_os_time_dim."year",
                        edw_vw_os_time_dim.qrtr,
                        edw_vw_os_time_dim.mnth_no
                      FROM edw_vw_os_time_dim
                    ) AS b1
                      ON (
                        CASE
                          WHEN (
                            (
                              NOT a.jj_yr_week_no IS NULL
                            )
                            OR (
                              CAST((
                                a.jj_yr_week_no
                              ) AS TEXT) <> CAST((
                                CAST('' AS VARCHAR)
                              ) AS TEXT)
                            )
                          )
                          THEN (
                            CAST((
                              a.jj_yr_week_no
                            ) AS TEXT) = b1.yr_wk
                          )
                          ELSE CAST(NULL AS BOOLEAN)
                        END
                      )
                )
                LEFT JOIN (
                  SELECT DISTINCT
                    edw_vw_os_time_dim.mnth_id,
                    edw_vw_os_time_dim."year",
                    edw_vw_os_time_dim.qrtr,
                    edw_vw_os_time_dim.mnth_no
                  FROM edw_vw_os_time_dim
                ) AS b2
                  ON (
                    CASE
                      WHEN (
                        (
                          a.jj_yr_week_no IS NULL
                        )
                        OR (
                          CAST((
                            a.jj_yr_week_no
                          ) AS TEXT) = CAST((
                            CAST('' AS VARCHAR)
                          ) AS TEXT)
                        )
                      )
                      THEN (
                        CAST((
                          a.jj_mnth_id
                        ) AS TEXT) = b2.mnth_id
                      )
                      ELSE CAST(NULL AS BOOLEAN)
                    END
                  )
              )
              WHERE
                (
                  CAST((
                    a.cntry_cd
                  ) AS TEXT) = CAST((
                    CAST('MY' AS VARCHAR)
                  ) AS TEXT)
                )
            ) AS veposf
            LEFT JOIN (
              SELECT
                edw_vw_my_material_dim.cntry_key,
                edw_vw_my_material_dim.sap_matl_num,
                edw_vw_my_material_dim.sap_mat_desc,
                edw_vw_my_material_dim.ean_num,
                edw_vw_my_material_dim.sap_mat_type_cd,
                edw_vw_my_material_dim.sap_mat_type_desc,
                edw_vw_my_material_dim.sap_base_uom_cd,
                edw_vw_my_material_dim.sap_prchse_uom_cd,
                edw_vw_my_material_dim.sap_prod_sgmt_cd,
                edw_vw_my_material_dim.sap_prod_sgmt_desc,
                edw_vw_my_material_dim.sap_base_prod_cd,
                edw_vw_my_material_dim.sap_base_prod_desc,
                edw_vw_my_material_dim.sap_mega_brnd_cd,
                edw_vw_my_material_dim.sap_mega_brnd_desc,
                edw_vw_my_material_dim.sap_brnd_cd,
                edw_vw_my_material_dim.sap_brnd_desc,
                edw_vw_my_material_dim.sap_vrnt_cd,
                edw_vw_my_material_dim.sap_vrnt_desc,
                edw_vw_my_material_dim.sap_put_up_cd,
                edw_vw_my_material_dim.sap_put_up_desc,
                edw_vw_my_material_dim.sap_grp_frnchse_cd,
                edw_vw_my_material_dim.sap_grp_frnchse_desc,
                edw_vw_my_material_dim.sap_frnchse_cd,
                edw_vw_my_material_dim.sap_frnchse_desc,
                edw_vw_my_material_dim.sap_prod_frnchse_cd,
                edw_vw_my_material_dim.sap_prod_frnchse_desc,
                edw_vw_my_material_dim.sap_prod_mjr_cd,
                edw_vw_my_material_dim.sap_prod_mjr_desc,
                edw_vw_my_material_dim.sap_prod_mnr_cd,
                edw_vw_my_material_dim.sap_prod_mnr_desc,
                edw_vw_my_material_dim.sap_prod_hier_cd,
                edw_vw_my_material_dim.sap_prod_hier_desc,
                edw_vw_my_material_dim.gph_region,
                edw_vw_my_material_dim.gph_reg_frnchse,
                edw_vw_my_material_dim.gph_reg_frnchse_grp,
                edw_vw_my_material_dim.gph_prod_frnchse,
                edw_vw_my_material_dim.gph_prod_brnd,
                edw_vw_my_material_dim.gph_prod_sub_brnd,
                edw_vw_my_material_dim.gph_prod_vrnt,
                edw_vw_my_material_dim.gph_prod_needstate,
                edw_vw_my_material_dim.gph_prod_ctgry,
                edw_vw_my_material_dim.gph_prod_subctgry,
                edw_vw_my_material_dim.gph_prod_sgmnt,
                edw_vw_my_material_dim.gph_prod_subsgmnt,
                edw_vw_my_material_dim.gph_prod_put_up_cd,
                edw_vw_my_material_dim.gph_prod_put_up_desc,
                edw_vw_my_material_dim.gph_prod_size,
                edw_vw_my_material_dim.gph_prod_size_uom,
                edw_vw_my_material_dim.launch_dt,
                edw_vw_my_material_dim.qty_shipper_pc,
                edw_vw_my_material_dim.prft_ctr,
                edw_vw_my_material_dim.shlf_life
              FROM edw_vw_my_material_dim
              WHERE
                (
                  CAST((
                    edw_vw_my_material_dim.cntry_key
                  ) AS TEXT) = CAST((
                    CAST('MY' AS VARCHAR)
                  ) AS TEXT)
                )
            ) AS veomd
              ON (
                (
                  UPPER(
                    LTRIM(
                      CAST((
                        veomd.sap_matl_num
                      ) AS TEXT),
                      CAST((
                        CAST((
                          0
                        ) AS VARCHAR)
                      ) AS TEXT)
                    )
                  ) = LTRIM(CAST((
                    veposf.sap_matl_num
                  ) AS TEXT), CAST((
                    CAST('0' AS VARCHAR)
                  ) AS TEXT))
                )
              )
          )
          LEFT JOIN (
            SELECT
              edw_vw_my_customer_dim.sap_cust_id,
              edw_vw_my_customer_dim.sap_cust_nm,
              edw_vw_my_customer_dim.sap_sls_org,
              edw_vw_my_customer_dim.sap_cmp_id,
              edw_vw_my_customer_dim.sap_cntry_cd,
              edw_vw_my_customer_dim.sap_cntry_nm,
              edw_vw_my_customer_dim.sap_addr,
              edw_vw_my_customer_dim.sap_region,
              edw_vw_my_customer_dim.sap_state_cd,
              edw_vw_my_customer_dim.sap_city,
              edw_vw_my_customer_dim.sap_post_cd,
              edw_vw_my_customer_dim.sap_chnl_cd,
              edw_vw_my_customer_dim.sap_chnl_desc,
              edw_vw_my_customer_dim.sap_sls_office_cd,
              edw_vw_my_customer_dim.sap_sls_office_desc,
              edw_vw_my_customer_dim.sap_sls_grp_cd,
              edw_vw_my_customer_dim.sap_sls_grp_desc,
              edw_vw_my_customer_dim.sap_curr_cd,
              edw_vw_my_customer_dim.sap_prnt_cust_key,
              edw_vw_my_customer_dim.sap_prnt_cust_desc,
              edw_vw_my_customer_dim.sap_cust_chnl_key,
              edw_vw_my_customer_dim.sap_cust_chnl_desc,
              edw_vw_my_customer_dim.sap_cust_sub_chnl_key,
              edw_vw_my_customer_dim.sap_sub_chnl_desc,
              edw_vw_my_customer_dim.sap_go_to_mdl_key,
              edw_vw_my_customer_dim.sap_go_to_mdl_desc,
              edw_vw_my_customer_dim.sap_bnr_key,
              edw_vw_my_customer_dim.sap_bnr_desc,
              edw_vw_my_customer_dim.sap_bnr_frmt_key,
              edw_vw_my_customer_dim.sap_bnr_frmt_desc,
              edw_vw_my_customer_dim.retail_env,
              edw_vw_my_customer_dim.gch_region,
              edw_vw_my_customer_dim.gch_cluster,
              edw_vw_my_customer_dim.gch_subcluster,
              edw_vw_my_customer_dim.gch_market,
              edw_vw_my_customer_dim.gch_retail_banner
            FROM edw_vw_my_customer_dim
            WHERE
              (
                CAST((
                  edw_vw_my_customer_dim.sap_cntry_cd
                ) AS TEXT) = CAST((
                  CAST('MY' AS VARCHAR)
                ) AS TEXT)
              )
          ) AS veocd
            ON (
              (
                UPPER(
                  LTRIM(CAST((
                    veocd.sap_cust_id
                  ) AS TEXT), CAST((
                    CAST('0' AS VARCHAR)
                  ) AS TEXT))
                ) = UPPER(TRIM(SUBSTRING(CAST((
                  veposf.cust_cd
                ) AS TEXT), 0, 6)))
              )
            )
        )
        LEFT JOIN (
          SELECT
            edw_vw_os_pos_material_dim.item_cd,
            MAX(CAST((
              edw_vw_os_pos_material_dim.item_nm
            ) AS TEXT)) AS item_nm,
            edw_vw_os_pos_material_dim.cust_cd,
            edw_vw_os_pos_material_dim.sap_item_cd
          FROM edw_vw_os_pos_material_dim
          WHERE
            (
              CAST((
                edw_vw_os_pos_material_dim.cntry_cd
              ) AS TEXT) = CAST((
                CAST('MY' AS VARCHAR)
              ) AS TEXT)
            )
          GROUP BY
            edw_vw_os_pos_material_dim.item_cd,
            edw_vw_os_pos_material_dim.cust_cd,
            edw_vw_os_pos_material_dim.sap_item_cd
        ) AS vopmd
          ON (
            (
              (
                (
                  UPPER(
                    LTRIM(CAST((
                      vopmd.sap_item_cd
                    ) AS TEXT), CAST((
                      CAST('0' AS VARCHAR)
                    ) AS TEXT))
                  ) = LTRIM(CAST((
                    veposf.sap_matl_num
                  ) AS TEXT), CAST((
                    CAST('0' AS VARCHAR)
                  ) AS TEXT))
                )
                AND (
                  UPPER(
                    SUBSTRING(
                      LTRIM(CAST((
                        vopmd.cust_cd
                      ) AS TEXT), CAST((
                        CAST('0' AS VARCHAR)
                      ) AS TEXT)),
                      0,
                      6
                    )
                  ) = UPPER(TRIM(SUBSTRING(CAST((
                    veposf.cust_cd
                  ) AS TEXT), 0, 6)))
                )
              )
              AND (
                UPPER(
                  LTRIM(CAST((
                    vopmd.item_cd
                  ) AS TEXT), CAST((
                    CAST('0' AS VARCHAR)
                  ) AS TEXT))
                ) = UPPER(TRIM(CAST((
                  veposf.item_cd
                ) AS TEXT)))
              )
            )
          )
      )
      LEFT JOIN (
        SELECT
          edw_vw_my_pos_customer_dim.cntry_cd,
          edw_vw_my_pos_customer_dim.cntry_nm,
          edw_vw_my_pos_customer_dim.cust_cd,
          edw_vw_my_pos_customer_dim.cust_nm,
          edw_vw_my_pos_customer_dim.sold_to,
          edw_vw_my_pos_customer_dim.brnch_cd,
          edw_vw_my_pos_customer_dim.brnch_nm,
          edw_vw_my_pos_customer_dim.brnch_frmt,
          edw_vw_my_pos_customer_dim.brnch_typ,
          edw_vw_my_pos_customer_dim.dept_cd,
          edw_vw_my_pos_customer_dim.dept_nm,
          edw_vw_my_pos_customer_dim.address1,
          edw_vw_my_pos_customer_dim.address2,
          edw_vw_my_pos_customer_dim.region_cd,
          edw_vw_my_pos_customer_dim.region_nm,
          edw_vw_my_pos_customer_dim.prov_cd,
          edw_vw_my_pos_customer_dim.prov_nm,
          edw_vw_my_pos_customer_dim.city_cd,
          edw_vw_my_pos_customer_dim.city_nm,
          edw_vw_my_pos_customer_dim.mncplty_cd,
          edw_vw_my_pos_customer_dim.mncplty_nm
        FROM edw_vw_my_pos_customer_dim
        WHERE
          (
            CAST((
              edw_vw_my_pos_customer_dim.cntry_cd
            ) AS TEXT) = CAST((
              CAST('MY' AS VARCHAR)
            ) AS TEXT)
          )
      ) AS vopcd
        ON (
          (
            (
              UPPER(
                LTRIM(CAST((
                  vopcd.sold_to
                ) AS TEXT), CAST((
                  CAST('0' AS VARCHAR)
                ) AS TEXT))
              ) = UPPER(TRIM(SUBSTRING(CAST((
                veposf.cust_cd
              ) AS TEXT), 0, 6)))
            )
            AND (
              UPPER(
                LTRIM(CAST((
                  vopcd.brnch_cd
                ) AS TEXT), CAST((
                  CAST('0' AS VARCHAR)
                ) AS TEXT))
              ) = UPPER(TRIM(CAST((
                veposf.cust_brnch_cd
              ) AS TEXT)))
            )
          )
        )
    )
    LEFT JOIN itg_my_customer_dim AS imcd
      ON (
        (
          LTRIM(CAST((
            imcd.cust_id
          ) AS TEXT), CAST((
            CAST('0' AS VARCHAR)
          ) AS TEXT)) = UPPER(TRIM(SUBSTRING(CAST((
            veposf.cust_cd
          ) AS TEXT), 0, 6)))
        )
      )
  )
  LEFT JOIN itg_my_material_dim AS immd
    ON (
      (
        LTRIM(CAST((
          immd.item_cd
        ) AS TEXT), CAST((
          CAST('0' AS VARCHAR)
        ) AS TEXT)) = LTRIM(CAST((
          veposf.sap_matl_num
        ) AS TEXT), CAST((
          CAST('0' AS VARCHAR)
        ) AS TEXT))
      )
    )
)
WHERE
  (
    (
      veposf.mnth_id >= veocurd.start_period
    )
    AND (
      veposf.mnth_id <= veocurd.end_period
    )
  )
UNION ALL
SELECT
  CAST('SAP BW BILLING' AS VARCHAR) AS data_src,
  veosf."year" AS jj_year,
  CAST((
    veosf.qrtr
  ) AS VARCHAR) AS jj_qtr,
  CAST((
    veosf.mnth_id
  ) AS VARCHAR) AS jj_mnth_id,
  veosf.mnth_no AS jj_mnth_no,
  CAST((
    (
      CAST((
        CAST((
          veosf."year"
        ) AS VARCHAR)
      ) AS TEXT) || CAST((
        CAST((
          veosf.jj_yr_wk_no
        ) AS VARCHAR)
      ) AS TEXT)
    )
  ) AS VARCHAR) AS jj_year_wk_no,
  'Malaysia' AS cntry_nm,
  NULL AS cust_cd,
  NULL AS cust_brnch_cd,
  NULL AS mt_cust_brnch_nm,
  NULL AS cust_dept_cd,
  NULL AS mt_cust_dept_nm,
  NULL AS region_cd,
  NULL AS region_nm,
  NULL AS item_cd,
  NULL AS mt_item_nm,
  CAST((
    LTRIM(CAST((
      veocd.sap_cust_id
    ) AS TEXT), CAST((
      CAST('0' AS VARCHAR)
    ) AS TEXT))
  ) AS VARCHAR) AS sold_to,
  veocd.sap_cust_nm AS sold_to_nm,
  'SELLIN' AS trade_type,
  imcd.dstrbtr_grp_cd,
  imcd.dstrbtr_grp_nm,
  veocd.sap_state_cd,
  veocd.sap_sls_org,
  veocd.sap_cmp_id,
  veocd.sap_cntry_cd,
  veocd.sap_cntry_nm,
  veocd.sap_addr,
  veocd.sap_region,
  veocd.sap_city,
  veocd.sap_post_cd,
  veocd.sap_chnl_cd,
  veocd.sap_chnl_desc,
  veocd.sap_sls_office_cd,
  veocd.sap_sls_office_desc,
  veocd.sap_sls_grp_cd,
  veocd.sap_sls_grp_desc,
  veocd.sap_curr_cd,
  veocd.gch_region,
  veocd.gch_cluster,
  veocd.gch_subcluster,
  veocd.gch_market,
  veocd.gch_retail_banner,
  CAST((
    LTRIM(CAST((
      veomd.sap_matl_num
    ) AS TEXT), CAST((
      CAST('0' AS VARCHAR)
    ) AS TEXT))
  ) AS VARCHAR) AS sku,
  immd.frnchse_desc,
  immd.brnd_desc,
  immd.vrnt_desc,
  immd.putup_desc,
  immd.item_desc2,
  veomd.sap_mat_desc AS sku_desc,
  veomd.sap_mat_type_cd,
  veomd.sap_mat_type_desc,
  veomd.sap_base_uom_cd,
  veomd.sap_prchse_uom_cd,
  veomd.sap_prod_sgmt_cd,
  veomd.sap_prod_sgmt_desc,
  veomd.sap_base_prod_cd,
  veomd.sap_base_prod_desc,
  veomd.sap_mega_brnd_cd,
  veomd.sap_mega_brnd_desc,
  veomd.sap_brnd_cd,
  veomd.sap_brnd_desc,
  veomd.sap_vrnt_cd,
  veomd.sap_vrnt_desc,
  veomd.sap_put_up_cd,
  veomd.sap_put_up_desc,
  veomd.sap_grp_frnchse_cd,
  veomd.sap_grp_frnchse_desc,
  veomd.sap_frnchse_cd,
  veomd.sap_frnchse_desc,
  veomd.sap_prod_frnchse_cd,
  veomd.sap_prod_frnchse_desc,
  veomd.sap_prod_mjr_cd,
  veomd.sap_prod_mjr_desc,
  veomd.sap_prod_mnr_cd,
  veomd.sap_prod_mnr_desc,
  veomd.sap_prod_hier_cd,
  veomd.sap_prod_hier_desc,
  veomd.gph_region AS global_mat_region,
  veomd.gph_prod_frnchse AS global_prod_franchise,
  veomd.gph_prod_brnd AS global_prod_brand,
  veomd.gph_prod_vrnt AS global_prod_variant,
  veomd.gph_prod_put_up_cd AS global_prod_put_up_cd,
  veomd.gph_prod_put_up_desc AS global_put_up_desc,
  veomd.gph_prod_sub_brnd AS global_prod_sub_brand,
  veomd.gph_prod_needstate AS global_prod_need_state,
  veomd.gph_prod_ctgry AS global_prod_category,
  veomd.gph_prod_subctgry AS global_prod_subcategory,
  veomd.gph_prod_sgmnt AS global_prod_segment,
  veomd.gph_prod_subsgmnt AS global_prod_subsegment,
  veomd.gph_prod_size AS global_prod_size,
  veomd.gph_prod_size_uom AS global_prod_size_uom,
  veocurd.from_ccy,
  veocurd.to_ccy,
  veocurd.exch_rate,
  veosf.bill_type,
  SUM(veosf.bill_qty_pc) AS bill_qty_pc,
  SUM((
    veosf.grs_trd_sls * veocurd.exch_rate
  )) AS billing_grs_trd_sls,
  SUM((
    veosf.subtotal_2 * veocurd.exch_rate
  )) AS billing_subtot2,
  SUM((
    veosf.subtotal_3 * veocurd.exch_rate
  )) AS billing_subtot3,
  SUM((
    veosf.subtotal_4 * veocurd.exch_rate
  )) AS billing_subtot4,
  SUM((
    veosf.net_amt * veocurd.exch_rate
  )) AS billing_net_amt,
  SUM((
    veosf.est_nts * veocurd.exch_rate
  )) AS billing_est_nts,
  SUM((
    veosf.invoice_val * veocurd.exch_rate
  )) AS billing_invoice_val,
  SUM((
    veosf.gross_val * veocurd.exch_rate
  )) AS billing_gross_val,
  0.0 AS pos_qty,
  0.0 AS pos_gts,
  0.0 AS pos_item_prc,
  0.0 AS pos_tax,
  0.0 AS pos_nts,
  0 AS conv_factor,
  0 AS jj_pos_qty_pc,
  0 AS jj_pos_item_prc_per_pc,
  0 AS jj_pos_gts,
  0 AS jj_pos_vat_amt,
  0 AS jj_pos_nts,
  immd.npi_ind AS is_npi,
  immd.npi_strt_period AS npi_str_period,
  NULL AS npi_end_period,
  NULL AS is_reg,
  immd.promo_reg_ind AS is_promo,
  NULL AS promo_strt_period,
  NULL AS promo_end_period,
  NULL AS is_mcl,
  immd.hero_ind AS is_hero
FROM (
  SELECT
    d.cntry_key,
    d.cntry_nm,
    d.rate_type,
    d.from_ccy,
    d.to_ccy,
    d.valid_date,
    d.jj_year,
    d.start_period,
    CASE
      WHEN (
        d.end_mnth_id = b.max_period
      )
      THEN CAST((
        CAST('209912' AS VARCHAR)
      ) AS TEXT)
      ELSE d.end_mnth_id
    END AS end_period,
    d.exch_rate
  FROM (
    SELECT
      a.cntry_key,
      a.cntry_nm,
      a.rate_type,
      a.from_ccy,
      a.to_ccy,
      a.valid_date,
      a.jj_year,
      MIN(CAST((
        a.jj_mnth_id
      ) AS TEXT)) AS start_period,
      MAX(CAST((
        a.jj_mnth_id
      ) AS TEXT)) AS end_mnth_id,
      a.exch_rate
    FROM edw_vw_os_curr_dim AS a
    WHERE
      (
        CAST((
          a.cntry_key
        ) AS TEXT) = CAST((
          CAST('MY' AS VARCHAR)
        ) AS TEXT)
      )
    GROUP BY
      a.cntry_key,
      a.cntry_nm,
      a.rate_type,
      a.from_ccy,
      a.to_ccy,
      a.valid_date,
      a.jj_year,
      a.exch_rate
  ) AS d, (
    SELECT
      MAX(CAST((
        a.jj_mnth_id
      ) AS TEXT)) AS max_period
    FROM edw_vw_os_curr_dim AS a
    WHERE
      (
        CAST((
          a.cntry_key
        ) AS TEXT) = CAST((
          CAST('MY' AS VARCHAR)
        ) AS TEXT)
      )
  ) AS b
) AS veocurd, (
  (
    (
      (
        (
          (
            SELECT
              veotd."year",
              veotd.qrtr_no,
              veotd.qrtr,
              veotd.mnth_id,
              veotd.mnth_no,
              veotd.mnth_nm,
              veotd.jj_yr_wk_no,
              t1.matl_num AS sap_matl_num,
              CAST(NULL AS VARCHAR) AS dstrbtr_matl_num,
              t1.sold_to AS sap_soldto_code,
              t1.bill_type,
              SUM(t1.bill_qty_pc) AS bill_qty_pc,
              SUM((
                t1.grs_trd_sls * ABS(t1.exchg_rate)
              )) AS grs_trd_sls,
              SUM((
                t1.subtotal_2 * ABS(t1.exchg_rate)
              )) AS subtotal_2,
              SUM((
                t1.subtotal_3 * ABS(t1.exchg_rate)
              )) AS subtotal_3,
              SUM((
                t1.subtotal_4 * ABS(t1.exchg_rate)
              )) AS subtotal_4,
              SUM((
                t1.net_amt * ABS(t1.exchg_rate)
              )) AS net_amt,
              SUM((
                t1.est_nts * ABS(t1.exchg_rate)
              )) AS est_nts,
              SUM((
                t1.net_val * ABS(t1.exchg_rate)
              )) AS invoice_val,
              SUM((
                t1.gross_val * ABS(t1.exchg_rate)
              )) AS gross_val
            FROM (
              SELECT
                edw_vw_my_billing_fact.cntry_key,
                edw_vw_my_billing_fact.cntry_nm,
                edw_vw_my_billing_fact.bill_dt,
                edw_vw_my_billing_fact.bill_num,
                edw_vw_my_billing_fact.bill_item,
                edw_vw_my_billing_fact.bill_type,
                edw_vw_my_billing_fact.sls_doc_num,
                edw_vw_my_billing_fact.sls_doc_item,
                edw_vw_my_billing_fact.doc_curr,
                edw_vw_my_billing_fact.sd_doc_catgy,
                edw_vw_my_billing_fact.sold_to,
                edw_vw_my_billing_fact.matl_num,
                edw_vw_my_billing_fact.sls_org,
                edw_vw_my_billing_fact.exchg_rate,
                edw_vw_my_billing_fact.bill_qty_pc,
                edw_vw_my_billing_fact.grs_trd_sls,
                edw_vw_my_billing_fact.subtotal_2,
                edw_vw_my_billing_fact.subtotal_3,
                edw_vw_my_billing_fact.subtotal_4,
                edw_vw_my_billing_fact.net_amt,
                edw_vw_my_billing_fact.est_nts,
                edw_vw_my_billing_fact.net_val,
                edw_vw_my_billing_fact.gross_val
              FROM edw_vw_my_billing_fact
              WHERE
                (
                  (
                    (
                      edw_vw_my_billing_fact.cntry_key = CAST((
                        CAST('MY' AS VARCHAR)
                      ) AS TEXT)
                    )
                    AND (
                      (
                        CAST((
                          edw_vw_my_billing_fact.bill_type
                        ) AS TEXT) = CAST((
                          CAST('ZF2M' AS VARCHAR)
                        ) AS TEXT)
                      )
                      OR (
                        CAST((
                          edw_vw_my_billing_fact.bill_type
                        ) AS TEXT) = CAST((
                          CAST('ZG2M' AS VARCHAR)
                        ) AS TEXT)
                      )
                    )
                  )
                  AND (
                    CAST((
                      edw_vw_my_billing_fact.sold_to
                    ) AS VARCHAR(20)) IN (
                      SELECT DISTINCT
                        b.cust_id
                      FROM itg_my_pos_cust_mstr AS a, itg_my_customer_dim AS b
                      WHERE
                        (
                          CAST((
                            b.dstrbtr_grp_cd
                          ) AS TEXT) = CAST((
                            a.cust_id
                          ) AS TEXT)
                        )
                    )
                  )
                )
            ) AS t1, (
              SELECT DISTINCT
                edw_vw_os_time_dim."year",
                edw_vw_os_time_dim.qrtr_no,
                edw_vw_os_time_dim.qrtr,
                edw_vw_os_time_dim.mnth_id,
                edw_vw_os_time_dim.mnth_no,
                edw_vw_os_time_dim.mnth_desc AS mnth_nm,
                edw_vw_os_time_dim.wk AS jj_yr_wk_no,
                edw_vw_os_time_dim.cal_date,
                edw_vw_os_time_dim.cal_date_id
              FROM edw_vw_os_time_dim
            ) AS veotd
            WHERE
              (
                
                  veotd.cal_date
                 = 
                  t1.bill_dt
                
              )
            GROUP BY
              veotd."year",
              veotd.qrtr_no,
              veotd.qrtr,
              veotd.mnth_id,
              veotd.mnth_no,
              veotd.mnth_nm,
              veotd.jj_yr_wk_no,
              t1.matl_num,
              t1.sold_to,
              t1.bill_type
          ) AS veosf
          LEFT JOIN (
            SELECT
              edw_vw_my_customer_dim.sap_cust_id,
              edw_vw_my_customer_dim.sap_cust_nm,
              edw_vw_my_customer_dim.sap_sls_org,
              edw_vw_my_customer_dim.sap_cmp_id,
              edw_vw_my_customer_dim.sap_cntry_cd,
              edw_vw_my_customer_dim.sap_cntry_nm,
              edw_vw_my_customer_dim.sap_addr,
              edw_vw_my_customer_dim.sap_region,
              edw_vw_my_customer_dim.sap_state_cd,
              edw_vw_my_customer_dim.sap_city,
              edw_vw_my_customer_dim.sap_post_cd,
              edw_vw_my_customer_dim.sap_chnl_cd,
              edw_vw_my_customer_dim.sap_chnl_desc,
              edw_vw_my_customer_dim.sap_sls_office_cd,
              edw_vw_my_customer_dim.sap_sls_office_desc,
              edw_vw_my_customer_dim.sap_sls_grp_cd,
              edw_vw_my_customer_dim.sap_sls_grp_desc,
              edw_vw_my_customer_dim.sap_curr_cd,
              edw_vw_my_customer_dim.sap_prnt_cust_key,
              edw_vw_my_customer_dim.sap_prnt_cust_desc,
              edw_vw_my_customer_dim.sap_cust_chnl_key,
              edw_vw_my_customer_dim.sap_cust_chnl_desc,
              edw_vw_my_customer_dim.sap_cust_sub_chnl_key,
              edw_vw_my_customer_dim.sap_sub_chnl_desc,
              edw_vw_my_customer_dim.sap_go_to_mdl_key,
              edw_vw_my_customer_dim.sap_go_to_mdl_desc,
              edw_vw_my_customer_dim.sap_bnr_key,
              edw_vw_my_customer_dim.sap_bnr_desc,
              edw_vw_my_customer_dim.sap_bnr_frmt_key,
              edw_vw_my_customer_dim.sap_bnr_frmt_desc,
              edw_vw_my_customer_dim.retail_env,
              edw_vw_my_customer_dim.gch_region,
              edw_vw_my_customer_dim.gch_cluster,
              edw_vw_my_customer_dim.gch_subcluster,
              edw_vw_my_customer_dim.gch_market,
              edw_vw_my_customer_dim.gch_retail_banner
            FROM edw_vw_my_customer_dim
            WHERE
              (
                CAST((
                  edw_vw_my_customer_dim.sap_cntry_cd
                ) AS TEXT) = CAST((
                  CAST('MY' AS VARCHAR)
                ) AS TEXT)
              )
          ) AS veocd
            ON (
              (
                LTRIM(CAST((
                  veocd.sap_cust_id
                ) AS TEXT), CAST((
                  CAST('0' AS VARCHAR)
                ) AS TEXT)) = LTRIM(veosf.sap_soldto_code, CAST((
                  CAST('0' AS VARCHAR)
                ) AS TEXT))
              )
            )
        )
        LEFT JOIN (
          SELECT
            edw_vw_my_material_dim.cntry_key,
            edw_vw_my_material_dim.sap_matl_num,
            edw_vw_my_material_dim.sap_mat_desc,
            edw_vw_my_material_dim.ean_num,
            edw_vw_my_material_dim.sap_mat_type_cd,
            edw_vw_my_material_dim.sap_mat_type_desc,
            edw_vw_my_material_dim.sap_base_uom_cd,
            edw_vw_my_material_dim.sap_prchse_uom_cd,
            edw_vw_my_material_dim.sap_prod_sgmt_cd,
            edw_vw_my_material_dim.sap_prod_sgmt_desc,
            edw_vw_my_material_dim.sap_base_prod_cd,
            edw_vw_my_material_dim.sap_base_prod_desc,
            edw_vw_my_material_dim.sap_mega_brnd_cd,
            edw_vw_my_material_dim.sap_mega_brnd_desc,
            edw_vw_my_material_dim.sap_brnd_cd,
            edw_vw_my_material_dim.sap_brnd_desc,
            edw_vw_my_material_dim.sap_vrnt_cd,
            edw_vw_my_material_dim.sap_vrnt_desc,
            edw_vw_my_material_dim.sap_put_up_cd,
            edw_vw_my_material_dim.sap_put_up_desc,
            edw_vw_my_material_dim.sap_grp_frnchse_cd,
            edw_vw_my_material_dim.sap_grp_frnchse_desc,
            edw_vw_my_material_dim.sap_frnchse_cd,
            edw_vw_my_material_dim.sap_frnchse_desc,
            edw_vw_my_material_dim.sap_prod_frnchse_cd,
            edw_vw_my_material_dim.sap_prod_frnchse_desc,
            edw_vw_my_material_dim.sap_prod_mjr_cd,
            edw_vw_my_material_dim.sap_prod_mjr_desc,
            edw_vw_my_material_dim.sap_prod_mnr_cd,
            edw_vw_my_material_dim.sap_prod_mnr_desc,
            edw_vw_my_material_dim.sap_prod_hier_cd,
            edw_vw_my_material_dim.sap_prod_hier_desc,
            edw_vw_my_material_dim.gph_region,
            edw_vw_my_material_dim.gph_reg_frnchse,
            edw_vw_my_material_dim.gph_reg_frnchse_grp,
            edw_vw_my_material_dim.gph_prod_frnchse,
            edw_vw_my_material_dim.gph_prod_brnd,
            edw_vw_my_material_dim.gph_prod_sub_brnd,
            edw_vw_my_material_dim.gph_prod_vrnt,
            edw_vw_my_material_dim.gph_prod_needstate,
            edw_vw_my_material_dim.gph_prod_ctgry,
            edw_vw_my_material_dim.gph_prod_subctgry,
            edw_vw_my_material_dim.gph_prod_sgmnt,
            edw_vw_my_material_dim.gph_prod_subsgmnt,
            edw_vw_my_material_dim.gph_prod_put_up_cd,
            edw_vw_my_material_dim.gph_prod_put_up_desc,
            edw_vw_my_material_dim.gph_prod_size,
            edw_vw_my_material_dim.gph_prod_size_uom,
            edw_vw_my_material_dim.launch_dt,
            edw_vw_my_material_dim.qty_shipper_pc,
            edw_vw_my_material_dim.prft_ctr,
            edw_vw_my_material_dim.shlf_life
          FROM edw_vw_my_material_dim
          WHERE
            (
              CAST((
                edw_vw_my_material_dim.cntry_key
              ) AS TEXT) = CAST((
                CAST('MY' AS VARCHAR)
              ) AS TEXT)
            )
        ) AS veomd
          ON (
            (
              LTRIM(CAST((
                veomd.sap_matl_num
              ) AS TEXT), CAST((
                CAST('0' AS VARCHAR)
              ) AS TEXT)) = LTRIM(veosf.sap_matl_num, CAST((
                CAST('0' AS VARCHAR)
              ) AS TEXT))
            )
          )
      )
      LEFT JOIN itg_my_dstrbtrr_dim AS imdd
        ON (
          (
            LTRIM(CAST((
              imdd.cust_id
            ) AS TEXT), CAST((
              CAST('0' AS VARCHAR)
            ) AS TEXT)) = LTRIM(veosf.sap_soldto_code, CAST((
              CAST('0' AS VARCHAR)
            ) AS TEXT))
          )
        )
    )
    LEFT JOIN itg_my_material_dim AS immd
      ON (
        (
          LTRIM(CAST((
            immd.item_cd
          ) AS TEXT), CAST((
            CAST('0' AS VARCHAR)
          ) AS TEXT)) = LTRIM(veosf.sap_matl_num, CAST((
            CAST('0' AS VARCHAR)
          ) AS TEXT))
        )
      )
  )
  LEFT JOIN itg_my_customer_dim AS imcd
    ON (
      (
        LTRIM(CAST((
          imcd.cust_id
        ) AS TEXT), CAST((
          CAST('0' AS VARCHAR)
        ) AS TEXT)) = LTRIM(veosf.sap_soldto_code, CAST((
          CAST('0' AS VARCHAR)
        ) AS TEXT))
      )
    )
)
WHERE
  (
    (
      veosf.mnth_id >= veocurd.start_period
    )
    AND (
      veosf.mnth_id <= veocurd.end_period
    )
  )
GROUP BY
  1,
  veosf."year",
  veosf.qrtr,
  veosf.mnth_id,
  veosf.mnth_no,
  veosf.jj_yr_wk_no,
  LTRIM(CAST((
    veocd.sap_cust_id
  ) AS TEXT), CAST((
    CAST('0' AS VARCHAR)
  ) AS TEXT)),
  veocd.sap_cust_nm,
  imcd.dstrbtr_grp_cd,
  imcd.dstrbtr_grp_nm,
  veocd.sap_state_cd,
  veocd.sap_sls_org,
  veocd.sap_cmp_id,
  veocd.sap_cntry_cd,
  veocd.sap_cntry_nm,
  veocd.sap_addr,
  veocd.sap_region,
  veocd.sap_city,
  veocd.sap_post_cd,
  veocd.sap_chnl_cd,
  veocd.sap_chnl_desc,
  veocd.sap_sls_office_cd,
  veocd.sap_sls_office_desc,
  veocd.sap_sls_grp_cd,
  veocd.sap_sls_grp_desc,
  veocd.sap_curr_cd,
  veocd.gch_region,
  veocd.gch_cluster,
  veocd.gch_subcluster,
  veocd.gch_market,
  veocd.gch_retail_banner,
  LTRIM(CAST((
    veomd.sap_matl_num
  ) AS TEXT), CAST((
    CAST('0' AS VARCHAR)
  ) AS TEXT)),
  immd.frnchse_desc,
  immd.brnd_desc,
  immd.vrnt_desc,
  immd.putup_desc,
  immd.item_desc2,
  veomd.sap_mat_desc,
  veomd.sap_mat_type_cd,
  veomd.sap_mat_type_desc,
  veomd.sap_base_uom_cd,
  veomd.sap_prchse_uom_cd,
  veomd.sap_prod_sgmt_cd,
  veomd.sap_prod_sgmt_desc,
  veomd.sap_base_prod_cd,
  veomd.sap_base_prod_desc,
  veomd.sap_mega_brnd_cd,
  veomd.sap_mega_brnd_desc,
  veomd.sap_brnd_cd,
  veomd.sap_brnd_desc,
  veomd.sap_vrnt_cd,
  veomd.sap_vrnt_desc,
  veomd.sap_put_up_cd,
  veomd.sap_put_up_desc,
  veomd.sap_grp_frnchse_cd,
  veomd.sap_grp_frnchse_desc,
  veomd.sap_frnchse_cd,
  veomd.sap_frnchse_desc,
  veomd.sap_prod_frnchse_cd,
  veomd.sap_prod_frnchse_desc,
  veomd.sap_prod_mjr_cd,
  veomd.sap_prod_mjr_desc,
  veomd.sap_prod_mnr_cd,
  veomd.sap_prod_mnr_desc,
  veomd.sap_prod_hier_cd,
  veomd.sap_prod_hier_desc,
  veomd.gph_region,
  veomd.gph_prod_frnchse,
  veomd.gph_prod_brnd,
  veomd.gph_prod_vrnt,
  veomd.gph_prod_put_up_cd,
  veomd.gph_prod_put_up_desc,
  veomd.gph_prod_sub_brnd,
  veomd.gph_prod_needstate,
  veomd.gph_prod_ctgry,
  veomd.gph_prod_subctgry,
  veomd.gph_prod_sgmnt,
  veomd.gph_prod_subsgmnt,
  veomd.gph_prod_size,
  veomd.gph_prod_size_uom,
  veocurd.from_ccy,
  veocurd.to_ccy,
  veocurd.exch_rate,
  veosf.bill_type,
  immd.npi_ind,
  immd.npi_strt_period,
  immd.promo_reg_ind,
  immd.hero_ind
)
select count(*) from final 