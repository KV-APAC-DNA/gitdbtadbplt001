with edw_vw_os_curr_dim as(
    select * from os_edw.edw_vw_os_curr_dim
),
edw_vw_os_pos_sales_fact as(
    select * from os_edw.edw_vw_os_pos_sales_fact
),
edw_vw_os_material_dim as(
    select * from os_edw.edw_vw_os_material_dim
),
edw_vw_os_customer_dim as(
    select * from os_edw.edw_vw_os_customer_dim
),
edw_vw_os_pos_material_dim as(
    select * from os_edw.edw_vw_os_pos_material_dim   
),
edw_vw_os_pos_customer_dim as(
    select * from os_edw.edw_vw_os_pos_customer_dim   
),
itg_my_customer_dim as(
    select * from os_itg.itg_my_customer_dim  
),
itg_my_material_dim as(
    select * from os_itg.itg_my_material_dim  
),
transformed as (
   select
  'POS' AS data_src,
  veposf."year" AS jj_year,
  veposf.qrtr AS jj_qtr,
  veposf.mnth_id AS jj_mnth_id,
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
  vopmd.item_nm AS mt_item_nm,
  UPPER(
    LTRIM(CAST((
      vopcd.sold_to
    ) AS TEXT), CAST((
      CAST('0' AS VARCHAR)
    ) AS TEXT))
  ) AS sold_to,
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
  LTRIM(CAST((
    veomd.sap_matl_num
  ) AS TEXT), CAST((
    CAST('0' AS VARCHAR)
  ) AS TEXT)) AS sku,
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
  CAST(NULL AS UNKNOWN) AS npi_end_period,
  CAST(NULL AS UNKNOWN) AS is_reg,
  immd.promo_reg_ind AS is_promo,
  CAST(NULL AS UNKNOWN) AS promo_strt_period,
  CAST(NULL AS UNKNOWN) AS promo_end_period,
  CAST(NULL AS UNKNOWN) AS is_mcl,
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
                CAST((
                  LEFT(CAST((
                    a.jj_mnth_id
                  ) AS TEXT), 4)
                ) AS INT) AS "year",
                (
                  (
                    LEFT(CAST((
                      a.jj_mnth_id
                    ) AS TEXT), 4) || CAST((
                      CAST('/' AS VARCHAR)
                    ) AS TEXT)
                  ) || CAST((
                    CASE
                      WHEN (
                        RIGHT(CAST((
                          a.jj_mnth_id
                        ) AS TEXT), 2) = CAST((
                          CAST((
                            1
                          ) AS VARCHAR)
                        ) AS TEXT)
                      )
                      THEN CAST('Q1' AS VARCHAR)
                      WHEN (
                        RIGHT(CAST((
                          a.jj_mnth_id
                        ) AS TEXT), 2) = CAST((
                          CAST((
                            2
                          ) AS VARCHAR)
                        ) AS TEXT)
                      )
                      THEN CAST('Q1' AS VARCHAR)
                      WHEN (
                        RIGHT(CAST((
                          a.jj_mnth_id
                        ) AS TEXT), 2) = CAST((
                          CAST((
                            3
                          ) AS VARCHAR)
                        ) AS TEXT)
                      )
                      THEN CAST('Q1' AS VARCHAR)
                      WHEN (
                        RIGHT(CAST((
                          a.jj_mnth_id
                        ) AS TEXT), 2) = CAST((
                          CAST((
                            4
                          ) AS VARCHAR)
                        ) AS TEXT)
                      )
                      THEN CAST('Q2' AS VARCHAR)
                      WHEN (
                        RIGHT(CAST((
                          a.jj_mnth_id
                        ) AS TEXT), 2) = CAST((
                          CAST((
                            5
                          ) AS VARCHAR)
                        ) AS TEXT)
                      )
                      THEN CAST('Q2' AS VARCHAR)
                      WHEN (
                        RIGHT(CAST((
                          a.jj_mnth_id
                        ) AS TEXT), 2) = CAST((
                          CAST((
                            6
                          ) AS VARCHAR)
                        ) AS TEXT)
                      )
                      THEN CAST('Q2' AS VARCHAR)
                      WHEN (
                        RIGHT(CAST((
                          a.jj_mnth_id
                        ) AS TEXT), 2) = CAST((
                          CAST((
                            7
                          ) AS VARCHAR)
                        ) AS TEXT)
                      )
                      THEN CAST('Q3' AS VARCHAR)
                      WHEN (
                        RIGHT(CAST((
                          a.jj_mnth_id
                        ) AS TEXT), 2) = CAST((
                          CAST((
                            8
                          ) AS VARCHAR)
                        ) AS TEXT)
                      )
                      THEN CAST('Q3' AS VARCHAR)
                      WHEN (
                        RIGHT(CAST((
                          a.jj_mnth_id
                        ) AS TEXT), 2) = CAST((
                          CAST((
                            9
                          ) AS VARCHAR)
                        ) AS TEXT)
                      )
                      THEN CAST('Q3' AS VARCHAR)
                      WHEN (
                        RIGHT(CAST((
                          a.jj_mnth_id
                        ) AS TEXT), 2) = CAST((
                          CAST((
                            10
                          ) AS VARCHAR)
                        ) AS TEXT)
                      )
                      THEN CAST('Q4' AS VARCHAR)
                      WHEN (
                        RIGHT(CAST((
                          a.jj_mnth_id
                        ) AS TEXT), 2) = CAST((
                          CAST((
                            11
                          ) AS VARCHAR)
                        ) AS TEXT)
                      )
                      THEN CAST('Q4' AS VARCHAR)
                      WHEN (
                        RIGHT(CAST((
                          a.jj_mnth_id
                        ) AS TEXT), 2) = CAST((
                          CAST((
                            12
                          ) AS VARCHAR)
                        ) AS TEXT)
                      )
                      THEN CAST('Q4' AS VARCHAR)
                      ELSE CAST(NULL AS VARCHAR)
                    END
                  ) AS TEXT)
                ) AS qrtr,
                a.jj_mnth_id AS mnth_id,
                CAST((
                  RIGHT(CAST((
                    a.jj_mnth_id
                  ) AS TEXT), 2)
                ) AS INT) AS mnth_no,
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
                a.jj_nts,
                a.dept_cd
              FROM edw_vw_os_pos_sales_fact AS a
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
                edw_vw_os_material_dim.cntry_key,
                edw_vw_os_material_dim.sap_matl_num,
                edw_vw_os_material_dim.sap_mat_desc,
                edw_vw_os_material_dim.ean_num,
                edw_vw_os_material_dim.sap_mat_type_cd,
                edw_vw_os_material_dim.sap_mat_type_desc,
                edw_vw_os_material_dim.sap_base_uom_cd,
                edw_vw_os_material_dim.sap_prchse_uom_cd,
                edw_vw_os_material_dim.sap_prod_sgmt_cd,
                edw_vw_os_material_dim.sap_prod_sgmt_desc,
                edw_vw_os_material_dim.sap_base_prod_cd,
                edw_vw_os_material_dim.sap_base_prod_desc,
                edw_vw_os_material_dim.sap_mega_brnd_cd,
                edw_vw_os_material_dim.sap_mega_brnd_desc,
                edw_vw_os_material_dim.sap_brnd_cd,
                edw_vw_os_material_dim.sap_brnd_desc,
                edw_vw_os_material_dim.sap_vrnt_cd,
                edw_vw_os_material_dim.sap_vrnt_desc,
                edw_vw_os_material_dim.sap_put_up_cd,
                edw_vw_os_material_dim.sap_put_up_desc,
                edw_vw_os_material_dim.sap_grp_frnchse_cd,
                edw_vw_os_material_dim.sap_grp_frnchse_desc,
                edw_vw_os_material_dim.sap_frnchse_cd,
                edw_vw_os_material_dim.sap_frnchse_desc,
                edw_vw_os_material_dim.sap_prod_frnchse_cd,
                edw_vw_os_material_dim.sap_prod_frnchse_desc,
                edw_vw_os_material_dim.sap_prod_mjr_cd,
                edw_vw_os_material_dim.sap_prod_mjr_desc,
                edw_vw_os_material_dim.sap_prod_mnr_cd,
                edw_vw_os_material_dim.sap_prod_mnr_desc,
                edw_vw_os_material_dim.sap_prod_hier_cd,
                edw_vw_os_material_dim.sap_prod_hier_desc,
                edw_vw_os_material_dim.gph_region,
                edw_vw_os_material_dim.gph_prod_frnchse,
                edw_vw_os_material_dim.gph_prod_brnd,
                edw_vw_os_material_dim.gph_prod_sub_brnd,
                edw_vw_os_material_dim.gph_prod_vrnt,
                edw_vw_os_material_dim.gph_prod_needstate,
                edw_vw_os_material_dim.gph_prod_ctgry,
                edw_vw_os_material_dim.gph_prod_subctgry,
                edw_vw_os_material_dim.gph_prod_sgmnt,
                edw_vw_os_material_dim.gph_prod_subsgmnt,
                edw_vw_os_material_dim.gph_prod_put_up_cd,
                edw_vw_os_material_dim.gph_prod_put_up_desc,
                edw_vw_os_material_dim.gph_prod_size,
                edw_vw_os_material_dim.gph_prod_size_uom,
                edw_vw_os_material_dim.launch_dt,
                edw_vw_os_material_dim.qty_shipper_pc,
                edw_vw_os_material_dim.prft_ctr,
                edw_vw_os_material_dim.shlf_life
              FROM edw_vw_os_material_dim
              WHERE
                (
                  CAST((
                    edw_vw_os_material_dim.cntry_key
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
              edw_vw_os_customer_dim.sap_cust_id,
              edw_vw_os_customer_dim.sap_cust_nm,
              edw_vw_os_customer_dim.sap_sls_org,
              edw_vw_os_customer_dim.sap_cmp_id,
              edw_vw_os_customer_dim.sap_cntry_cd,
              edw_vw_os_customer_dim.sap_cntry_nm,
              edw_vw_os_customer_dim.sap_addr,
              edw_vw_os_customer_dim.sap_region,
              edw_vw_os_customer_dim.sap_state_cd,
              edw_vw_os_customer_dim.sap_city,
              edw_vw_os_customer_dim.sap_post_cd,
              edw_vw_os_customer_dim.sap_chnl_cd,
              edw_vw_os_customer_dim.sap_chnl_desc,
              edw_vw_os_customer_dim.sap_sls_office_cd,
              edw_vw_os_customer_dim.sap_sls_office_desc,
              edw_vw_os_customer_dim.sap_sls_grp_cd,
              edw_vw_os_customer_dim.sap_sls_grp_desc,
              edw_vw_os_customer_dim.sap_curr_cd,
              edw_vw_os_customer_dim.gch_region,
              edw_vw_os_customer_dim.gch_cluster,
              edw_vw_os_customer_dim.gch_subcluster,
              edw_vw_os_customer_dim.gch_market,
              edw_vw_os_customer_dim.gch_retail_banner
            FROM edw_vw_os_customer_dim
            WHERE
              (
                CAST((
                  edw_vw_os_customer_dim.sap_cntry_cd
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
                ) AS TEXT), 0, 7)))
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
                      7
                    )
                  ) = UPPER(TRIM(SUBSTRING(CAST((
                    veposf.cust_cd
                  ) AS TEXT), 0, 7)))
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
          edw_vw_os_pos_customer_dim.cntry_cd,
          edw_vw_os_pos_customer_dim.cntry_nm,
          edw_vw_os_pos_customer_dim.cust_cd,
          edw_vw_os_pos_customer_dim.cust_nm,
          edw_vw_os_pos_customer_dim.sold_to,
          edw_vw_os_pos_customer_dim.brnch_cd,
          edw_vw_os_pos_customer_dim.brnch_nm,
          edw_vw_os_pos_customer_dim.brnch_frmt,
          edw_vw_os_pos_customer_dim.brnch_typ,
          edw_vw_os_pos_customer_dim.dept_cd,
          edw_vw_os_pos_customer_dim.dept_nm,
          edw_vw_os_pos_customer_dim.address1,
          edw_vw_os_pos_customer_dim.address2,
          edw_vw_os_pos_customer_dim.region_cd,
          edw_vw_os_pos_customer_dim.region_nm,
          edw_vw_os_pos_customer_dim.prov_cd,
          edw_vw_os_pos_customer_dim.prov_nm,
          edw_vw_os_pos_customer_dim.city_cd,
          edw_vw_os_pos_customer_dim.city_nm,
          edw_vw_os_pos_customer_dim.mncplty_cd,
          edw_vw_os_pos_customer_dim.mncplty_nm
        FROM edw_vw_os_pos_customer_dim
        WHERE
          (
            CAST((
              edw_vw_os_pos_customer_dim.cntry_cd
            ) AS TEXT) = CAST((
              CAST('MY' AS VARCHAR)
            ) AS TEXT)
          )
      ) AS vopcd
        ON (
          (
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
                ) AS TEXT), 0, 7)))
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
            AND (
              UPPER(TRIM(CAST((
                COALESCE(vopcd.dept_cd, CAST('NA' AS VARCHAR))
              ) AS TEXT))) = UPPER(TRIM(CAST((
                COALESCE(veposf.dept_cd, CAST('NA' AS VARCHAR))
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
          ) AS TEXT), 0, 7)))
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
      CAST((
        veposf.mnth_id
      ) AS TEXT) >= veocurd.start_period
    )
    AND (
      CAST((
        veposf.mnth_id
      ) AS TEXT) <= veocurd.end_period
    )
  )
)

select * from transformed