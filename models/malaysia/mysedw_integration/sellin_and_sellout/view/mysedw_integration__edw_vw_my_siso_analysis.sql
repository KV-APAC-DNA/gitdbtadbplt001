with edw_vw_my_sellout_sales_fact as(
    select * from {{ ref('mysedw_integration__edw_vw_my_sellout_sales_fact') }}
),
edw_vw_my_curr_dim as(
    select * from {{ ref('mysedw_integration__edw_vw_my_curr_dim') }}
),
edw_vw_os_time_dim as(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_vw_my_dstrbtr_customer_dim as(
    select * from {{ ref('mysedw_integration__edw_vw_my_dstrbtr_customer_dim') }}
),
itg_my_gt_outlet_exclusion as(
    select * from {{ source('mysitg_integration', 'itg_my_gt_outlet_exclusion') }}
),
edw_vw_my_customer_dim as(
    select * from {{ ref('mysedw_integration__edw_vw_my_customer_dim') }}
),
edw_vw_my_material_dim as(
    select * from {{ ref('mysedw_integration__edw_vw_my_material_dim') }}
),
edw_vw_my_listprice as(
    select * from {{ ref('mysedw_integration__edw_vw_my_listprice') }}
),
edw_vw_my_billing_fact as(
    select * from {{ ref('mysedw_integration__edw_vw_my_billing_fact') }}
),
edw_vw_my_sellout_inventory_fact as(
    select * from {{ ref('mysedw_integration__edw_vw_my_sellout_inventory_fact') }}
),
itg_my_in_transit as(
    select * from  {{ ref('mysitg_integration__itg_my_in_transit') }}
),
transformed as(
    (
  (
    (
      SELECT
        CAST('GT Sellout' AS VARCHAR) AS data_src,
        veosf.year,
        veosf.qrtr_no,
        veosf.mnth_id,
        veosf.mnth_no,
        veosf.mnth_nm,
        CAST((
          SUBSTRING(
            REPLACE(
              CAST((
                CAST((
                  ym.bill_date
                ) AS VARCHAR)
              ) AS TEXT),
              CAST((
                CAST('-' AS VARCHAR)
              ) AS TEXT),
              CAST((
                CAST('' AS VARCHAR)
              ) AS TEXT)
            ),
            0,
            6
          )
        ) AS VARCHAR) AS max_yearmo,
        'Malaysia' AS cntry_nm,
        veosf.dstrbtr_grp_cd,
        imcd.dstrbtr_grp_nm,
        veosf.cust_cd AS dstrbtr_cust_cd,
        veosf.cust_nm AS dstrbtr_cust_nm,
        CAST((
          LTRIM(CAST((
            imdd.cust_id
          ) AS TEXT), CAST((
            CAST('0' AS VARCHAR)
          ) AS TEXT))
        ) AS VARCHAR) AS sap_soldto_code,
        imdd.cust_nm AS sap_soldto_nm,
        imdd.lvl1 AS dstrbtr_lvl1,
        imdd.lvl2 AS dstrbtr_lvl2,
        imdd.lvl3 AS dstrbtr_lvl3,
        imdd.lvl4 AS dstrbtr_lvl4,
        imdd.lvl5 AS dstrbtr_lvl5,
        imdd.region AS region_nm,
        veosf.town_nm,
        veosf.slsmn_cd,
        veosf.chnl_desc,
        veosf.sub_chnl_desc,
        veosf.chnl_attr1_desc,
        veosf.chnl_attr2_desc,
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
        veosf.wh_id,
        veosf.doc_type,
        veosf.doc_type_desc,
        SUM((
          veosf.base_sls * veocurd.exch_rate
        )) AS base_sls,
        SUM(veosf.sls_qty) AS sls_qty,
        SUM(veosf.ret_qty) AS ret_qty,
        veosf.uom,
        SUM(veosf.sls_qty_pc) AS sls_qty_pc,
        SUM(veosf.ret_qty_pc) AS ret_qty_pc,
        0 AS bill_qty_pc,
        0 AS in_transit_qty,
        SUM(lp.rate) AS mat_list_price,
        SUM((
          veosf.grs_trd_sls * veocurd.exch_rate
        )) AS grs_trd_sls,
        SUM((
          veosf.ret_val * veocurd.exch_rate
        )) AS ret_val,
        SUM((
          veosf.trd_discnt * veocurd.exch_rate
        )) AS trd_discnt,
        SUM((
          veosf.trd_sls * veocurd.exch_rate
        )) AS trd_sls,
        SUM((
          veosf.net_trd_sls * veocurd.exch_rate
        )) AS net_trd_sls,
        SUM((
          veosf.jj_grs_trd_sls * veocurd.exch_rate
        )) AS jj_grs_trd_sls,
        SUM((
          veosf.jj_ret_val * veocurd.exch_rate
        )) AS jj_ret_val,
        SUM((
          veosf.jj_trd_sls * veocurd.exch_rate
        )) AS jj_trd_sls,
        SUM((
          veosf.jj_net_trd_sls * veocurd.exch_rate
        )) AS jj_net_trd_sls,
        0 AS billing_grs_trd_sls,
        0 AS billing_subtot2,
        0 AS billing_subtot3,
        0 AS billing_subtot4,
        0 AS billing_net_amt,
        0 AS billing_est_nts,
        0 AS billing_invoice_val,
        0 AS billing_gross_val,
        0 AS in_transit_val,
        0 AS trgt_val,
        NULL AS warehse_cd,
        0 AS end_stock_qty,
        0 AS end_stock_val_raw,
        0 AS end_stock_val,
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
          MAX(edw_vw_my_sellout_sales_fact.bill_date) AS bill_date
        FROM edw_vw_my_sellout_sales_fact
        WHERE
          (
            CAST((
              edw_vw_my_sellout_sales_fact.cntry_cd
            ) AS TEXT) = CAST((
              CAST('MY' AS VARCHAR)
            ) AS TEXT)
          )
      ) AS ym, (
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
          FROM edw_vw_my_curr_dim AS a
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
          FROM edw_vw_my_curr_dim AS a
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
                      veotd.year,
                      veotd.qrtr_no,
                      veotd.mnth_id,
                      veotd.mnth_no,
                      veotd.mnth_nm,
                      t1.dstrbtr_grp_cd,
                      t1.cust_cd,
                      t1.slsmn_cd,
                      evodcd.cust_nm,
                      evodcd.sap_soldto_code,
                      t1.sap_matl_num,
                      UPPER(TRIM(CAST((
                        t1.dstrbtr_matl_num
                      ) AS TEXT))) AS dstrbtr_matl_num,
                      evodcd.region_nm,
                      evodcd.town_nm,
                      evodcd.chnl_desc,
                      evodcd.sub_chnl_desc,
                      evodcd.chnl_attr1_desc,
                      evodcd.chnl_attr2_desc,
                      t1.wh_id,
                      t1.doc_type,
                      t1.doc_type_desc,
                      SUM(t1.base_sls) AS base_sls,
                      t1.bill_doc,
                      SUM(t1.sls_qty) AS sls_qty,
                      SUM(t1.ret_qty) AS ret_qty,
                      t1.uom,
                      SUM(t1.sls_qty_pc) AS sls_qty_pc,
                      SUM(t1.ret_qty_pc) AS ret_qty_pc,
                      SUM(t1.grs_trd_sls) AS grs_trd_sls,
                      SUM(t1.ret_val) AS ret_val,
                      SUM(t1.trd_discnt) AS trd_discnt,
                      SUM(t1.trd_sls) AS trd_sls,
                      SUM(t1.net_trd_sls) AS net_trd_sls,
                      SUM(t1.jj_grs_trd_sls) AS jj_grs_trd_sls,
                      SUM(t1.jj_ret_val) AS jj_ret_val,
                      SUM(t1.jj_trd_sls) AS jj_trd_sls,
                      SUM(t1.jj_net_trd_sls) AS jj_net_trd_sls
                    FROM (
                      SELECT DISTINCT
                        edw_vw_os_time_dim.cal_year AS year,
                        edw_vw_os_time_dim.cal_qrtr_no AS qrtr_no,
                        edw_vw_os_time_dim.cal_mnth_id AS mnth_id,
                        edw_vw_os_time_dim.cal_mnth_no AS mnth_no,
                        edw_vw_os_time_dim.cal_mnth_nm AS mnth_nm,
                        edw_vw_os_time_dim.cal_date,
                        edw_vw_os_time_dim.cal_date_id
                      FROM edw_vw_os_time_dim
                    ) AS veotd, (
                      edw_vw_my_sellout_sales_fact AS t1
                        LEFT JOIN (
                          SELECT
                            edw_vw_my_dstrbtr_customer_dim.cntry_cd,
                            edw_vw_my_dstrbtr_customer_dim.cntry_nm,
                            edw_vw_my_dstrbtr_customer_dim.dstrbtr_grp_cd,
                            edw_vw_my_dstrbtr_customer_dim.dstrbtr_soldto_code,
                            edw_vw_my_dstrbtr_customer_dim.sap_soldto_code,
                            edw_vw_my_dstrbtr_customer_dim.cust_cd,
                            edw_vw_my_dstrbtr_customer_dim.cust_nm,
                            edw_vw_my_dstrbtr_customer_dim.alt_cust_cd,
                            edw_vw_my_dstrbtr_customer_dim.alt_cust_nm,
                            edw_vw_my_dstrbtr_customer_dim.addr,
                            edw_vw_my_dstrbtr_customer_dim.area_cd,
                            edw_vw_my_dstrbtr_customer_dim.area_nm,
                            edw_vw_my_dstrbtr_customer_dim.state_cd,
                            edw_vw_my_dstrbtr_customer_dim.state_nm,
                            edw_vw_my_dstrbtr_customer_dim.region_cd,
                            edw_vw_my_dstrbtr_customer_dim.region_nm,
                            edw_vw_my_dstrbtr_customer_dim.prov_cd,
                            edw_vw_my_dstrbtr_customer_dim.prov_nm,
                            edw_vw_my_dstrbtr_customer_dim.town_cd,
                            edw_vw_my_dstrbtr_customer_dim.town_nm,
                            edw_vw_my_dstrbtr_customer_dim.city_cd,
                            edw_vw_my_dstrbtr_customer_dim.city_nm,
                            edw_vw_my_dstrbtr_customer_dim.post_cd,
                            edw_vw_my_dstrbtr_customer_dim.post_nm,
                            edw_vw_my_dstrbtr_customer_dim.slsmn_cd,
                            edw_vw_my_dstrbtr_customer_dim.slsmn_nm,
                            edw_vw_my_dstrbtr_customer_dim.chnl_cd,
                            edw_vw_my_dstrbtr_customer_dim.chnl_desc,
                            edw_vw_my_dstrbtr_customer_dim.sub_chnl_cd,
                            edw_vw_my_dstrbtr_customer_dim.sub_chnl_desc,
                            edw_vw_my_dstrbtr_customer_dim.chnl_attr1_cd,
                            edw_vw_my_dstrbtr_customer_dim.chnl_attr1_desc,
                            edw_vw_my_dstrbtr_customer_dim.chnl_attr2_cd,
                            edw_vw_my_dstrbtr_customer_dim.chnl_attr2_desc,
                            edw_vw_my_dstrbtr_customer_dim.outlet_type_cd,
                            edw_vw_my_dstrbtr_customer_dim.outlet_type_desc,
                            edw_vw_my_dstrbtr_customer_dim.cust_grp_cd,
                            edw_vw_my_dstrbtr_customer_dim.cust_grp_desc,
                            edw_vw_my_dstrbtr_customer_dim.cust_grp_attr1_cd,
                            edw_vw_my_dstrbtr_customer_dim.cust_grp_attr1_desc,
                            edw_vw_my_dstrbtr_customer_dim.cust_grp_attr2_cd,
                            edw_vw_my_dstrbtr_customer_dim.cust_grp_attr2_desc,
                            edw_vw_my_dstrbtr_customer_dim.sls_dstrct_cd,
                            edw_vw_my_dstrbtr_customer_dim.sls_dstrct_nm,
                            edw_vw_my_dstrbtr_customer_dim.sls_office_cd,
                            edw_vw_my_dstrbtr_customer_dim.sls_office_desc,
                            edw_vw_my_dstrbtr_customer_dim.sls_grp_cd,
                            edw_vw_my_dstrbtr_customer_dim.sls_grp_desc,
                            edw_vw_my_dstrbtr_customer_dim.status
                          FROM edw_vw_my_dstrbtr_customer_dim
                          WHERE
                            (
                              CAST((
                                edw_vw_my_dstrbtr_customer_dim.cntry_cd
                              ) AS TEXT) = CAST((
                                CAST('MY' AS VARCHAR)
                              ) AS TEXT)
                            )
                        ) AS evodcd
                          ON (
                            (
                              (
                                (
                                  LTRIM(CAST((
                                    evodcd.cust_cd
                                  ) AS TEXT), CAST((
                                    CAST('0' AS VARCHAR)
                                  ) AS TEXT)) = LTRIM(CAST((
                                    t1.cust_cd
                                  ) AS TEXT), CAST((
                                    CAST('0' AS VARCHAR)
                                  ) AS TEXT))
                                )
                                AND (
                                  LTRIM(
                                    CAST((
                                      evodcd.dstrbtr_grp_cd
                                    ) AS TEXT),
                                    CAST((
                                      CAST('0' AS VARCHAR)
                                    ) AS TEXT)
                                  ) = LTRIM(CAST((
                                    t1.dstrbtr_grp_cd
                                  ) AS TEXT), CAST((
                                    CAST('0' AS VARCHAR)
                                  ) AS TEXT))
                                )
                              )
                              AND (
                                LTRIM(
                                  CAST((
                                    evodcd.sap_soldto_code
                                  ) AS TEXT),
                                  CAST((
                                    CAST('0' AS VARCHAR)
                                  ) AS TEXT)
                                ) = LTRIM(
                                  CAST((
                                    t1.dstrbtr_soldto_code
                                  ) AS TEXT),
                                  CAST((
                                    CAST('0' AS VARCHAR)
                                  ) AS TEXT)
                                )
                              )
                            )
                          )
                    )
                    WHERE
                      (
                        (
                          (
                            CAST((
                              veotd.cal_date
                            ) AS TIMESTAMPNTZ) = t1.bill_date
                          )
                          AND (
                            CAST((
                              t1.cntry_cd
                            ) AS TEXT) = CAST((
                              CAST('MY' AS VARCHAR)
                            ) AS TEXT)
                          )
                        )
                        AND (
                          NOT (
                            (
                              COALESCE(
                                LTRIM(
                                  CAST((
                                    t1.dstrbtr_soldto_code
                                  ) AS TEXT),
                                  CAST((
                                    CAST('0' AS VARCHAR)
                                  ) AS TEXT)
                                ),
                                CAST((
                                  CAST('0' AS VARCHAR)
                                ) AS TEXT)
                              ) || COALESCE(TRIM(CAST((
                                t1.cust_cd
                              ) AS TEXT)), CAST((
                                CAST('0' AS VARCHAR)
                              ) AS TEXT))
                            ) IN (
                              SELECT DISTINCT
                                (
                                  CAST((
                                    COALESCE(itg_my_gt_outlet_exclusion.dstrbtr_cd, CAST('0' AS VARCHAR))
                                  ) AS TEXT) || CAST((
                                    COALESCE(itg_my_gt_outlet_exclusion.outlet_cd, CAST('0' AS VARCHAR))
                                  ) AS TEXT)
                                )
                              FROM itg_my_gt_outlet_exclusion
                            )
                          )
                        )
                      )
                    GROUP BY
                      veotd.year,
                      veotd.qrtr_no,
                      veotd.mnth_id,
                      veotd.mnth_no,
                      veotd.mnth_nm,
                      t1.dstrbtr_grp_cd,
                      t1.cust_cd,
                      t1.slsmn_cd,
                      evodcd.cust_nm,
                      evodcd.sap_soldto_code,
                      t1.sap_matl_num,
                      UPPER(TRIM(CAST((
                        t1.dstrbtr_matl_num
                      ) AS TEXT))),
                      evodcd.region_nm,
                      evodcd.town_nm,
                      evodcd.chnl_desc,
                      evodcd.sub_chnl_desc,
                      evodcd.chnl_attr1_desc,
                      evodcd.chnl_attr2_desc,
                      t1.wh_id,
                      t1.doc_type,
                      t1.doc_type_desc,
                      t1.bill_doc,
                      t1.uom
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
                        ) AS TEXT)) = LTRIM(
                          CAST((
                            veosf.sap_soldto_code
                          ) AS TEXT),
                          CAST((
                            CAST('0' AS VARCHAR)
                          ) AS TEXT)
                        )
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
                      ) AS TEXT)) = LTRIM(CAST((
                        veosf.sap_matl_num
                      ) AS TEXT), CAST((
                        CAST('0' AS VARCHAR)
                      ) AS TEXT))
                    )
                  )
              )
              LEFT JOIN snaposeitg_integration.itg_my_dstrbtrr_dim AS imdd
                ON (
                  (
                    LTRIM(CAST((
                      imdd.cust_id
                    ) AS TEXT), CAST((
                      CAST('0' AS VARCHAR)
                    ) AS TEXT)) = LTRIM(
                      CAST((
                        veosf.sap_soldto_code
                      ) AS TEXT),
                      CAST((
                        CAST('0' AS VARCHAR)
                      ) AS TEXT)
                    )
                  )
                )
            )
            LEFT JOIN snaposeitg_integration.itg_my_material_dim AS immd
              ON (
                (
                  LTRIM(CAST((
                    immd.item_cd
                  ) AS TEXT), CAST((
                    CAST('0' AS VARCHAR)
                  ) AS TEXT)) = LTRIM(CAST((
                    veosf.sap_matl_num
                  ) AS TEXT), CAST((
                    CAST('0' AS VARCHAR)
                  ) AS TEXT))
                )
              )
          )
          LEFT JOIN snaposeitg_integration.itg_my_customer_dim AS imcd
            ON (
              (
                TRIM(CAST((
                  imcd.cust_id
                ) AS TEXT)) = TRIM(CAST((
                  veosf.sap_soldto_code
                ) AS TEXT))
              )
            )
        )
        LEFT JOIN (
          SELECT
            edw_vw_my_listprice.cntry_key,
            edw_vw_my_listprice.cntry_nm,
            edw_vw_my_listprice.plant,
            edw_vw_my_listprice.cnty,
            edw_vw_my_listprice.item_cd,
            edw_vw_my_listprice.item_desc,
            edw_vw_my_listprice.valid_from,
            edw_vw_my_listprice.valid_to,
            edw_vw_my_listprice.rate,
            edw_vw_my_listprice.currency,
            edw_vw_my_listprice.price_unit,
            edw_vw_my_listprice.uom,
            edw_vw_my_listprice.yearmo,
            edw_vw_my_listprice.mnth_type,
            edw_vw_my_listprice.snapshot_dt
          FROM edw_vw_my_listprice
          WHERE
            (
              (
                CAST((
                  edw_vw_my_listprice.cntry_key
                ) AS TEXT) = CAST((
                  CAST('MY' AS VARCHAR)
                ) AS TEXT)
              )
              AND (
                CAST((
                  edw_vw_my_listprice.mnth_type
                ) AS TEXT) = CAST((
                  CAST('CAL' AS VARCHAR)
                ) AS TEXT)
              )
            )
        ) AS lp
          ON (
            (
              (
                LTRIM(CAST((
                  lp.item_cd
                ) AS TEXT), CAST((
                  CAST('0' AS VARCHAR)
                ) AS TEXT)) = LTRIM(CAST((
                  veosf.sap_matl_num
                ) AS TEXT), CAST((
                  CAST('0' AS VARCHAR)
                ) AS TEXT))
              )
              AND (
                CAST((
                  lp.yearmo
                ) AS TEXT) = CAST((
                  CAST((
                    veosf.mnth_id
                  ) AS VARCHAR)
                ) AS TEXT)
              )
            )
          )
      )
      WHERE
        (
          (
            CAST((
              CAST((
                veosf.mnth_id
              ) AS VARCHAR)
            ) AS TEXT) >= veocurd.start_period
          )
          AND (
            CAST((
              CAST((
                veosf.mnth_id
              ) AS VARCHAR)
            ) AS TEXT) <= veocurd.end_period
          )
        )
      GROUP BY
        1,
        veosf.year,
        veosf.qrtr_no,
        veosf.mnth_id,
        veosf.mnth_no,
        veosf.mnth_nm,
        ym.bill_date,
        veosf.dstrbtr_grp_cd,
        imcd.dstrbtr_grp_nm,
        veosf.cust_cd,
        veosf.cust_nm,
        LTRIM(CAST((
          imdd.cust_id
        ) AS TEXT), CAST((
          CAST('0' AS VARCHAR)
        ) AS TEXT)),
        imdd.cust_nm,
        imdd.lvl1,
        imdd.lvl2,
        imdd.lvl3,
        imdd.lvl4,
        imdd.lvl5,
        imdd.region,
        veosf.town_nm,
        veosf.slsmn_cd,
        veosf.chnl_desc,
        veosf.sub_chnl_desc,
        veosf.chnl_attr1_desc,
        veosf.chnl_attr2_desc,
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
        veosf.uom,
        immd.npi_ind,
        immd.npi_strt_period,
        immd.promo_reg_ind,
        immd.hero_ind,
        veosf.wh_id,
        veosf.doc_type,
        veosf.doc_type_desc
      UNION ALL
      SELECT
        CAST('SAP BW BILLING' AS VARCHAR) AS data_src,
        veosf.year,
        veosf.qrtr_no,
        veosf.mnth_id,
        veosf.mnth_no,
        veosf.mnth_nm,
        NULL AS max_yearmo,
        'Malaysia' AS cntry_nm,
        imcd.dstrbtr_grp_cd,
        imcd.dstrbtr_grp_nm,
        NULL AS dstrbtr_cust_cd,
        NULL AS dstrbtr_cust_nm,
        CAST((
          LTRIM(CAST((
            imdd.cust_id
          ) AS TEXT), CAST((
            CAST('0' AS VARCHAR)
          ) AS TEXT))
        ) AS VARCHAR) AS sap_soldto_code,
        imdd.cust_nm AS sap_soldto_nm,
        imdd.lvl1 AS dstrbtr_lvl1,
        imdd.lvl2 AS dstrbtr_lvl2,
        imdd.lvl3 AS dstrbtr_lvl3,
        imdd.lvl4 AS dstrbtr_lvl4,
        imdd.lvl5 AS dstrbtr_lvl5,
        imdd.region AS region_nm,
        NULL AS town_nm,
        NULL AS slsmn_cd,
        NULL AS chnl_desc,
        NULL AS sub_chnl_desc,
        NULL AS chnl_attr1_desc,
        NULL AS chnl_attr2_desc,
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
        NULL AS wh_id,
        NULL AS doc_type,
        NULL AS doc_type_desc,
        0 AS base_sls,
        0 AS sls_qty,
        0 AS ret_qty,
        NULL AS uom,
        0 AS sls_qty_pc,
        0 AS ret_qty_pc,
        SUM(veosf.bill_qty_pc) AS bill_qty_pc,
        0 AS in_transit_qty,
        0 AS mat_list_price,
        0 AS grs_trd_sls,
        0 AS ret_val,
        0 AS trd_discnt,
        0 AS trd_sls,
        0 AS net_trd_sls,
        0 AS jj_grs_trd_sls,
        0 AS jj_ret_val,
        0 AS jj_trd_sls,
        0 AS jj_net_trd_sls,
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
        0 AS in_transit_val,
        0 AS trgt_val,
        NULL AS warehse_cd,
        0 AS end_stock_qty,
        0 AS end_stock_val_raw,
        0 AS end_stock_val,
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
          FROM edw_vw_my_curr_dim AS a
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
          FROM edw_vw_my_curr_dim AS a
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
                    veotd.year,
                    veotd.qrtr_no,
                    veotd.mnth_id,
                    veotd.mnth_no,
                    veotd.mnth_nm,
                    t1.matl_num AS sap_matl_num,
                    CAST(NULL AS VARCHAR) AS dstrbtr_matl_num,
                    t1.sold_to AS sap_soldto_code,
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
                            CAST((
                              edw_vw_my_billing_fact.bill_type
                            ) AS TEXT) = CAST((
                              CAST('ZF2M' AS VARCHAR)
                            ) AS TEXT)
                          )
                        )
                        AND (
                          CAST((
                            edw_vw_my_billing_fact.sold_to
                          ) AS VARCHAR(10)) IN (
                            SELECT DISTINCT
                              itg_my_dstrbtrr_dim.cust_id
                            FROM snaposeitg_integration.itg_my_dstrbtrr_dim
                          )
                        )
                      )
                  ) AS t1, (
                    SELECT DISTINCT
                      edw_vw_os_time_dim.cal_year AS year,
                      edw_vw_os_time_dim.cal_qrtr_no AS qrtr_no,
                      edw_vw_os_time_dim.cal_mnth_id AS mnth_id,
                      edw_vw_os_time_dim.cal_mnth_no AS mnth_no,
                      edw_vw_os_time_dim.cal_mnth_nm AS mnth_nm,
                      edw_vw_os_time_dim.cal_date,
                      edw_vw_os_time_dim.cal_date_id
                    FROM edw_vw_os_time_dim
                  ) AS veotd
                  WHERE
                    (
                      CAST((
                        veotd.cal_date
                      ) AS TIMESTAMPNTZ) = CAST((
                        t1.bill_dt
                      ) AS TIMESTAMPNTZ)
                    )
                  GROUP BY
                    veotd.year,
                    veotd.qrtr_no,
                    veotd.mnth_id,
                    veotd.mnth_no,
                    veotd.mnth_nm,
                    t1.matl_num,
                    t1.sold_to
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
            LEFT JOIN snaposeitg_integration.itg_my_dstrbtrr_dim AS imdd
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
          LEFT JOIN snaposeitg_integration.itg_my_material_dim AS immd
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
        LEFT JOIN snaposeitg_integration.itg_my_customer_dim AS imcd
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
            CAST((
              CAST((
                veosf.mnth_id
              ) AS VARCHAR)
            ) AS TEXT) >= veocurd.start_period
          )
          AND (
            CAST((
              CAST((
                veosf.mnth_id
              ) AS VARCHAR)
            ) AS TEXT) <= veocurd.end_period
          )
        )
      GROUP BY
        1,
        veosf.year,
        veosf.qrtr_no,
        veosf.mnth_id,
        veosf.mnth_no,
        veosf.mnth_nm,
        imcd.dstrbtr_grp_cd,
        imcd.dstrbtr_grp_nm,
        LTRIM(CAST((
          imdd.cust_id
        ) AS TEXT), CAST((
          CAST('0' AS VARCHAR)
        ) AS TEXT)),
        imdd.cust_nm,
        imdd.lvl1,
        imdd.lvl2,
        imdd.lvl3,
        imdd.lvl4,
        imdd.lvl5,
        imdd.region,
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
        veosf.bill_qty_pc,
        immd.npi_ind,
        immd.npi_strt_period,
        immd.promo_reg_ind,
        immd.hero_ind
    )
    UNION ALL
    SELECT
      'GT Inventory' AS data_src,
      evosif.year,
      evosif.qrtr_no,
      evosif.mnth_id,
      evosif.mnth_no,
      evosif.mnth_nm,
      NULL AS max_yearmo,
      'Malaysia' AS cntry_nm,
      evosif.dstrbtr_grp_cd,
      imcd.dstrbtr_grp_nm,
      NULL AS dstrbtr_cust_cd,
      NULL AS dstrbtr_cust_nm,
      CAST((
        LTRIM(CAST((
          imdd.cust_id
        ) AS TEXT), CAST((
          CAST('0' AS VARCHAR)
        ) AS TEXT))
      ) AS VARCHAR) AS sap_soldto_code,
      imdd.cust_nm AS sap_soldto_nm,
      imdd.lvl1 AS dstrbtr_lvl1,
      imdd.lvl2 AS dstrbtr_lvl2,
      imdd.lvl3 AS dstrbtr_lvl3,
      imdd.lvl4 AS dstrbtr_lvl4,
      imdd.lvl5 AS dstrbtr_lvl5,
      imdd.region AS region_nm,
      NULL AS town_nm,
      NULL AS slsmn_cd,
      NULL AS chnl_desc,
      NULL AS sub_chnl_desc,
      NULL AS chnl_attr1_desc,
      NULL AS chnl_attr2_desc,
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
      NULL AS wh_id,
      NULL AS doc_type,
      NULL AS doc_type_desc,
      0 AS base_sls,
      0 AS sls_qty,
      0 AS ret_qty,
      NULL AS uom,
      0 AS sls_qty_pc,
      0 AS ret_qty_pc,
      0 AS bill_qty_pc,
      0 AS in_transit_qty,
      lp.rate AS mat_list_price,
      0 AS grs_trd_sls,
      0 AS ret_val,
      0 AS trd_discnt,
      0 AS trd_sls,
      0 AS net_trd_sls,
      0 AS jj_grs_trd_sls,
      0 AS jj_ret_val,
      0 AS jj_trd_sls,
      0 AS jj_net_trd_sls,
      0 AS billing_grs_trd_sls,
      0 AS billing_subtot2,
      0 AS billing_subtot3,
      0 AS billing_subtot4,
      0 AS billing_net_amt,
      0 AS billing_est_nts,
      0 AS billing_invoice_val,
      0 AS billing_gross_val,
      0 AS in_transit_val,
      0 AS trgt_val,
      evosif.warehse_cd,
      evosif.end_stock_qty,
      (
        evosif.end_stock_val * veocurd.exch_rate
      ) AS end_stock_val_raw,
      (
        (
          evosif.end_stock_qty * veocurd.exch_rate
        ) * lp.rate
      ) AS end_stock_val,
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
        FROM edw_vw_my_curr_dim AS a
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
        FROM edw_vw_my_curr_dim AS a
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
                    a.year,
                    a.qrtr_no,
                    a.mnth_id,
                    a.mnth_no,
                    a.mnth_nm,
                    a.warehse_cd,
                    a.dstrbtr_grp_cd,
                    a.dstrbtr_soldto_code,
                    a.dstrbtr_matl_num,
                    a.sap_matl_num,
                    a.end_stock_qty,
                    a.end_stock_val
                  FROM (
                    SELECT
                      t1.year,
                      t1.qrtr_no,
                      t1.mnth_id,
                      t1.mnth_no,
                      t1.mnth_nm,
                      t1.warehse_cd,
                      t1.dstrbtr_grp_cd,
                      t1.dstrbtr_soldto_code,
                      t1.dstrbtr_matl_num,
                      t1.sap_matl_num,
                      SUM(t1.end_stock_qty) AS end_stock_qty,
                      SUM(t1.end_stock_val) AS end_stock_val
                    FROM (
                      SELECT
                        veotd.year,
                        veotd.qrtr_no,
                        veotd.mnth_id,
                        veotd.mnth_no,
                        veotd.mnth_nm,
                        inv.warehse_cd,
                        inv.dstrbtr_grp_cd,
                        inv.dstrbtr_soldto_code,
                        inv.dstrbtr_matl_num,
                        inv.sap_matl_num,
                        inv.inv_dt,
                        inv.end_stock_qty,
                        inv.end_stock_val
                      FROM edw_vw_my_sellout_inventory_fact AS inv, (
                        SELECT DISTINCT
                          edw_vw_os_time_dim.cal_year AS year,
                          edw_vw_os_time_dim.cal_qrtr_no AS qrtr_no,
                          edw_vw_os_time_dim.cal_mnth_id AS mnth_id,
                          edw_vw_os_time_dim.cal_mnth_no AS mnth_no,
                          edw_vw_os_time_dim.cal_mnth_nm AS mnth_nm,
                          edw_vw_os_time_dim.cal_date
                        FROM edw_vw_os_time_dim
                      ) AS veotd
                      WHERE
                        (
                          (
                            CAST((
                              veotd.cal_date
                            ) AS TIMESTAMPNTZ) = inv.inv_dt
                          )
                          AND (
                            CAST((
                              inv.cntry_cd
                            ) AS TEXT) = CAST((
                              CAST('MY' AS VARCHAR)
                            ) AS TEXT)
                          )
                        )
                    ) AS t1, (
                      SELECT DISTINCT
                        veotd.cal_year AS year,
                        veotd.cal_qrtr_no AS qrtr_no,
                        veotd.cal_mnth_id AS mnth_id,
                        veotd.cal_mnth_no AS mnth_no,
                        veotd.cal_mnth_nm AS mnth_nm,
                        MAX(t2.inv_dt) OVER (PARTITION BY veotd.cal_mnth_id order by null ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_inv_dt
                      FROM edw_vw_os_time_dim AS veotd, (
                        SELECT
                          edw_vw_my_sellout_inventory_fact.cntry_cd,
                          edw_vw_my_sellout_inventory_fact.cntry_nm,
                          edw_vw_my_sellout_inventory_fact.warehse_cd,
                          edw_vw_my_sellout_inventory_fact.dstrbtr_grp_cd,
                          edw_vw_my_sellout_inventory_fact.dstrbtr_soldto_code,
                          edw_vw_my_sellout_inventory_fact.dstrbtr_matl_num,
                          edw_vw_my_sellout_inventory_fact.sap_matl_num,
                          edw_vw_my_sellout_inventory_fact.inv_dt,
                          edw_vw_my_sellout_inventory_fact.soh,
                          edw_vw_my_sellout_inventory_fact.soh_val,
                          edw_vw_my_sellout_inventory_fact.jj_soh_val,
                          edw_vw_my_sellout_inventory_fact.beg_stock_qty,
                          edw_vw_my_sellout_inventory_fact.end_stock_qty,
                          edw_vw_my_sellout_inventory_fact.beg_stock_val,
                          edw_vw_my_sellout_inventory_fact.end_stock_val,
                          edw_vw_my_sellout_inventory_fact.jj_beg_stock_qty,
                          edw_vw_my_sellout_inventory_fact.jj_end_stock_qty,
                          edw_vw_my_sellout_inventory_fact.jj_beg_stock_val,
                          edw_vw_my_sellout_inventory_fact.jj_end_stock_val
                        FROM edw_vw_my_sellout_inventory_fact
                        WHERE
                          (
                            CAST((
                              edw_vw_my_sellout_inventory_fact.cntry_cd
                            ) AS TEXT) = CAST((
                              CAST('MY' AS VARCHAR)
                            ) AS TEXT)
                          )
                      ) AS t2
                      WHERE
                        (
                          CAST((
                            veotd.cal_date
                          ) AS TIMESTAMPNTZ) = t2.inv_dt
                        )
                    ) AS t3
                    WHERE
                      (
                        t3.max_inv_dt = t1.inv_dt
                      )
                    GROUP BY
                      t1.year,
                      t1.qrtr_no,
                      t1.mnth_id,
                      t1.mnth_no,
                      t1.mnth_nm,
                      t1.warehse_cd,
                      t1.dstrbtr_grp_cd,
                      t1.dstrbtr_soldto_code,
                      t1.dstrbtr_matl_num,
                      t1.sap_matl_num
                  ) AS a
                ) AS evosif
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
                      ) AS TEXT)) = LTRIM(
                        CAST((
                          evosif.dstrbtr_soldto_code
                        ) AS TEXT),
                        CAST((
                          CAST('0' AS VARCHAR)
                        ) AS TEXT)
                      )
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
                    ) AS TEXT)) = LTRIM(CAST((
                      evosif.sap_matl_num
                    ) AS TEXT), CAST((
                      CAST('0' AS VARCHAR)
                    ) AS TEXT))
                  )
                )
            )
            LEFT JOIN snaposeitg_integration.itg_my_dstrbtrr_dim AS imdd
              ON (
                (
                  LTRIM(CAST((
                    imdd.cust_id
                  ) AS TEXT), CAST((
                    CAST('0' AS VARCHAR)
                  ) AS TEXT)) = LTRIM(
                    CAST((
                      evosif.dstrbtr_soldto_code
                    ) AS TEXT),
                    CAST((
                      CAST('0' AS VARCHAR)
                    ) AS TEXT)
                  )
                )
              )
          )
          LEFT JOIN snaposeitg_integration.itg_my_material_dim AS immd
            ON (
              (
                LTRIM(CAST((
                  immd.item_cd
                ) AS TEXT), CAST((
                  CAST('0' AS VARCHAR)
                ) AS TEXT)) = LTRIM(CAST((
                  evosif.sap_matl_num
                ) AS TEXT), CAST((
                  CAST('0' AS VARCHAR)
                ) AS TEXT))
              )
            )
        )
        LEFT JOIN snaposeitg_integration.itg_my_customer_dim AS imcd
          ON (
            (
              LTRIM(CAST((
                imcd.cust_id
              ) AS TEXT), CAST((
                CAST('0' AS VARCHAR)
              ) AS TEXT)) = LTRIM(
                CAST((
                  evosif.dstrbtr_soldto_code
                ) AS TEXT),
                CAST((
                  CAST('0' AS VARCHAR)
                ) AS TEXT)
              )
            )
          )
      )
      LEFT JOIN (
        SELECT
          edw_vw_my_listprice.cntry_key,
          edw_vw_my_listprice.cntry_nm,
          edw_vw_my_listprice.plant,
          edw_vw_my_listprice.cnty,
          edw_vw_my_listprice.item_cd,
          edw_vw_my_listprice.item_desc,
          edw_vw_my_listprice.valid_from,
          edw_vw_my_listprice.valid_to,
          edw_vw_my_listprice.rate,
          edw_vw_my_listprice.currency,
          edw_vw_my_listprice.price_unit,
          edw_vw_my_listprice.uom,
          edw_vw_my_listprice.yearmo,
          edw_vw_my_listprice.mnth_type,
          edw_vw_my_listprice.snapshot_dt
        FROM edw_vw_my_listprice
        WHERE
          (
            (
              CAST((
                edw_vw_my_listprice.cntry_key
              ) AS TEXT) = CAST((
                CAST('MY' AS VARCHAR)
              ) AS TEXT)
            )
            AND (
              CAST((
                edw_vw_my_listprice.mnth_type
              ) AS TEXT) = CAST((
                CAST('CAL' AS VARCHAR)
              ) AS TEXT)
            )
          )
      ) AS lp
        ON (
          (
            (
              LTRIM(CAST((
                lp.item_cd
              ) AS TEXT), CAST((
                CAST('0' AS VARCHAR)
              ) AS TEXT)) = LTRIM(CAST((
                evosif.sap_matl_num
              ) AS TEXT), CAST((
                CAST('0' AS VARCHAR)
              ) AS TEXT))
            )
            AND (
              CAST((
                lp.yearmo
              ) AS TEXT) = CAST((
                CAST((
                  evosif.mnth_id
                ) AS VARCHAR)
              ) AS TEXT)
            )
          )
        )
    )
    WHERE
      (
        (
          CAST((
            CAST((
              evosif.mnth_id
            ) AS VARCHAR)
          ) AS TEXT) >= veocurd.start_period
        )
        AND (
          CAST((
            CAST((
              evosif.mnth_id
            ) AS VARCHAR)
          ) AS TEXT) <= veocurd.end_period
        )
      )
  )
  UNION ALL
  SELECT
    'GT Target' AS data_src,
    veotd.year,
    veotd.qrtr_no,
    veotd.mnth_id,
    veotd.mnth_no,
    veotd.mnth_nm,
    NULL AS max_yearmo,
    'Malaysia' AS cntry_nm,
    evodcd.dstrbtr_grp_cd,
    imcd.dstrbtr_grp_nm,
    NULL AS dstrbtr_cust_cd,
    NULL AS dstrbtr_cust_nm,
    CAST((
      LTRIM(CAST((
        imdd.cust_id
      ) AS TEXT), CAST((
        CAST('0' AS VARCHAR)
      ) AS TEXT))
    ) AS VARCHAR) AS sap_soldto_code,
    imdd.cust_nm AS sap_soldto_nm,
    imdd.lvl1 AS dstrbtr_lvl1,
    imdd.lvl2 AS dstrbtr_lvl2,
    imdd.lvl3 AS dstrbtr_lvl3,
    imdd.lvl4 AS dstrbtr_lvl4,
    imdd.lvl5 AS dstrbtr_lvl5,
    imdd.region AS region_nm,
    NULL AS town_nm,
    NULL AS slsmn_cd,
    NULL AS chnl_desc,
    NULL AS sub_chnl_desc,
    NULL AS chnl_attr1_desc,
    NULL AS chnl_attr2_desc,
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
    NULL AS sku,
    NULL AS frnchse_desc,
    NULL AS brnd_desc,
    NULL AS vrnt_desc,
    NULL AS putup_desc,
    NULL AS item_desc2,
    NULL AS sku_desc,
    NULL AS sap_mat_type_cd,
    NULL AS sap_mat_type_desc,
    NULL AS sap_base_uom_cd,
    NULL AS sap_prchse_uom_cd,
    NULL AS sap_prod_sgmt_cd,
    NULL AS sap_prod_sgmt_desc,
    NULL AS sap_base_prod_cd,
    NULL AS sap_base_prod_desc,
    NULL AS sap_mega_brnd_cd,
    NULL AS sap_mega_brnd_desc,
    NULL AS sap_brnd_cd,
    NULL AS sap_brnd_desc,
    NULL AS sap_vrnt_cd,
    NULL AS sap_vrnt_desc,
    NULL AS sap_put_up_cd,
    NULL AS sap_put_up_desc,
    NULL AS sap_grp_frnchse_cd,
    NULL AS sap_grp_frnchse_desc,
    NULL AS sap_frnchse_cd,
    NULL AS sap_frnchse_desc,
    NULL AS sap_prod_frnchse_cd,
    NULL AS sap_prod_frnchse_desc,
    NULL AS sap_prod_mjr_cd,
    NULL AS sap_prod_mjr_desc,
    NULL AS sap_prod_mnr_cd,
    NULL AS sap_prod_mnr_desc,
    NULL AS sap_prod_hier_cd,
    NULL AS sap_prod_hier_desc,
    NULL AS global_mat_region,
    CASE
      WHEN (
        CAST((
          imt.sub_segment
        ) AS TEXT) = CAST((
          CAST('ALL' AS VARCHAR)
        ) AS TEXT)
      )
      THEN veomd.gph_prod_frnchse
      ELSE veomd1.gph_prod_frnchse
    END AS global_prod_franchise,
    CASE
      WHEN (
        CAST((
          imt.sub_segment
        ) AS TEXT) = CAST((
          CAST('ALL' AS VARCHAR)
        ) AS TEXT)
      )
      THEN veomd.gph_prod_brnd
      ELSE veomd1.gph_prod_brnd
    END AS global_prod_brand,
    NULL AS global_prod_variant,
    NULL AS global_prod_put_up_cd,
    NULL AS global_put_up_desc,
    NULL AS global_prod_sub_brand,
    NULL AS global_prod_need_state,
    NULL AS global_prod_category,
    NULL AS global_prod_subcategory,
    NULL AS global_prod_segment,
    veomd1.gph_prod_subsgmnt AS global_prod_subsegment,
    NULL AS global_prod_size,
    NULL AS global_prod_size_uom,
    veocurd.from_ccy,
    veocurd.to_ccy,
    veocurd.exch_rate,
    NULL AS wh_id,
    NULL AS doc_type,
    NULL AS doc_type_desc,
    0 AS base_sls,
    0 AS sls_qty,
    0 AS ret_qty,
    NULL AS uom,
    0 AS sls_qty_pc,
    0 AS ret_qty_pc,
    0 AS bill_qty_pc,
    0 AS in_transit_qty,
    0 AS mat_list_price,
    0 AS grs_trd_sls,
    0 AS ret_val,
    0 AS trd_discnt,
    0 AS trd_sls,
    0 AS net_trd_sls,
    0 AS jj_grs_trd_sls,
    0 AS jj_ret_val,
    0 AS jj_trd_sls,
    0 AS jj_net_trd_sls,
    0 AS billing_grs_trd_sls,
    0 AS billing_subtot2,
    0 AS billing_subtot3,
    0 AS billing_subtot4,
    0 AS billing_net_amt,
    0 AS billing_est_nts,
    0 AS billing_invoice_val,
    0 AS billing_gross_val,
    0 AS in_transit_val,
    (
      imt.trgt_val * veocurd.exch_rate
    ) AS trgt_val,
    NULL AS warehse_cd,
    0 AS end_stock_qty,
    0 AS end_stock_val_raw,
    0 AS end_stock_val,
    NULL AS is_npi,
    NULL AS npi_str_period,
    NULL AS npi_end_period,
    NULL AS is_reg,
    NULL AS is_promo,
    NULL AS promo_strt_period,
    NULL AS promo_end_period,
    NULL AS is_mcl,
    NULL AS is_hero
  FROM (
    SELECT
      edw_vw_os_time_dim.cal_year AS year,
      edw_vw_os_time_dim.cal_qrtr_no AS qrtr_no,
      edw_vw_os_time_dim.cal_mnth_id AS mnth_id,
      edw_vw_os_time_dim.cal_mnth_no AS mnth_no,
      edw_vw_os_time_dim.cal_mnth_nm AS mnth_nm
    FROM edw_vw_os_time_dim
    GROUP BY
      edw_vw_os_time_dim.cal_year,
      edw_vw_os_time_dim.cal_qrtr_no,
      edw_vw_os_time_dim.cal_mnth_id,
      edw_vw_os_time_dim.cal_mnth_no,
      edw_vw_os_time_dim.cal_mnth_nm
  ) AS veotd, (
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
      FROM edw_vw_my_curr_dim AS a
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
      FROM edw_vw_my_curr_dim AS a
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
              snaposeitg_integration.itg_my_trgts AS imt
                LEFT JOIN snaposeitg_integration.itg_my_dstrbtrr_dim AS imdd
                  ON (
                    (
                      LTRIM(CAST((
                        imdd.cust_id
                      ) AS TEXT), CAST((
                        CAST('0' AS VARCHAR)
                      ) AS TEXT)) = LTRIM(CAST((
                        imt.cust_id
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
                  LTRIM(CAST((
                    veocd.sap_cust_id
                  ) AS TEXT), CAST((
                    CAST('0' AS VARCHAR)
                  ) AS TEXT)) = LTRIM(CAST((
                    imt.cust_id
                  ) AS TEXT), CAST((
                    CAST('0' AS VARCHAR)
                  ) AS TEXT))
                )
              )
          )
          LEFT JOIN (
            SELECT DISTINCT
              edw_vw_my_material_dim.gph_prod_frnchse,
              edw_vw_my_material_dim.gph_prod_brnd
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
              CASE
                WHEN (
                  CAST((
                    imt.sub_segment
                  ) AS TEXT) = CAST((
                    CAST('ALL' AS VARCHAR)
                  ) AS TEXT)
                )
                THEN (
                  TRIM(UPPER(CAST((
                    veomd.gph_prod_brnd
                  ) AS TEXT))) = TRIM(UPPER(CAST((
                    imt.brnd_desc
                  ) AS TEXT)))
                )
                ELSE CAST(NULL AS BOOLEAN)
              END
            )
        )
        LEFT JOIN (
          SELECT DISTINCT
            edw_vw_my_material_dim.gph_prod_frnchse,
            edw_vw_my_material_dim.gph_prod_brnd,
            edw_vw_my_material_dim.gph_prod_subsgmnt
          FROM edw_vw_my_material_dim
          WHERE
            (
              CAST((
                edw_vw_my_material_dim.cntry_key
              ) AS TEXT) = CAST((
                CAST('MY' AS VARCHAR)
              ) AS TEXT)
            )
        ) AS veomd1
          ON (
            CASE
              WHEN (
                CAST((
                  imt.sub_segment
                ) AS TEXT) <> CAST((
                  CAST('ALL' AS VARCHAR)
                ) AS TEXT)
              )
              THEN (
                (
                  TRIM(UPPER(CAST((
                    veomd1.gph_prod_subsgmnt
                  ) AS TEXT))) = TRIM(UPPER(CAST((
                    imt.sub_segment
                  ) AS TEXT)))
                )
                AND (
                  TRIM(UPPER(CAST((
                    veomd1.gph_prod_brnd
                  ) AS TEXT))) = TRIM(UPPER(CAST((
                    imt.brnd_desc
                  ) AS TEXT)))
                )
              )
              ELSE CAST(NULL AS BOOLEAN)
            END
          )
      )
      LEFT JOIN (
        SELECT
          edw_vw_my_dstrbtr_customer_dim.cntry_cd,
          edw_vw_my_dstrbtr_customer_dim.cntry_nm,
          edw_vw_my_dstrbtr_customer_dim.dstrbtr_grp_cd,
          edw_vw_my_dstrbtr_customer_dim.dstrbtr_soldto_code,
          edw_vw_my_dstrbtr_customer_dim.sap_soldto_code,
          edw_vw_my_dstrbtr_customer_dim.cust_cd,
          edw_vw_my_dstrbtr_customer_dim.cust_nm,
          edw_vw_my_dstrbtr_customer_dim.alt_cust_cd,
          edw_vw_my_dstrbtr_customer_dim.alt_cust_nm,
          edw_vw_my_dstrbtr_customer_dim.addr,
          edw_vw_my_dstrbtr_customer_dim.area_cd,
          edw_vw_my_dstrbtr_customer_dim.area_nm,
          edw_vw_my_dstrbtr_customer_dim.state_cd,
          edw_vw_my_dstrbtr_customer_dim.state_nm,
          edw_vw_my_dstrbtr_customer_dim.region_cd,
          edw_vw_my_dstrbtr_customer_dim.region_nm,
          edw_vw_my_dstrbtr_customer_dim.prov_cd,
          edw_vw_my_dstrbtr_customer_dim.prov_nm,
          edw_vw_my_dstrbtr_customer_dim.town_cd,
          edw_vw_my_dstrbtr_customer_dim.town_nm,
          edw_vw_my_dstrbtr_customer_dim.city_cd,
          edw_vw_my_dstrbtr_customer_dim.city_nm,
          edw_vw_my_dstrbtr_customer_dim.post_cd,
          edw_vw_my_dstrbtr_customer_dim.post_nm,
          edw_vw_my_dstrbtr_customer_dim.slsmn_cd,
          edw_vw_my_dstrbtr_customer_dim.slsmn_nm,
          edw_vw_my_dstrbtr_customer_dim.chnl_cd,
          edw_vw_my_dstrbtr_customer_dim.chnl_desc,
          edw_vw_my_dstrbtr_customer_dim.sub_chnl_cd,
          edw_vw_my_dstrbtr_customer_dim.sub_chnl_desc,
          edw_vw_my_dstrbtr_customer_dim.chnl_attr1_cd,
          edw_vw_my_dstrbtr_customer_dim.chnl_attr1_desc,
          edw_vw_my_dstrbtr_customer_dim.chnl_attr2_cd,
          edw_vw_my_dstrbtr_customer_dim.chnl_attr2_desc,
          edw_vw_my_dstrbtr_customer_dim.outlet_type_cd,
          edw_vw_my_dstrbtr_customer_dim.outlet_type_desc,
          edw_vw_my_dstrbtr_customer_dim.cust_grp_cd,
          edw_vw_my_dstrbtr_customer_dim.cust_grp_desc,
          edw_vw_my_dstrbtr_customer_dim.cust_grp_attr1_cd,
          edw_vw_my_dstrbtr_customer_dim.cust_grp_attr1_desc,
          edw_vw_my_dstrbtr_customer_dim.cust_grp_attr2_cd,
          edw_vw_my_dstrbtr_customer_dim.cust_grp_attr2_desc,
          edw_vw_my_dstrbtr_customer_dim.sls_dstrct_cd,
          edw_vw_my_dstrbtr_customer_dim.sls_dstrct_nm,
          edw_vw_my_dstrbtr_customer_dim.sls_office_cd,
          edw_vw_my_dstrbtr_customer_dim.sls_office_desc,
          edw_vw_my_dstrbtr_customer_dim.sls_grp_cd,
          edw_vw_my_dstrbtr_customer_dim.sls_grp_desc,
          edw_vw_my_dstrbtr_customer_dim.status
        FROM edw_vw_my_dstrbtr_customer_dim
        WHERE
          (
            CAST((
              edw_vw_my_dstrbtr_customer_dim.cntry_cd
            ) AS TEXT) = CAST((
              CAST('MY' AS VARCHAR)
            ) AS TEXT)
          )
      ) AS evodcd
        ON (
          (
            LTRIM(CAST((
              evodcd.cust_cd
            ) AS TEXT), CAST((
              CAST('0' AS VARCHAR)
            ) AS TEXT)) = LTRIM(CAST((
              imt.cust_id
            ) AS TEXT), CAST((
              CAST('0' AS VARCHAR)
            ) AS TEXT))
          )
        )
    )
    LEFT JOIN snaposeitg_integration.itg_my_customer_dim AS imcd
      ON (
        (
          LTRIM(CAST((
            imcd.cust_id
          ) AS TEXT), CAST((
            CAST('0' AS VARCHAR)
          ) AS TEXT)) = LTRIM(CAST((
            imt.cust_id
          ) AS TEXT), CAST((
            CAST('0' AS VARCHAR)
          ) AS TEXT))
        )
      )
  )
  WHERE
    (
      (
        (
          (
            CAST((
              imt.trgt_type
            ) AS TEXT) = CAST((
              CAST('BP' AS VARCHAR)
            ) AS TEXT)
          )
          AND (
            CAST((
              CAST((
                veotd.mnth_id
              ) AS VARCHAR)
            ) AS TEXT) = CAST((
              imt.jj_mnth_id
            ) AS TEXT)
          )
        )
        AND (
          CAST((
            imt.jj_mnth_id
          ) AS TEXT) >= veocurd.start_period
        )
      )
      AND (
        CAST((
          imt.jj_mnth_id
        ) AS TEXT) <= veocurd.end_period
      )
    )
)
UNION ALL
SELECT
  'IN Transit' AS data_src,
  veoint.year,
  veoint.qrtr_no,
  veoint.mnth_id,
  veoint.mnth_no,
  veoint.mnth_nm,
  NULL AS max_yearmo,
  'Malaysia' AS cntry_nm,
  imcd.dstrbtr_grp_cd,
  imcd.dstrbtr_grp_nm,
  NULL AS dstrbtr_cust_cd,
  NULL AS dstrbtr_cust_nm,
  CAST((
    LTRIM(CAST((
      imdd.cust_id
    ) AS TEXT), CAST((
      CAST('0' AS VARCHAR)
    ) AS TEXT))
  ) AS VARCHAR) AS sap_soldto_code,
  imdd.cust_nm AS sap_soldto_nm,
  imdd.lvl1 AS dstrbtr_lvl1,
  imdd.lvl2 AS dstrbtr_lvl2,
  imdd.lvl3 AS dstrbtr_lvl3,
  imdd.lvl4 AS dstrbtr_lvl4,
  imdd.lvl5 AS dstrbtr_lvl5,
  imdd.region AS region_nm,
  NULL AS town_nm,
  NULL AS slsmn_cd,
  NULL AS chnl_desc,
  NULL AS sub_chnl_desc,
  NULL AS chnl_attr1_desc,
  NULL AS chnl_attr2_desc,
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
  NULL AS wh_id,
  NULL AS doc_type,
  NULL AS doc_type_desc,
  0 AS base_sls,
  0 AS sls_qty,
  0 AS ret_qty,
  NULL AS uom,
  0 AS sls_qty_pc,
  0 AS ret_qty_pc,
  0 AS bill_qty_pc,
  veoint.bill_qty_pc AS in_transit_qty,
  0 AS mat_list_price,
  0 AS grs_trd_sls,
  0 AS ret_val,
  0 AS trd_discnt,
  0 AS trd_sls,
  0 AS net_trd_sls,
  0 AS jj_grs_trd_sls,
  0 AS jj_ret_val,
  0 AS jj_trd_sls,
  0 AS jj_net_trd_sls,
  0 AS billing_grs_trd_sls,
  0 AS billing_subtot2,
  0 AS billing_subtot3,
  0 AS billing_subtot4,
  0 AS billing_net_amt,
  0 AS billing_est_nts,
  0 AS billing_invoice_val,
  0 AS billing_gross_val,
  (
    veoint.billing_gross_val * veocurd.exch_rate
  ) AS in_transit_val,
  0 AS trgt_val,
  NULL AS warehse_cd,
  0 AS end_stock_qty,
  0 AS end_stock_val_raw,
  0 AS end_stock_val,
  NULL AS is_npi,
  NULL AS npi_str_period,
  NULL AS npi_end_period,
  NULL AS is_reg,
  NULL AS is_promo,
  NULL AS promo_strt_period,
  NULL AS promo_end_period,
  NULL AS is_mcl,
  NULL AS is_hero
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
    FROM edw_vw_my_curr_dim AS a
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
    FROM edw_vw_my_curr_dim AS a
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
              a.bill_doc,
              b.bill_num,
              b.sold_to,
              b.matl_num,
              b.bill_qty_pc,
              b.billing_gross_val,
              veotd.year,
              veotd.qrtr_no,
              veotd.mnth_id,
              veotd.mnth_no,
              veotd.mnth_nm
            FROM itg_my_in_transit AS a, (
              SELECT
                edw_vw_my_billing_fact.bill_num,
                edw_vw_my_billing_fact.sold_to,
                edw_vw_my_billing_fact.matl_num,
                SUM(edw_vw_my_billing_fact.bill_qty_pc) AS bill_qty_pc,
                SUM(
                  (
                    edw_vw_my_billing_fact.grs_trd_sls * ABS(edw_vw_my_billing_fact.exchg_rate)
                  )
                ) AS billing_gross_val
              FROM edw_vw_my_billing_fact
              WHERE
                (
                  edw_vw_my_billing_fact.cntry_key = CAST((
                    CAST('MY' AS VARCHAR)
                  ) AS TEXT)
                )
              GROUP BY
                edw_vw_my_billing_fact.bill_num,
                edw_vw_my_billing_fact.sold_to,
                edw_vw_my_billing_fact.matl_num
            ) AS b, (
              SELECT
                edw_vw_os_time_dim.cal_year AS year,
                edw_vw_os_time_dim.cal_qrtr_no AS qrtr_no,
                edw_vw_os_time_dim.cal_mnth_id AS mnth_id,
                edw_vw_os_time_dim.cal_mnth_no AS mnth_no,
                edw_vw_os_time_dim.cal_mnth_nm AS mnth_nm,
                edw_vw_os_time_dim.cal_date
              FROM edw_vw_os_time_dim
              GROUP BY
                edw_vw_os_time_dim.cal_year,
                edw_vw_os_time_dim.cal_qrtr_no,
                edw_vw_os_time_dim.cal_mnth_id,
                edw_vw_os_time_dim.cal_mnth_no,
                edw_vw_os_time_dim.cal_mnth_nm,
                edw_vw_os_time_dim.cal_date
            ) AS veotd
            WHERE
              (
                (
                  b.bill_num = CAST((
                    a.bill_doc
                  ) AS TEXT)
                )
                AND (
                  a.closing_dt = veotd.cal_date
                )
              )
          ) AS veoint
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
                ) AS TEXT)) = LTRIM(veoint.sold_to, CAST((
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
              ) AS TEXT)) = LTRIM(veoint.matl_num, CAST((
                CAST('0' AS VARCHAR)
              ) AS TEXT))
            )
          )
      )
      LEFT JOIN snaposeitg_integration.itg_my_dstrbtrr_dim AS imdd
        ON (
          (
            LTRIM(CAST((
              imdd.cust_id
            ) AS TEXT), CAST((
              CAST('0' AS VARCHAR)
            ) AS TEXT)) = LTRIM(veoint.sold_to, CAST((
              CAST('0' AS VARCHAR)
            ) AS TEXT))
          )
        )
    )
    LEFT JOIN snaposeitg_integration.itg_my_material_dim AS immd
      ON (
        (
          LTRIM(CAST((
            immd.item_cd
          ) AS TEXT), CAST((
            CAST('0' AS VARCHAR)
          ) AS TEXT)) = LTRIM(veoint.matl_num, CAST((
            CAST('0' AS VARCHAR)
          ) AS TEXT))
        )
      )
  )
  LEFT JOIN snaposeitg_integration.itg_my_customer_dim AS imcd
    ON (
      (
        LTRIM(CAST((
          imcd.cust_id
        ) AS TEXT), CAST((
          CAST('0' AS VARCHAR)
        ) AS TEXT)) = LTRIM(veoint.sold_to, CAST((
          CAST('0' AS VARCHAR)
        ) AS TEXT))
      )
    )
)
WHERE
  (
    (
      CAST((
        CAST((
          veoint.mnth_id
        ) AS VARCHAR)
      ) AS TEXT) >= veocurd.start_period
    )
    AND (
      CAST((
        CAST((
          veoint.mnth_id
        ) AS VARCHAR)
      ) AS TEXT) <= veocurd.end_period
    )
  )
)

select * from transformed