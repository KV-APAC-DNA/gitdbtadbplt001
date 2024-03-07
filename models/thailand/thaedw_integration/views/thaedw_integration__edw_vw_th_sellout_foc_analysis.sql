with edw_vw_th_sellout_sales_foc_fact  as (
  SELECT * from DEV_DNA_CORE.SNAPOSEEDW_INTEGRATION.EDW_VW_TH_SELLOUT_SALES_FOC_FACT
),
edw_vw_os_time_dim as (
  select * from DEV_DNA_CORE.SNAPOSEEDW_INTEGRATION.EDW_VW_OS_TIME_DIM
),
edw_vw_os_dstrbtr_customer_dim as (
  select * from DEV_DNA_CORE.SNAPOSEEDW_INTEGRATION.EDW_VW_OS_DSTRBTR_CUSTOMER_DIM
),
edw_vw_os_dstrbtr_material_dim as (
  select * from DEV_DNA_CORE.SNAPOSEEDW_INTEGRATION.EDW_VW_OS_DSTRBTR_MATERIAL_DIM
),
itg_th_target_distribution as (
  select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_TH_TARGET_DISTRIBUTION
),
itg_th_productgrouping as (
  select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_TH_PRODUCTGROUPING
),
itg_th_target_sales as (
  select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_TH_TARGET_SALES
),
edw_vw_os_customer_dim as (
  select * from DEV_DNA_CORE.SNAPOSEEDW_INTEGRATION.EDW_VW_OS_CUSTOMER_DIM
),
edw_vw_os_material_dim as (
  select * from DEV_DNA_CORE.SNAPOSEEDW_INTEGRATION.EDW_VW_OS_MATERIAL_DIM
),

so_matl as 
(
    SELECT DISTINCT
      edw_vw_os_dstrbtr_material_dim.sap_matl_num,
      edw_vw_os_dstrbtr_material_dim.dstrbtr_bar_cd
    FROM edw_vw_os_dstrbtr_material_dim
    WHERE
      (
        CAST((
          edw_vw_os_dstrbtr_material_dim.cntry_cd
        ) AS TEXT) = CAST((
          CAST('TH' AS VARCHAR)
        ) AS TEXT)
      )
  ) ,
 si_matl as (
      SELECT DISTINCT
        edw_vw_os_material_dim.cntry_key,
        edw_vw_os_material_dim.sap_matl_num,
        edw_vw_os_material_dim.sap_mat_desc,
        edw_vw_os_material_dim.gph_region,
        edw_vw_os_material_dim.gph_prod_frnchse,
        edw_vw_os_material_dim.gph_prod_brnd,
        edw_vw_os_material_dim.gph_prod_vrnt,
        edw_vw_os_material_dim.gph_prod_sgmnt,
        edw_vw_os_material_dim.gph_prod_put_up_desc,
        edw_vw_os_material_dim.gph_prod_sub_brnd AS prod_sub_brand,
        edw_vw_os_material_dim.gph_prod_subsgmnt AS prod_subsegment,
        edw_vw_os_material_dim.gph_prod_ctgry AS prod_category,
        edw_vw_os_material_dim.gph_prod_subctgry AS prod_subcategory
      FROM edw_vw_os_material_dim AS edw_vw_os_material_dim
      WHERE
        (
          CAST((
            edw_vw_os_material_dim.cntry_key
          ) AS TEXT) = CAST((
            CAST('TH' AS VARCHAR)
          ) AS TEXT)
        )
    ) ,
sellin_cust as (
        SELECT
          sellin_cust.sap_cust_id,
          sellin_cust.sap_cust_nm,
          sellin_cust.sap_sls_org,
          sellin_cust.sap_cmp_id,
          sellin_cust.sap_cntry_cd,
          sellin_cust.sap_cntry_nm,
          sellin_cust.sap_addr,
          sellin_cust.sap_region,
          sellin_cust.sap_state_cd,
          sellin_cust.sap_city,
          sellin_cust.sap_post_cd,
          sellin_cust.sap_chnl_cd,
          sellin_cust.sap_chnl_desc,
          sellin_cust.sap_sls_office_cd,
          sellin_cust.sap_sls_office_desc,
          sellin_cust.sap_sls_grp_cd,
          sellin_cust.sap_sls_grp_desc,
          sellin_cust.sap_prnt_cust_key,
          sellin_cust.sap_prnt_cust_desc,
          sellin_cust.sap_cust_chnl_key,
          sellin_cust.sap_cust_chnl_desc,
          sellin_cust.sap_cust_sub_chnl_key,
          sellin_cust.sap_sub_chnl_desc,
          sellin_cust.sap_go_to_mdl_key,
          sellin_cust.sap_go_to_mdl_desc,
          sellin_cust.sap_bnr_key,
          sellin_cust.sap_bnr_desc,
          sellin_cust.sap_bnr_frmt_key,
          sellin_cust.sap_bnr_frmt_desc,
          sellin_cust.retail_env
        FROM edw_vw_os_customer_dim AS sellin_cust
        WHERE
          (
            CAST((
              sellin_cust.sap_cntry_cd
            ) AS TEXT) = CAST((
              CAST('TH' AS VARCHAR)
            ) AS TEXT)
          )
      ),
target_sales as (
          SELECT
            itg_th_target_sales.dstrbtr_id,
            itg_th_target_sales.sls_office,
            itg_th_target_sales.sls_grp,
            itg_th_target_sales.target,
            itg_th_target_sales.period
          FROM itg_th_target_sales
        ) ,
target_distribution as (
            SELECT
              "target".dstrbtr_id,
              "target".period,
              "target".target,
              prodgroup.prod_cd
            FROM itg_th_target_distribution AS "target", itg_th_productgrouping AS prodgroup
            WHERE
              (
                UPPER(CAST((
                  "target".prod_nm
                ) AS TEXT)) = UPPER(CAST((
                  prodgroup.prod_grp
                ) AS TEXT))
              )
          ) ,
sellout_cust as (
              SELECT DISTINCT
                edw_vw_os_dstrbtr_customer_dim.region_nm,
                edw_vw_os_dstrbtr_customer_dim.prov_nm,
                edw_vw_os_dstrbtr_customer_dim.city_nm,
                edw_vw_os_dstrbtr_customer_dim.cust_nm,
                edw_vw_os_dstrbtr_customer_dim.chnl_cd,
                edw_vw_os_dstrbtr_customer_dim.chnl_desc,
                edw_vw_os_dstrbtr_customer_dim.sls_office_cd,
                edw_vw_os_dstrbtr_customer_dim.sls_grp_cd,
                edw_vw_os_dstrbtr_customer_dim.cust_grp_cd,
                edw_vw_os_dstrbtr_customer_dim.outlet_type_cd,
                edw_vw_os_dstrbtr_customer_dim.outlet_type_desc,
                edw_vw_os_dstrbtr_customer_dim.cust_cd,
                edw_vw_os_dstrbtr_customer_dim.dstrbtr_grp_cd,
                edw_vw_os_dstrbtr_customer_dim.sap_soldto_code
              FROM edw_vw_os_dstrbtr_customer_dim
              WHERE
                (
                  CAST((
                    edw_vw_os_dstrbtr_customer_dim.cntry_cd
                  ) AS TEXT) = CAST((
                    CAST('TH' AS VARCHAR)
                  ) AS TEXT)
                )
            ) 
,
"time" as  (
                  SELECT DISTINCT
                    edw_vw_os_time_dim.year,
                    edw_vw_os_time_dim.qrtr,
                    edw_vw_os_time_dim.mnth_id,
                    edw_vw_os_time_dim.mnth_no,
                    edw_vw_os_time_dim.wk,
                    edw_vw_os_time_dim.mnth_wk_no,
                    edw_vw_os_time_dim.cal_date
                  FROM edw_vw_os_time_dim AS edw_vw_os_time_dim
                  WHERE
                    (
                      
                        edw_vw_os_time_dim.year > (DATE_PART(YEAR,CURRENT_TIMESTAMP()) - 3)
                      
                      OR (
                        edw_vw_os_time_dim.year > (DATE_PART(YEAR,CURRENT_TIMESTAMP()) - 3)
                      )
                    )
                ) ,
sales as (
  (
              SELECT
                sales.cntry_cd,
                sales.cntry_nm,
                "time".year,
                "time".qrtr,
                "time".mnth_id,
                "time".mnth_no,
                "time".wk,
                "time".mnth_wk_no,
                "time".cal_date,
                sales.bill_date,
                sales.order_no,
                sales.iscancel,
                sales.grp_cd,
                sales.prom_cd1,
                sales.prom_cd2,
                sales.prom_cd3,
                sales.dstrbtr_grp_cd,
                sales.dstrbtr_matl_num,
                sales.cust_cd,
                sales.slsmn_nm,
                sales.slsmn_cd,
                sales.cn_reason_cd,
                sales.cn_reason_desc,
                sales.grs_prc,
                sales.grs_trd_sls,
                sales.cn_dmgd_gds,
                sales.crdt_nt_amt,
                sales.trd_discnt_item_lvl,
                sales.trd_discnt_bill_lvl,
                sales.sls_qty,
                sales.ret_qty,
                sales.quantity_dz,
                sales.net_trd_sls,
                sales.tot_bf_discount,
                sales.product_name2
              FROM (
                (
                  SELECT
                    edw_vw_th_sellout_sales_foc_fact.cntry_cd,
                    edw_vw_th_sellout_sales_foc_fact.cntry_nm,
                    edw_vw_th_sellout_sales_foc_fact.bill_date,
                    edw_vw_th_sellout_sales_foc_fact.order_no,
                    edw_vw_th_sellout_sales_foc_fact.product_name2,
                    edw_vw_th_sellout_sales_foc_fact.iscancel,
                    edw_vw_th_sellout_sales_foc_fact.grp_cd,
                    edw_vw_th_sellout_sales_foc_fact.prom_cd1,
                    edw_vw_th_sellout_sales_foc_fact.prom_cd2,
                    edw_vw_th_sellout_sales_foc_fact.prom_cd3,
                    edw_vw_th_sellout_sales_foc_fact.dstrbtr_grp_cd,
                    edw_vw_th_sellout_sales_foc_fact.dstrbtr_matl_num,
                    edw_vw_th_sellout_sales_foc_fact.cust_cd,
                    edw_vw_th_sellout_sales_foc_fact.slsmn_nm,
                    edw_vw_th_sellout_sales_foc_fact.slsmn_cd,
                    edw_vw_th_sellout_sales_foc_fact.cn_reason_cd,
                    edw_vw_th_sellout_sales_foc_fact.cn_reason_desc,
                    edw_vw_th_sellout_sales_foc_fact.grs_prc,
                    SUM(
                      CASE
                        WHEN (
                          (
                            edw_vw_th_sellout_sales_foc_fact.cn_reason_cd IS NULL
                          )
                          OR (
                            LEFT(CAST((
                              edw_vw_th_sellout_sales_foc_fact.cn_reason_cd
                            ) AS TEXT), 1) <> CAST((
                              CAST('N' AS VARCHAR)
                            ) AS TEXT)
                          )
                        )
                        THEN CAST((
                          (
                            edw_vw_th_sellout_sales_foc_fact.grs_trd_sls + edw_vw_th_sellout_sales_foc_fact.ret_val
                          )
                        ) AS DOUBLE)
                        ELSE CAST(NULL AS DOUBLE)
                      END
                    ) AS grs_trd_sls,
                    SUM(
                      CASE
                        WHEN (
                          (
                            edw_vw_th_sellout_sales_foc_fact.cn_reason_cd IS NULL
                          )
                          OR (
                            CAST((
                              edw_vw_th_sellout_sales_foc_fact.cn_reason_cd
                            ) AS TEXT) = CAST((
                              CAST('' AS VARCHAR)
                            ) AS TEXT)
                          )
                        )
                        THEN CAST((
                          0.0
                        ) AS DOUBLE)
                        WHEN (
                          
                              edw_vw_th_sellout_sales_foc_fact.cn_reason_cd
                            like 'D%' 
                          )
                        
                        THEN CAST((
                          0.0
                        ) AS DOUBLE)
                        WHEN (
                         
                              edw_vw_th_sellout_sales_foc_fact.cn_reason_cd
                            like 'N%' 
                          )
                        
                        THEN CAST((
                          edw_vw_th_sellout_sales_foc_fact.net_trd_sls
                        ) AS DOUBLE)
                        ELSE CAST(NULL AS DOUBLE)
                      END
                    ) AS cn_dmgd_gds,
                    SUM(
                      CASE
                        WHEN (
                          
                            (
                              edw_vw_th_sellout_sales_foc_fact.cn_reason_cd IS NULL
                            )
                            OR (
                              CAST((
                                edw_vw_th_sellout_sales_foc_fact.cn_reason_cd
                              ) AS TEXT) = CAST((
                                CAST('' AS VARCHAR)
                              ) AS TEXT)
                            )
                          )
                          AND (
                            CAST((
                              edw_vw_th_sellout_sales_foc_fact.net_trd_sls
                            ) AS DOUBLE) < CAST((
                              0
                            ) AS DOUBLE)
                          )
                        
                        THEN CAST((
                          edw_vw_th_sellout_sales_foc_fact.net_trd_sls
                        ) AS DOUBLE)
                        WHEN
                              edw_vw_th_sellout_sales_foc_fact.cn_reason_cd
                            like 'D%' 
                        
                        THEN CAST((
                          edw_vw_th_sellout_sales_foc_fact.net_trd_sls
                        ) AS DOUBLE)
                        WHEN (
                          
                              edw_vw_th_sellout_sales_foc_fact.cn_reason_cd
                            like 'N%'
                          )
                        
                        THEN CAST((
                          0.0
                        ) AS DOUBLE)
                        ELSE CAST(NULL AS DOUBLE)
                      END
                    ) AS crdt_nt_amt,
                    SUM(edw_vw_th_sellout_sales_foc_fact.trd_discnt_item_lvl) AS trd_discnt_item_lvl,
                    SUM(edw_vw_th_sellout_sales_foc_fact.trd_discnt_bill_lvl) AS trd_discnt_bill_lvl,
                    SUM(edw_vw_th_sellout_sales_foc_fact.ret_val) AS ret_val,
                    SUM(edw_vw_th_sellout_sales_foc_fact.sls_qty) AS sls_qty,
                    SUM(edw_vw_th_sellout_sales_foc_fact.ret_qty) AS ret_qty,
                    SUM(
                      CASE
                        WHEN (
                          (
                            edw_vw_th_sellout_sales_foc_fact.cn_reason_cd IS NULL
                          )
                          OR (
                            LEFT(CAST((
                              edw_vw_th_sellout_sales_foc_fact.cn_reason_cd
                            ) AS TEXT), 1) <> CAST((
                              CAST('N' AS VARCHAR)
                            ) AS TEXT)
                          )
                        )
                        THEN CAST((
                          edw_vw_th_sellout_sales_foc_fact.sls_qty
                        ) AS DOUBLE)
                        ELSE CAST(NULL AS DOUBLE)
                      END
                    ) AS quantity_dz,
                    SUM(edw_vw_th_sellout_sales_foc_fact.net_trd_sls) AS net_trd_sls,
                    SUM(
                      CASE
                        WHEN (
                          (
                            edw_vw_th_sellout_sales_foc_fact.cn_reason_cd IS NULL
                          )
                          OR (
                            CAST((
                              edw_vw_th_sellout_sales_foc_fact.cn_reason_cd
                            ) AS TEXT) = CAST((
                              CAST('' AS VARCHAR)
                            ) AS TEXT)
                          )
                        )
                        THEN CAST((
                          edw_vw_th_sellout_sales_foc_fact.tot_bf_discount
                        ) AS DOUBLE)
                        WHEN (
                          
                              edw_vw_th_sellout_sales_foc_fact.cn_reason_cd
                            like 'D%' 
                          
                        )
                        THEN CAST((
                          edw_vw_th_sellout_sales_foc_fact.tot_bf_discount
                        ) AS DOUBLE)
                        WHEN (
                          
                              edw_vw_th_sellout_sales_foc_fact.cn_reason_cd
                            like 'N%' 
                          )
                        
                        THEN CAST((
                          0.0
                        ) AS DOUBLE)
                        ELSE CAST(NULL AS DOUBLE)
                      END
                    ) AS tot_bf_discount
                  FROM edw_vw_th_sellout_sales_foc_fact
                  WHERE
                    (
                      CAST((
                        edw_vw_th_sellout_sales_foc_fact.cntry_cd
                      ) AS TEXT) = CAST((
                        CAST('TH' AS VARCHAR)
                      ) AS TEXT)
                    )
                  GROUP BY
                    edw_vw_th_sellout_sales_foc_fact.cntry_cd,
                    edw_vw_th_sellout_sales_foc_fact.cntry_nm,
                    edw_vw_th_sellout_sales_foc_fact.bill_date,
                    edw_vw_th_sellout_sales_foc_fact.order_no,
                    edw_vw_th_sellout_sales_foc_fact.product_name2,
                    edw_vw_th_sellout_sales_foc_fact.iscancel,
                    edw_vw_th_sellout_sales_foc_fact.grp_cd,
                    edw_vw_th_sellout_sales_foc_fact.prom_cd1,
                    edw_vw_th_sellout_sales_foc_fact.prom_cd2,
                    edw_vw_th_sellout_sales_foc_fact.prom_cd3,
                    edw_vw_th_sellout_sales_foc_fact.dstrbtr_grp_cd,
                    edw_vw_th_sellout_sales_foc_fact.dstrbtr_matl_num,
                    edw_vw_th_sellout_sales_foc_fact.cust_cd,
                    edw_vw_th_sellout_sales_foc_fact.slsmn_nm,
                    edw_vw_th_sellout_sales_foc_fact.slsmn_cd,
                    edw_vw_th_sellout_sales_foc_fact.cn_reason_cd,
                    edw_vw_th_sellout_sales_foc_fact.cn_reason_desc,
                    edw_vw_th_sellout_sales_foc_fact.grs_prc
                ) AS sales
                JOIN "time"
                  ON (
                    (
                      sales.bill_date = CAST((
                        "time".cal_date
                      ) AS TIMESTAMPNTZ)
                    )
                  )
              )
            ) 
),
final as (
SELECT
  sales.bill_date AS order_date,
  sales.order_no,
  sales.iscancel,
  sales.grp_cd,
  sales.prom_cd1,
  sales.prom_cd2,
  sales.prom_cd3,
  sales.year,
  sales.qrtr AS year_quarter,
  sales.mnth_id AS month_year,
  sales.mnth_no AS month_number,
  sales.wk AS year_week_number,
  sales.mnth_wk_no AS month_week_number,
  sales.cntry_cd AS country_code,
  sales.cntry_nm AS country_name,
  sales.dstrbtr_grp_cd AS distributor_id,
  sellout_cust.region_nm AS region_desc,
  sellout_cust.prov_nm AS city,
  sellout_cust.city_nm AS district,
  sales.cust_cd AS ar_code,
  sellout_cust.cust_nm AS ar_name,
  sellout_cust.chnl_cd AS channel_code,
  sellout_cust.chnl_desc AS channel,
  sellout_cust.sls_office_cd AS sales_office_code,
  CASE
    WHEN (
      SUBSTRING(CAST((
        sellout_cust.sls_grp_cd
      ) AS TEXT), 2, 1) = CAST((
        CAST('1' AS VARCHAR)
      ) AS TEXT)
    )
    THEN (
      CAST((
        sales.dstrbtr_grp_cd
      ) AS TEXT) || CAST((
        CAST(' Van' AS VARCHAR)
      ) AS TEXT)
    )
    ELSE (
      CAST((
        sales.dstrbtr_grp_cd
      ) AS TEXT) || CAST((
        CAST(' Credit' AS VARCHAR)
      ) AS TEXT)
    )
  END AS sales_office_name,
  sellout_cust.sls_grp_cd AS sales_group,
  sellout_cust.cust_grp_cd AS "cluster",
  sellout_cust.outlet_type_cd AS ar_type_code,
  sellout_cust.outlet_type_desc AS ar_type_name,
  sellin_cust.sap_cust_nm AS distributor_name,
  sellin_cust.sap_cust_id,
  sellin_cust.sap_cust_nm,
  sellin_cust.sap_sls_org,
  sellin_cust.sap_cmp_id,
  sellin_cust.sap_cntry_cd,
  sellin_cust.sap_cntry_nm,
  sellin_cust.sap_addr,
  sellin_cust.sap_region,
  sellin_cust.sap_state_cd,
  sellin_cust.sap_city,
  sellin_cust.sap_post_cd,
  sellin_cust.sap_chnl_cd,
  sellin_cust.sap_chnl_desc,
  sellin_cust.sap_sls_office_cd,
  sellin_cust.sap_sls_office_desc,
  sellin_cust.sap_sls_grp_cd,
  sellin_cust.sap_sls_grp_desc,
  sellin_cust.sap_prnt_cust_key,
  sellin_cust.sap_prnt_cust_desc,
  sellin_cust.sap_cust_chnl_key,
  sellin_cust.sap_cust_chnl_desc,
  sellin_cust.sap_cust_sub_chnl_key,
  sellin_cust.sap_sub_chnl_desc,
  sellin_cust.sap_go_to_mdl_key,
  sellin_cust.sap_go_to_mdl_desc,
  sellin_cust.sap_bnr_key,
  sellin_cust.sap_bnr_desc,
  sellin_cust.sap_bnr_frmt_key,
  sellin_cust.sap_bnr_frmt_desc,
  sellin_cust.retail_env,
  sales.dstrbtr_matl_num AS sku_code,
  CASE
    WHEN (
      si_matl.sap_mat_desc IS NULL
    )
    THEN sales.product_name2
    ELSE si_matl.sap_mat_desc
  END AS sku_description,
  so_matl.dstrbtr_bar_cd AS bar_code,
  si_matl.gph_prod_frnchse AS franchise,
  si_matl.gph_prod_brnd AS brand,
  si_matl.gph_prod_vrnt AS variant,
  si_matl.gph_prod_sgmnt AS segment,
  si_matl.gph_prod_put_up_desc AS put_up_description,
  si_matl.prod_sub_brand,
  si_matl.prod_subsegment,
  si_matl.prod_category,
  si_matl.prod_subcategory,
  sales.slsmn_nm AS salesman_name,
  sales.slsmn_cd AS salesman_code,
  sales.cn_reason_cd AS cn_reason_code,
  sales.cn_reason_desc AS cn_reason_description,
  sales.grs_prc AS price,
  sales.grs_trd_sls AS gross_trade_sales,
  sales.cn_dmgd_gds AS cn_damaged_goods,
  sales.crdt_nt_amt AS credit_note_amount,
  sales.trd_discnt_item_lvl AS line_discount,
  sales.trd_discnt_bill_lvl AS bottom_line_discount,
  sales.sls_qty AS sales_quantity,
  sales.ret_qty AS return_quantity,
  sales.quantity_dz,
  sales.net_trd_sls AS net_invoice,
  sales.tot_bf_discount,
  target_distribution.target AS target_calls,
  target_sales.target AS target_sales
FROM 
            sales
            LEFT JOIN  sellout_cust
              ON (
                (
                  (
                    UPPER(CAST((
                      sales.cust_cd
                    ) AS TEXT)) = UPPER(CAST((
                      sellout_cust.cust_cd
                    ) AS TEXT))
                  )
                  AND (
                    UPPER(CAST((
                      sales.dstrbtr_grp_cd
                    ) AS TEXT)) = UPPER(CAST((
                      sellout_cust.dstrbtr_grp_cd
                    ) AS TEXT))
                  )
                )
              )
          
          LEFT JOIN  target_distribution
            ON (
              (
                (
                  (
                    UPPER(CAST((
                      sales.dstrbtr_matl_num
                    ) AS TEXT)) = UPPER(CAST((
                      target_distribution.prod_cd
                    ) AS TEXT))
                  )
                  AND (
                    UPPER(CAST((
                      sales.dstrbtr_grp_cd
                    ) AS TEXT)) = UPPER(CAST((
                      target_distribution.dstrbtr_id
                    ) AS TEXT))
                  )
                )
                AND (
                  (
                    SUBSTRING(CAST((
                      CAST((
                        sales.bill_date
                      ) AS VARCHAR)
                    ) AS TEXT), 1, 4) || SUBSTRING(CAST((
                      CAST((
                        sales.bill_date
                      ) AS VARCHAR)
                    ) AS TEXT), 6, 2)
                  ) = CAST((
                    target_distribution.period
                  ) AS TEXT)
                )
              )
            )
        
        LEFT JOIN  target_sales
          ON (
            (
              (
                (
                  (
                    CAST((
                      sales.dstrbtr_grp_cd
                    ) AS TEXT) = CAST((
                      target_sales.dstrbtr_id
                    ) AS TEXT)
                  )
                  AND (
                    (
                      SUBSTRING(CAST((
                        CAST((
                          sales.bill_date
                        ) AS VARCHAR)
                      ) AS TEXT), 1, 4) || SUBSTRING(CAST((
                        CAST((
                          sales.bill_date
                        ) AS VARCHAR)
                      ) AS TEXT), 6, 2)
                    ) = CAST((
                      target_sales.period
                    ) AS TEXT)
                  )
                )
                AND (
                  UPPER(CAST((
                    sellout_cust.sls_office_cd
                  ) AS TEXT)) = UPPER(CAST((
                    target_sales.sls_office
                  ) AS TEXT))
                )
              )
              AND (
                UPPER(CAST((
                  sellout_cust.sls_grp_cd
                ) AS TEXT)) = UPPER(CAST((
                  target_sales.sls_grp
                ) AS TEXT))
              )
            )
          )
      
      LEFT JOIN  sellin_cust
        ON (
          (
            UPPER(CAST((
              sellout_cust.sap_soldto_code
            ) AS TEXT)) = UPPER(CAST((
              sellin_cust.sap_cust_id
            ) AS TEXT))
          )
        )
    
    LEFT JOIN si_matl
      ON (
        (
          UPPER(CAST((
            sales.dstrbtr_matl_num
          ) AS TEXT)) = UPPER(
            LTRIM(CAST((
              si_matl.sap_matl_num
            ) AS TEXT), CAST((
              CAST('0' AS VARCHAR)
            ) AS TEXT))
          )
        )
      )
  
  LEFT JOIN  so_matl
    ON (
      (
        CAST((
          si_matl.sap_matl_num
        ) AS TEXT) = CAST((
          so_matl.sap_matl_num
        ) AS TEXT)
      )
    )


    )

select * from final 
