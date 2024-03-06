with 
edw_vw_os_sellout_sales_fact as (
    select * from {{ source('snaposeedw_integration','edw_vw_os_sellout_sales_fact') }}
),

edw_vw_os_sellout_inventory_fact as (
    select * from {{ source('snaposeedw_integration','edw_vw_os_sellout_inventory_fact') }}
),

itg_th_dstrbtr_material_dim as (
    select * from {{ source('snaposeitg_integration','itg_th_dstrbtr_material_dim') }}
),

edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),

edw_vw_os_material_dim as (
    select * from {{ source('snaposeedw_integration','edw_vw_os_material_dim') }}
),

edw_vw_os_dstrbtr_customer_dim as (
    select * from {{ source('snaposeedw_integration','edw_vw_os_dstrbtr_customer_dim') }}
),

edw_vw_os_sellin_sales_fact as (
    select * from {{ source('snaposeedw_integration','edw_vw_os_sellin_sales_fact') }}
),

edw_customer_sales_dim as (
    select * from {{ source('snapaspedw_integration','edw_customer_sales_dim') }}
),

edw_vw_os_customer_dim as (
    select * from {{ source('snaposeedw_integration','edw_vw_os_customer_dim') }}
),
edw_company_dim as (
    select * from {{ source('snapaspedw_integration','edw_company_dim') }}
),
v_edw_customer_sales_dim as (
    select * from {{ source('snapaspedw_integration','v_edw_customer_sales_dim') }}
),
itg_th_dtsdistributor as (
    select * from {{ source('snaposeitg_integration','itg_th_dtsdistributor') }}
),
edw_vw_os_dstrbtr_material_dim as (
    select * from {{ source('snaposeedw_integration','edw_vw_os_dstrbtr_material_dim') }}
),

sales as 
(
      SELECT
        sales.typ,
        sales.cntry_cd,
        sales.cntry_nm,
        "time"."year",
        "time".qrtr,
        "time".mnth_id,
        "time".mnth_no,
        "time".wk,
        "time".mnth_wk_no,
        "time".cal_date,
        sales.warehse_cd,
        sales.warehse_grp,
        sales.bill_date,
        sales.dstrbtr_grp_cd,
        sales.dstrbtr_matl_num,
        sales.sls_qty,
        sales.ret_qty,
        sales.grs_trd_sls,
        sales.soh,
        sales.amt_bfr_disc,
        sales.amount_sls
      FROM (
        (
          SELECT
            CAST('Sales' AS VARCHAR) AS typ,
            edw_vw_os_sellout_sales_fact.cntry_cd,
            edw_vw_os_sellout_sales_fact.cntry_nm,
            edw_vw_os_sellout_sales_fact.bill_date,
            edw_vw_os_sellout_sales_fact.dstrbtr_grp_cd,
            edw_vw_os_sellout_sales_fact.dstrbtr_matl_num,
            CAST(NULL AS VARCHAR) AS warehse_cd,
            CAST(NULL AS VARCHAR) AS warehse_grp,
            SUM(edw_vw_os_sellout_sales_fact.sls_qty) AS sls_qty,
            SUM(edw_vw_os_sellout_sales_fact.ret_qty) AS ret_qty,
            SUM(
              CASE
                WHEN (
                  (
                    edw_vw_os_sellout_sales_fact.cn_reason_cd IS NULL
                  )
                  OR (
                    LEFT(CAST((
                      edw_vw_os_sellout_sales_fact.cn_reason_cd
                    ) AS TEXT), 1) <> CAST((
                      CAST('N' AS VARCHAR)
                    ) AS TEXT)
                  )
                )
                THEN CAST((
                  (
                    edw_vw_os_sellout_sales_fact.grs_trd_sls + edw_vw_os_sellout_sales_fact.ret_val
                  )
                ) AS DOUBLE)
                ELSE CAST(NULL AS DOUBLE)
              END
            ) AS grs_trd_sls,
            0 AS soh,
            0 AS amt_bfr_disc,
            0 AS amount_sls
          FROM edw_vw_os_sellout_sales_fact
          WHERE
            (
              CAST((
                edw_vw_os_sellout_sales_fact.cntry_cd
              ) AS TEXT) = CAST((
                CAST('TH' AS VARCHAR)
              ) AS TEXT)
            )
          GROUP BY
            edw_vw_os_sellout_sales_fact.cntry_cd,
            edw_vw_os_sellout_sales_fact.cntry_nm,
            edw_vw_os_sellout_sales_fact.bill_date,
            edw_vw_os_sellout_sales_fact.dstrbtr_grp_cd,
            edw_vw_os_sellout_sales_fact.dstrbtr_matl_num
          UNION ALL
          SELECT
            CAST('Inventory' AS VARCHAR) AS typ,
            CAST('TH' AS VARCHAR) AS cntry_cd,
            CAST('Thailand' AS VARCHAR) AS cntry_nm,
            inventory.inv_dt,
            inventory.dstrbtr_grp_cd,
            inventory.dstrbtr_matl_num,
            inventory.warehse_cd,
            CASE
              WHEN (
                SUBSTRING(CAST((
                  inventory.warehse_cd
                ) AS TEXT), 2, 1) = CAST((
                  CAST('7' AS VARCHAR)
                ) AS TEXT)
              )
              THEN CAST('Damage Goods' AS VARCHAR)
              WHEN (
                CAST((
                  inventory.warehse_cd
                ) AS TEXT) = CAST((
                  CAST('V902' AS VARCHAR)
                ) AS TEXT)
              )
              THEN CAST('Damage Goods' AS VARCHAR)
              WHEN (
                (
                  SUBSTRING(CAST((
                    inventory.warehse_cd
                  ) AS TEXT), 2, 1) <> CAST((
                    CAST('7' AS VARCHAR)
                  ) AS TEXT)
                )
                OR (
                  CAST((
                    inventory.warehse_cd
                  ) AS TEXT) <> CAST((
                    CAST('V902' AS VARCHAR)
                  ) AS TEXT)
                )
              )
              THEN CAST('Normal Goods' AS VARCHAR)
              ELSE CAST(NULL AS VARCHAR)
            END AS warehse_grp,
            0 AS sls_qty,
            0 AS ret_qty,
            0 AS net_trd_sls,
            inventory.soh,
            (
              inventory.soh * CAST((
                itg_material.sls_prc3
              ) AS DOUBLE)
            ) AS amt_bfr_disc,
            (
              CAST((
                itg_material.sls_prc_credit
              ) AS DOUBLE) * inventory.soh
            ) AS amount_inv
          FROM (
            (
              SELECT DISTINCT
                inventory.warehse_cd,
                inventory.dstrbtr_grp_cd,
                inventory.dstrbtr_matl_num,
                inventory.inv_dt,
                (
                  CAST((
                    SUM(inventory.soh)
                  ) AS DOUBLE) / CAST((
                    12
                  ) AS DOUBLE)
                ) AS soh
              FROM edw_vw_os_sellout_inventory_fact AS inventory
              WHERE
                (
                  (
                    CAST((
                      inventory.cntry_cd
                    ) AS TEXT) = CAST((
                      CAST('TH' AS VARCHAR)
                    ) AS TEXT)
                  )
                  AND (
                    inventory.soh > CAST((
                      CAST((
                        CAST((
                          0
                        ) AS DECIMAL)
                      ) AS DECIMAL(18, 0))
                    ) AS DECIMAL(22, 6))
                  )
                )
              GROUP BY
                inventory.warehse_cd,
                inventory.dstrbtr_grp_cd,
                inventory.dstrbtr_matl_num,
                inventory.inv_dt
            ) AS inventory
            LEFT JOIN (
              SELECT DISTINCT
                itg_th_dstrbtr_material_dim.item_cd,
                itg_th_dstrbtr_material_dim.sls_prc3,
                itg_th_dstrbtr_material_dim.sls_prc_credit
              FROM itg_th_dstrbtr_material_dim
            ) AS itg_material
              ON (
                (
                  CAST((
                    inventory.dstrbtr_matl_num
                  ) AS TEXT) = CAST((
                    itg_material.item_cd
                  ) AS TEXT)
                )
              )
          )
        ) AS sales
        JOIN (
          SELECT DISTINCT
            edw_vw_os_time_dim."year",
            edw_vw_os_time_dim.qrtr,
            edw_vw_os_time_dim.mnth_id,
            edw_vw_os_time_dim.mnth_no,
            edw_vw_os_time_dim.wk,
            edw_vw_os_time_dim.mnth_wk_no,
            edw_vw_os_time_dim.cal_date
          FROM edw_vw_os_time_dim AS edw_vw_os_time_dim
          WHERE
            (
              (
                edw_vw_os_time_dim."year" >(DATE_PART(YEAR, CURRENT_TIMESTAMP()) - 6)
              )
              OR (
                edw_vw_os_time_dim."year" >(DATE_PART(YEAR, CURRENT_TIMESTAMP()) - 6)
              )
            )
        ) AS "time"
          ON (
            (
              sales.bill_date = CAST((
                "time".cal_date
              ) AS TIMESTAMPNTZ)
            )
          )
      )
    ) ,
matl as 
(
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
      FROM edw_vw_os_material_dim
      WHERE
        (
          CAST((
            edw_vw_os_material_dim.cntry_key
          ) AS TEXT) = CAST((
            CAST('TH' AS VARCHAR)
          ) AS TEXT)
        )
    ),
cust as 
(
    SELECT
      sellout_cust.dstrbtr_grp_cd,
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
    FROM (
      (
        SELECT DISTINCT
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
      ) AS sellout_cust
      JOIN edw_vw_os_customer_dim AS sellin_cust
        ON (
          (
            CAST((
              sellout_cust.sap_soldto_code
            ) AS TEXT) = CAST((
              sellin_cust.sap_cust_id
            ) AS TEXT)
          )
        )
    )
  ) ,
sellin_fact as 
(
              SELECT
                edw_vw_os_sellin_sales_fact.item_cd,
                edw_vw_os_sellin_sales_fact.cust_id,
                edw_vw_os_sellin_sales_fact.sls_org,
                edw_vw_os_sellin_sales_fact.sls_grp AS sap_sls_grp_cd,
                sls_grp_lkp.sls_grp_desc AS sap_sls_grp_desc,
                edw_vw_os_sellin_sales_fact.sls_ofc AS sap_sls_office_cd,
                sls_ofc_lkp.sls_ofc_desc AS sap_sls_office_desc,
                edw_vw_os_sellin_sales_fact.plnt,
                edw_vw_os_sellin_sales_fact.acct_no,
                edw_vw_os_sellin_sales_fact.cust_grp,
                edw_vw_os_sellin_sales_fact.cust_sls,
                edw_vw_os_sellin_sales_fact.pstng_per,
                edw_vw_os_sellin_sales_fact.dstr_chnl,
                edw_vw_os_sellin_sales_fact.jj_mnth_id,
                SUM(edw_vw_os_sellin_sales_fact.base_val) AS base_val,
                SUM(edw_vw_os_sellin_sales_fact.sls_qty) AS sls_qty,
                SUM(edw_vw_os_sellin_sales_fact.ret_qty) AS ret_qty,
                SUM(edw_vw_os_sellin_sales_fact.sls_less_rtn_qty) AS sls_less_rtn_qty,
                SUM(edw_vw_os_sellin_sales_fact.gts_val) AS gts_val,
                SUM(edw_vw_os_sellin_sales_fact.ret_val) AS ret_val,
                SUM(edw_vw_os_sellin_sales_fact.gts_less_rtn_val) AS gts_less_rtn_val,
                SUM(edw_vw_os_sellin_sales_fact.tp_val) AS tp_val,
                SUM(edw_vw_os_sellin_sales_fact.nts_val) AS nts_val,
                SUM(edw_vw_os_sellin_sales_fact.nts_qty) AS nts_qty
              FROM (
                (
                  edw_vw_os_sellin_sales_fact AS edw_vw_os_sellin_sales_fact
                    LEFT JOIN (
                      SELECT DISTINCT
                        edw_customer_sales_dim.sls_ofc AS sap_sls_office_cd,
                        edw_customer_sales_dim.sls_ofc_desc
                      FROM edw_customer_sales_dim
                      WHERE
                        (
                          (
                            (
                              CAST((
                                edw_customer_sales_dim.sls_org
                              ) AS TEXT) = CAST((
                                CAST('2400' AS VARCHAR)
                              ) AS TEXT)
                            )
                            OR (
                              CAST((
                                edw_customer_sales_dim.sls_org
                              ) AS TEXT) = CAST((
                                CAST('2500' AS VARCHAR)
                              ) AS TEXT)
                            )
                          )
                          AND (
                            NOT CASE
                              WHEN (
                                CAST((
                                  edw_customer_sales_dim.sls_ofc
                                ) AS TEXT) = CAST((
                                  CAST('' AS VARCHAR)
                                ) AS TEXT)
                              )
                              THEN CAST(NULL AS VARCHAR)
                              ELSE edw_customer_sales_dim.sls_ofc
                            END IS NULL
                          )
                        )
                    ) AS sls_ofc_lkp
                      ON (
                        (
                          CAST((
                            sls_ofc_lkp.sap_sls_office_cd
                          ) AS TEXT) = CAST((
                            edw_vw_os_sellin_sales_fact.sls_ofc
                          ) AS TEXT)
                        )
                      )
                )
                LEFT JOIN (
                  SELECT DISTINCT
                    edw_customer_sales_dim.sls_grp,
                    edw_customer_sales_dim.sls_grp_desc
                  FROM edw_customer_sales_dim
                  WHERE
                    (
                      (
                        (
                          CAST((
                            edw_customer_sales_dim.sls_org
                          ) AS TEXT) = CAST((
                            CAST('2400' AS VARCHAR)
                          ) AS TEXT)
                        )
                        OR (
                          CAST((
                            edw_customer_sales_dim.sls_org
                          ) AS TEXT) = CAST((
                            CAST('2500' AS VARCHAR)
                          ) AS TEXT)
                        )
                      )
                      AND (
                        NOT CASE
                          WHEN (
                            CAST((
                              edw_customer_sales_dim.sls_grp
                            ) AS TEXT) = CAST((
                              CAST('' AS VARCHAR)
                            ) AS TEXT)
                          )
                          THEN CAST(NULL AS VARCHAR)
                          ELSE edw_customer_sales_dim.sls_grp
                        END IS NULL
                      )
                    )
                ) AS sls_grp_lkp
                  ON (
                    (
                      CAST((
                        sls_grp_lkp.sls_grp
                      ) AS TEXT) = CAST((
                        edw_vw_os_sellin_sales_fact.sls_grp
                      ) AS TEXT)
                    )
                  )
              )
              WHERE
                (
                  CAST((
                    edw_vw_os_sellin_sales_fact.cntry_nm
                  ) AS TEXT) = CAST((
                    CAST('TH' AS VARCHAR)
                  ) AS TEXT)
                )
              GROUP BY
                edw_vw_os_sellin_sales_fact.item_cd,
                edw_vw_os_sellin_sales_fact.cust_id,
                edw_vw_os_sellin_sales_fact.sls_grp,
                edw_vw_os_sellin_sales_fact.sls_org,
                edw_vw_os_sellin_sales_fact.sls_ofc,
                edw_vw_os_sellin_sales_fact.plnt,
                edw_vw_os_sellin_sales_fact.acct_no,
                edw_vw_os_sellin_sales_fact.dstr_chnl,
                edw_vw_os_sellin_sales_fact.cust_grp,
                edw_vw_os_sellin_sales_fact.cust_sls,
                edw_vw_os_sellin_sales_fact.pstng_per,
                edw_vw_os_sellin_sales_fact.jj_mnth_id,
                sls_ofc_lkp.sls_ofc_desc,
                sls_grp_lkp.sls_grp_desc
            ) ,
cmp as 
(
          SELECT
            edw_company_dim.co_cd,
            edw_company_dim.company_nm
          FROM edw_company_dim
          WHERE
            (
              CAST((
                edw_company_dim.ctry_key
              ) AS TEXT) = CAST((
                CAST('TH' AS VARCHAR)
              ) AS TEXT)
            )
        ) ,
sellin_mat as 
(
          SELECT DISTINCT
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
sellout_mat as 
(
          SELECT DISTINCT
            edw_vw_os_dstrbtr_material_dim.dstrbtr_matl_num,
            edw_vw_os_dstrbtr_material_dim.is_npi,
            edw_vw_os_dstrbtr_material_dim.npi_str_period,
            edw_vw_os_dstrbtr_material_dim.npi_end_period,
            edw_vw_os_dstrbtr_material_dim.is_reg,
            edw_vw_os_dstrbtr_material_dim.is_promo,
            edw_vw_os_dstrbtr_material_dim.promo_strt_period,
            edw_vw_os_dstrbtr_material_dim.promo_end_period,
            edw_vw_os_dstrbtr_material_dim.is_mcl,
            edw_vw_os_dstrbtr_material_dim.is_hero
          FROM edw_vw_os_dstrbtr_material_dim
          WHERE
            (
              CAST((
                edw_vw_os_dstrbtr_material_dim.cntry_cd
              ) AS TEXT) = CAST((
                CAST('TH' AS VARCHAR)
              ) AS TEXT)
            )
        ),
mat as 
(
      SELECT
        sellin_mat.sap_matl_num,
        sellin_mat.sap_mat_desc,
        sellin_mat.gph_region,
        sellin_mat.gph_prod_frnchse,
        sellin_mat.gph_prod_brnd,
        sellin_mat.gph_prod_vrnt,
        sellin_mat.gph_prod_sgmnt,
        sellin_mat.gph_prod_put_up_desc,
        sellin_mat.prod_sub_brand,
        sellin_mat.prod_subsegment,
        sellin_mat.prod_category,
        sellin_mat.prod_subcategory,
        sellout_mat.is_npi,
        sellout_mat.npi_str_period,
        sellout_mat.npi_end_period,
        sellout_mat.is_reg,
        sellout_mat.is_promo,
        sellout_mat.promo_strt_period,
        sellout_mat.promo_end_period,
        sellout_mat.is_mcl,
        sellout_mat.is_hero
      FROM (
         sellin_mat
        LEFT JOIN  sellout_mat
          ON (
            (
              CAST((
                sellin_mat.sap_matl_num
              ) AS TEXT) = CAST((
                sellout_mat.dstrbtr_matl_num
              ) AS TEXT)
            )
          )
      )
    ) ,


sellin as 
(
  SELECT
    "time"."year" AS year_jnj,
    "time".qrtr AS year_quarter_jnj,
    "time".mnth_id AS year_month_jnj,
    "time".mnth_no AS month_number_jnj,
    sellin_fact.cust_id AS customer_id,
    cmp.company_nm AS sap_company_name,
    cust.sap_cust_id,
    cust.sap_cust_nm,
    cust.sap_sls_org,
    cust.sap_cmp_id,
    cust.sap_cntry_cd,
    cust.sap_cntry_nm,
    cust.sap_addr,
    cust.sap_region,
    cust.sap_state_cd,
    cust.sap_city,
    cust.sap_post_cd,
    cust.sap_chnl_cd,
    cust.sap_chnl_desc,
    sellin_fact.sap_sls_office_cd,
    sellin_fact.sap_sls_office_desc,
    sellin_fact.sap_sls_grp_cd,
    sellin_fact.sap_sls_grp_desc,
    cust.sap_prnt_cust_key,
    cust.sap_prnt_cust_desc,
    cust.sap_cust_chnl_key,
    cust.sap_cust_chnl_desc,
    cust.sap_cust_sub_chnl_key,
    cust.sap_sub_chnl_desc,
    cust.sap_go_to_mdl_key,
    cust.sap_go_to_mdl_desc AS go_to_model_description,
    cust.sap_bnr_key,
    cust.sap_bnr_desc AS banner_description,
    cust.sap_bnr_frmt_key,
    cust.sap_bnr_frmt_desc,
    cust.retail_env,
    so_cust.dstrbtr_grp_cd,
    sellin_fact.plnt AS plant,
    sellin_fact.acct_no AS account_number,
    sellin_fact.cust_grp AS customer_group,
    sellin_fact.cust_sls AS customer_sales,
    sellin_fact.pstng_per AS posting_per,
    sellin_fact.dstr_chnl AS distributor_channel,
    sellin_fact.item_cd AS item_code,
    mat.sap_mat_desc AS item_description,
    mat.gph_prod_frnchse AS franchise,
    mat.gph_prod_brnd AS brand,
    mat.gph_prod_vrnt AS variant,
    mat.gph_prod_sgmnt AS segment,
    mat.gph_prod_put_up_desc AS put_up,
    mat.prod_sub_brand,
    mat.prod_subsegment,
    mat.prod_category,
    mat.prod_subcategory,
    mat.is_npi AS npi_indicator,
    mat.npi_str_period AS npi_start_date,
    mat.npi_end_period AS npi_end_date,
    mat.is_reg AS reg_indicator,
    mat.is_hero AS hero_indicator,
    sellin_fact.base_val AS base_value,
    sellin_fact.sls_qty AS sales_quantity,
    sellin_fact.ret_qty AS return_quantity,
    sellin_fact.sls_less_rtn_qty AS sales_less_return_quantity,
    sellin_fact.gts_val AS gross_trade_sales_value,
    sellin_fact.ret_val AS return_value,
    sellin_fact.gts_less_rtn_val AS gross_trade_sales_less_return_value,
    sellin_fact.tp_val AS tp_value,
    sellin_fact.nts_val AS net_trade_sales_value,
    sellin_fact.nts_qty AS net_trade_sales_quantity
  FROM (
    (
      (
        (
          (
             sellin_fact
            JOIN (
              SELECT DISTINCT
                edw_vw_os_time_dim."year",
                edw_vw_os_time_dim.qrtr,
                edw_vw_os_time_dim.mnth_id,
                edw_vw_os_time_dim.mnth_no
              FROM edw_vw_os_time_dim AS edw_vw_os_time_dim
              WHERE
                (
                  edw_vw_os_time_dim."year" > (DATE_PART(YEAR, CURRENT_TIMESTAMP()) - 6)
                )
            ) AS "time"
              ON (
                (
                  CAST((
                    sellin_fact.jj_mnth_id
                  ) AS TEXT) = "time".mnth_id
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
              edw_vw_os_customer_dim.sap_prnt_cust_key,
              edw_vw_os_customer_dim.sap_prnt_cust_desc,
              edw_vw_os_customer_dim.sap_cust_chnl_key,
              edw_vw_os_customer_dim.sap_cust_chnl_desc,
              edw_vw_os_customer_dim.sap_cust_sub_chnl_key,
              edw_vw_os_customer_dim.sap_sub_chnl_desc,
              edw_vw_os_customer_dim.sap_go_to_mdl_key,
              edw_vw_os_customer_dim.sap_go_to_mdl_desc,
              edw_vw_os_customer_dim.sap_bnr_key,
              edw_vw_os_customer_dim.sap_bnr_desc,
              edw_vw_os_customer_dim.sap_bnr_frmt_key,
              edw_vw_os_customer_dim.sap_bnr_frmt_desc,
              edw_vw_os_customer_dim.retail_env
            FROM edw_vw_os_customer_dim AS edw_vw_os_customer_dim
            WHERE
              (
                CAST((
                  edw_vw_os_customer_dim.sap_cntry_cd
                ) AS TEXT) = CAST((
                  CAST('TH' AS VARCHAR)
                ) AS TEXT)
              )
          ) AS cust
            ON (
              (
                CAST((
                  sellin_fact.cust_id
                ) AS TEXT) = CAST((
                  cust.sap_cust_id
                ) AS TEXT)
              )
            )
        )
        LEFT JOIN  cmp
          ON (
            (
              CAST((
                cust.sap_cmp_id
              ) AS TEXT) = CAST((
                cmp.co_cd
              ) AS TEXT)
            )
          )
      )
      JOIN (
        SELECT DISTINCT
          dist.dstrbtr_id AS dstrbtr_grp_cd,
          LTRIM(
            CAST((
              cust.cust_num
            ) AS TEXT),
            CAST((
              CAST((
                0
              ) AS VARCHAR)
            ) AS TEXT)
          ) AS sap_soldto_code
        FROM (
          v_edw_customer_sales_dim AS cust
            LEFT JOIN itg_th_dtsdistributor AS dist
              ON (
                (
                  CAST((
                    cust."parent customer"
                  ) AS TEXT) = CAST((
                    dist.dist_nm
                  ) AS TEXT)
                )
              )
        )
        WHERE
          (
            (
              (
                CAST((
                  cust."go to model"
                ) AS TEXT) = CAST((
                  CAST('Indirect Accounts' AS VARCHAR)
                ) AS TEXT)
              )
              AND (
                CAST((
                  cust."sub channel"
                ) AS TEXT) = CAST((
                  CAST('Distributor (General)' AS VARCHAR)
                ) AS TEXT)
              )
            )
            AND (
              (
                CAST((
                  cust.sls_ofc_desc
                ) AS TEXT) = CAST((
                  CAST('General Trade' AS VARCHAR)
                ) AS TEXT)
              )
              OR (
                CAST((
                  cust.sls_ofc_desc
                ) AS TEXT) = CAST((
                  CAST('General Trade (OTC)' AS VARCHAR)
                ) AS TEXT)
              )
            )
          )
      ) AS so_cust
        ON (
          (
            CAST((
              cust.sap_cust_id
            ) AS TEXT) = CAST((
              CAST((
                so_cust.sap_soldto_code
              ) AS VARCHAR)
            ) AS TEXT)
          )
        )
    )
    LEFT JOIN  mat
      ON (
        (
          CAST((
            sellin_fact.item_cd
          ) AS TEXT) = CAST((
            mat.sap_matl_num
          ) AS TEXT)
        )
      )
  )
),

final as    
(
SELECT
  sales.typ AS data_type,
  CAST((
    sales.bill_date
  ) AS VARCHAR) AS order_date,
  sales."year",
  CAST((
    sales.qrtr
  ) AS VARCHAR) AS year_quarter,
  CAST((
    sales.mnth_id
  ) AS VARCHAR) AS month_year,
  sales.mnth_no AS month_number,
  CAST((
    sales.wk
  ) AS VARCHAR) AS year_week_number,
  CAST((
    sales.mnth_wk_no
  ) AS VARCHAR) AS month_week_number,
  sales.cntry_cd AS country_code,
  sales.cntry_nm AS country_name,
  sales.dstrbtr_grp_cd AS distributor_id,
  sales.warehse_cd AS whcode,
  sales.warehse_grp AS whgroup,
  sales.dstrbtr_matl_num AS sku_code,
  matl.sap_mat_desc AS sku_description,
  matl.gph_prod_frnchse AS franchise,
  matl.gph_prod_brnd AS brand,
  matl.gph_prod_vrnt AS variant,
  matl.gph_prod_sgmnt AS segment,
  matl.gph_prod_put_up_desc AS put_up_description,
  matl.prod_sub_brand,
  matl.prod_subsegment,
  matl.prod_category,
  matl.prod_subcategory,
  cust.sap_cust_id,
  cust.sap_cust_nm,
  cust.sap_sls_org,
  cust.sap_cmp_id,
  cust.sap_cntry_cd,
  cust.sap_cntry_nm,
  cust.sap_addr,
  cust.sap_region,
  cust.sap_state_cd,
  cust.sap_city,
  cust.sap_post_cd,
  cust.sap_chnl_cd,
  cust.sap_chnl_desc,
  cust.sap_sls_office_cd,
  cust.sap_sls_office_desc,
  cust.sap_sls_grp_cd,
  cust.sap_sls_grp_desc,
  cust.sap_prnt_cust_key,
  cust.sap_prnt_cust_desc,
  cust.sap_cust_chnl_key,
  cust.sap_cust_chnl_desc,
  cust.sap_cust_sub_chnl_key,
  cust.sap_sub_chnl_desc,
  cust.sap_go_to_mdl_key,
  cust.sap_go_to_mdl_desc,
  cust.sap_bnr_key,
  cust.sap_bnr_desc,
  cust.sap_bnr_frmt_key,
  cust.sap_bnr_frmt_desc,
  cust.retail_env,
  sales.sls_qty AS sales_quantity,
  sales.ret_qty AS return_quantity,
  sales.grs_trd_sls AS gross_trade_sales,
  sales.soh AS inventory_quantity,
  sales.amt_bfr_disc AS amount_before_discount,
  sales.amount_sls AS inventory,
  0 AS si_gross_trade_sales_value,
  0 AS si_tp_value,
  0 AS si_net_trade_sales_value
FROM (
  (
     sales
    LEFT JOIN  matl
      ON (
        (
          UPPER(CAST((
            sales.dstrbtr_matl_num
          ) AS TEXT)) = UPPER(
            LTRIM(CAST((
              matl.sap_matl_num
            ) AS TEXT), CAST((
              CAST('0' AS VARCHAR)
            ) AS TEXT))
          )
        )
      )
  )
  LEFT JOIN  cust
    ON (
      (
        CAST((
          cust.dstrbtr_grp_cd
        ) AS TEXT) = CAST((
          sales.dstrbtr_grp_cd
        ) AS TEXT)
      )
    )
)
UNION ALL
SELECT
  CAST('Sellin' AS VARCHAR) AS data_type,
  CAST(NULL AS VARCHAR) AS order_date,
  sellin.year_jnj AS "year",
  CAST((
    sellin.year_quarter_jnj
  ) AS VARCHAR) AS year_quarter,
  CAST((
    sellin.year_month_jnj
  ) AS VARCHAR) AS month_year,
  sellin.month_number_jnj AS month_number,
  CAST(NULL AS VARCHAR) AS year_week_number,
  CAST(NULL AS VARCHAR) AS month_week_number,
  CAST('TH' AS VARCHAR) AS country_code,
  CAST('Thailand' AS VARCHAR) AS country_name,
  sellin.dstrbtr_grp_cd AS distributor_id,
  CAST(NULL AS VARCHAR) AS whcode,
  CAST(NULL AS VARCHAR) AS whgroup,
  sellin.item_code AS sku_code,
  sellin.item_description AS sku_description,
  sellin.franchise,
  sellin.brand,
  sellin.variant,
  sellin.segment,
  sellin.put_up AS put_up_description,
  sellin.prod_sub_brand,
  sellin.prod_subsegment,
  sellin.prod_category,
  sellin.prod_subcategory,
  sellin.sap_cust_id,
  sellin.sap_cust_nm,
  sellin.sap_sls_org,
  sellin.sap_cmp_id,
  sellin.sap_cntry_cd,
  sellin.sap_cntry_nm,
  sellin.sap_addr,
  sellin.sap_region,
  sellin.sap_state_cd,
  sellin.sap_city,
  sellin.sap_post_cd,
  sellin.sap_chnl_cd,
  sellin.sap_chnl_desc,
  sellin.sap_sls_office_cd,
  sellin.sap_sls_office_desc,
  sellin.sap_sls_grp_cd,
  sellin.sap_sls_grp_desc,
  sellin.sap_prnt_cust_key,
  sellin.sap_prnt_cust_desc,
  sellin.sap_cust_chnl_key,
  sellin.sap_cust_chnl_desc,
  sellin.sap_cust_sub_chnl_key,
  sellin.sap_sub_chnl_desc,
  sellin.sap_go_to_mdl_key,
  sellin.go_to_model_description AS sap_go_to_mdl_desc,
  sellin.sap_bnr_key,
  sellin.banner_description AS sap_bnr_desc,
  sellin.sap_bnr_frmt_key,
  sellin.sap_bnr_frmt_desc,
  sellin.retail_env,
  0 AS sales_quantity,
  0 AS return_quantity,
  0 AS gross_trade_sales,
  0 AS inventory_quantity,
  0 AS amount_before_discount,
  0 AS inventory,
  SUM(sellin.gross_trade_sales_value) AS si_gross_trade_sales_value,
  SUM(sellin.tp_value) AS si_tp_value,
  SUM(sellin.net_trade_sales_value) AS si_net_trade_sales_value
FROM  sellin
GROUP BY
  1,
  2,
  sellin.year_jnj,
  CAST((
    sellin.year_quarter_jnj
  ) AS VARCHAR),
  CAST((
    sellin.year_month_jnj
  ) AS VARCHAR),
  sellin.month_number_jnj,
  7,
  8,
  9,
  10,
  sellin.dstrbtr_grp_cd,
  12,
  13,
  sellin.item_code,
  sellin.item_description,
  sellin.franchise,
  sellin.brand,
  sellin.variant,
  sellin.segment,
  sellin.put_up,
  sellin.prod_sub_brand,
  sellin.prod_subsegment,
  sellin.prod_category,
  sellin.prod_subcategory,
  sellin.sap_cust_id,
  sellin.sap_cust_nm,
  sellin.sap_sls_org,
  sellin.sap_cmp_id,
  sellin.sap_cntry_cd,
  sellin.sap_cntry_nm,
  sellin.sap_addr,
  sellin.sap_region,
  sellin.sap_state_cd,
  sellin.sap_city,
  sellin.sap_post_cd,
  sellin.sap_chnl_cd,
  sellin.sap_chnl_desc,
  sellin.sap_sls_office_cd,
  sellin.sap_sls_office_desc,
  sellin.sap_sls_grp_cd,
  sellin.sap_sls_grp_desc,
  sellin.sap_prnt_cust_key,
  sellin.sap_prnt_cust_desc,
  sellin.sap_cust_chnl_key,
  sellin.sap_cust_chnl_desc,
  sellin.sap_cust_sub_chnl_key,
  sellin.sap_sub_chnl_desc,
  sellin.sap_go_to_mdl_key,
  sellin.go_to_model_description,
  sellin.sap_bnr_key,
  sellin.banner_description,
  sellin.sap_bnr_frmt_key,
  sellin.sap_bnr_frmt_desc,
  sellin.retail_env
)



select *  from final

