with edw_vw_th_sellout_sales_foc_fact as(
 select * from {{ ref('thaedw_integration__edw_vw_th_sellout_sales_foc_fact') }}
),
edw_vw_os_time_dim as (
    select * from {{ source('aspitg_integration','edw_vw_os_time_dim') }}
),
itg_th_dstrbtr_customer_dim as (
    select * from dev_dna_core.ymoger01_workspace.thaitg_integration__itg_th_dstrbtr_customer_dim--{{ ref('thaitg_integration__itg_th_dstrbtr_customer_dim') }} -- issue with model -sathya
),
-- itg_th_dstrbtr_customer_dim_snapshot as (
--     select * from {{ ref('thaitg_integration__itg_th_dstrbtr_customer_dim_snapshot') }}
-- ),
-- itg_th_dstrbtr_material_dim as (
--    select * from {{ ref('thaitg_integration__itg_th_dstrbtr_material_dim') }} 
-- ),
itg_th_gt_dstrbtr_control as (
    select * from {{ ref('thaitg_integration__itg_th_gt_dstrbtr_control') }}
),
itg_th_gt_target_sales_re as (
    select * from {{ ref('thaitg_integration__itg_th_gt_target_sales_re') }}
),
-- itg_th_target_sales as (
--     select * from {{ ref('thaitg_integration__itg_th_target_sales') }} -- issue with model - niketh
-- ),
-- edw_vw_th_gt_msl_distribution as (
--     select * from dev_dna_core.snenav01_workspace.thaedw_integration__edw_vw_th_gt_msl_distribution
-- ),
-- edw_vw_th_gt_route as (
--     select * from dev_dna_core.snenav01_workspace.thaedw_integration__edw_vw_th_gt_route
-- ),
-- edw_vw_th_gt_sales_order as (
--     select * from dev_dna_core.snenav01_workspace.thaedw_integration__edw_vw_th_gt_sales_order
-- ),
-- edw_vw_th_gt_schedule as (
--     select * from dev_dna_core.snenav01_workspace.thaedw_integration__edw_vw_th_gt_schedule
-- ),
-- edw_vw_th_gt_visit as (
--     select * from dev_dna_core.snenav01_workspace.thaedw_integration__edw_vw_th_gt_visit
-- ),

foc_fact_sales_re as (
                          SELECT
                            CAST('ACTUAL' AS VARCHAR) AS identifier,
                            sls.cntry_cd,
                            CAST('THB' AS VARCHAR) AS crncy_cd,
                            sls.cntry_nm,
                            CAST((cal.cal_mnth_id) AS VARCHAR) AS year_month,
                            cal.cal_year AS "year",
                            cal.cal_mnth_no AS "month",
                            sls.dstrbtr_grp_cd AS distributor_id,
                            COALESCE(cust.re_nm, CAST('N/A' AS VARCHAR)) AS store_type,
                            CAST((
                              ((
                                  CAST((COALESCE(cust.sls_grp, CAST('N/A' AS VARCHAR))) AS TEXT) 
                                  + CAST((CAST('-' AS VARCHAR)) AS TEXT)) 
                                  + CAST((COALESCE(cust.salesareaname, CAST('N/A' AS VARCHAR))) AS TEXT)
                              )) AS VARCHAR) AS sales_area,
                            SUM(
                              CASE
                                WHEN (
                                  (
                                    (UPPER(SUBSTRING(CAST((sls.cn_reason_cd) AS TEXT), 1, 1)) 
                                    = CAST((CAST('D' AS VARCHAR)) AS TEXT))
                                    OR (sls.cn_reason_cd IS NULL)
                                  )
                                  OR (
                                    CAST((sls.cn_reason_cd) AS TEXT) = CAST((CAST('' AS VARCHAR)) AS TEXT)
                                  ))
                                THEN sls.jj_net_trd_sls
                                ELSE CAST((
                                  CAST((0) AS DECIMAL)) AS DECIMAL(18, 0))
                              END
                            ) AS sell_out,
                            SUM(
                              CASE
                                WHEN (
                                  (
                                    sls.cn_reason_cd IS NULL
                                  )
                                  OR (
                                    LEFT(CAST((
                                      sls.cn_reason_cd
                                    ) AS TEXT), 1) <> CAST((
                                      CAST('N' AS VARCHAR)
                                    ) AS TEXT)
                                  )
                                )
                                THEN (
                                  sls.grs_trd_sls + sls.ret_val
                                )
                                ELSE CAST((
                                  CAST((
                                    0
                                  ) AS DECIMAL)
                                ) AS DECIMAL(18, 0))
                              END
                            ) AS gross_sell_out,
                            SUM(sls.jj_net_trd_sls) AS net_sell_out,
                            CAST((
                              CAST((
                                0
                              ) AS DECIMAL)
                            ) AS DECIMAL(18, 0)) AS sellout_target,
                            CAST((
                              0
                            ) AS BIGINT) AS msld_actuals_count,
                            CAST((
                              0
                            ) AS BIGINT) AS msld_target_count,
                            CAST((
                              0
                            ) AS BIGINT) AS planned_call_count,
                            CAST((
                              0
                            ) AS BIGINT) AS visited_call_count,
                            CAST((
                              0
                            ) AS BIGINT) AS effective_call_count,
                            CAST((
                              0
                            ) AS BIGINT) AS sales_order_count,
                            CAST((
                              0
                            ) AS BIGINT) AS on_time_count,
                            CAST((
                              0
                            ) AS BIGINT) AS in_time_count,
                            CAST((
                              0
                            ) AS BIGINT) AS otif_count,
                            CAST((
                              0
                            ) AS BIGINT) AS coverage_stores_count,
                            CAST('0' AS VARCHAR) AS reactivate_stores_count,
                            CAST('0' AS VARCHAR) AS inactive_stores_count,
                            CAST('0' AS VARCHAR) AS active_stores_count,
                            CAST((
                              0
                            ) AS BIGINT) AS route_com_all,
                            CAST((
                              0
                            ) AS BIGINT) AS effective_call_all,
                            CAST((
                              0
                            ) AS BIGINT) AS planned_store,
                            CAST((
                              0
                            ) AS BIGINT) AS effective_store_in_plan,
                            CAST((
                              0
                            ) AS BIGINT) AS effective_store_all,
                            CAST((
                              0
                            ) AS BIGINT) AS total_skus,
                            CAST((
                              0
                            ) AS BIGINT) AS total_stores
                          FROM (
                            (
                              edw_vw_th_sellout_sales_foc_fact AS sls
                                LEFT JOIN edw_vw_os_time_dim AS cal
                                  ON (
                                    (
                                      TRUNC(sls.bill_date) = TRUNC(CAST((
                                        cal.cal_date
                                      ) AS TIMESTAMPNTZ))
                                    )
                                  )
                            )
                            LEFT JOIN itg_th_dstrbtr_customer_dim AS cust
                              ON (
                                (
                                  (
                                    UPPER(CAST((
                                      sls.cust_cd
                                    ) AS TEXT)) = UPPER(CAST((
                                      cust.ar_cd
                                    ) AS TEXT))
                                  )
                                  AND (
                                    UPPER(CAST((
                                      sls.dstrbtr_grp_cd
                                    ) AS TEXT)) = UPPER(CAST((
                                      cust.dstrbtr_id
                                    ) AS TEXT))
                                  )
                                )
                              )
                          )
                          WHERE
                            (
                              (
                                UPPER(CAST((
                                  sls.dstrbtr_grp_cd
                                ) AS TEXT)) IN (
                                  SELECT DISTINCT
                                    UPPER(CAST((
                                      itg_th_gt_dstrbtr_control.distributor_id
                                    ) AS TEXT)) AS distributor_id
                                  FROM itg_th_gt_dstrbtr_control
                                  WHERE
                                    (
                                      UPPER(CAST((
                                        itg_th_gt_dstrbtr_control.sellout_flag
                                      ) AS TEXT)) = CAST((
                                        CAST('Y' AS VARCHAR)
                                      ) AS TEXT)
                                    )
                                )
                              )
                              AND (
                                sls.iscancel = 0
                              )
                            )
                          GROUP BY
                            sls.cntry_cd,
                            sls.cntry_nm,
                            CAST((cal.cal_mnth_id) AS VARCHAR),
                            cal.cal_year,
                            cal.cal_mnth_no,
                            sls.dstrbtr_grp_cd,
                            cust.re_nm,
                            cust.sls_grp,
                            cust.salesareaname

                          UNION ALL

                          SELECT
                            CAST('TARGET_RE' AS VARCHAR) AS identifier,
                            tgt.cntry_cd,
                            CAST('THB' AS VARCHAR) AS crncy_cd,
                            CAST('Thailand' AS VARCHAR) AS cntry_nm,
                            tgt.period AS year_month,
                            CAST((
                              SUBSTRING(CAST((
                                tgt.period
                              ) AS TEXT), 1, 4)
                            ) AS INT) AS "year",
                            CAST((
                              SUBSTRING(CAST((
                                tgt.period
                              ) AS TEXT), 5, 6)
                            ) AS INT) AS "month",
                            tgt.distributor_id,
                            COALESCE(cust.re_nm, CAST('N/A' AS VARCHAR)) AS store_type,
                            CAST('N/A' AS VARCHAR) AS sales_area,
                            CAST((
                              CAST((
                                0
                              ) AS DECIMAL)
                            ) AS DECIMAL(18, 0)) AS sell_out,
                            CAST((
                              CAST((
                                0
                              ) AS DECIMAL)
                            ) AS DECIMAL(18, 0)) AS gross_sell_out,
                            CAST((
                              CAST((
                                0
                              ) AS DECIMAL)
                            ) AS DECIMAL(18, 0)) AS net_sell_out,
                            SUM(tgt."target") AS sellout_target,
                            CAST((
                              0
                            ) AS BIGINT) AS msld_actuals_count,
                            CAST((
                              0
                            ) AS BIGINT) AS msld_target_count,
                            CAST((
                              0
                            ) AS BIGINT) AS planned_call_count,
                            CAST((
                              0
                            ) AS BIGINT) AS visited_call_count,
                            CAST((
                              0
                            ) AS BIGINT) AS effective_call_count,
                            CAST((
                              0
                            ) AS BIGINT) AS sales_order_count,
                            CAST((
                              0
                            ) AS BIGINT) AS on_time_count,
                            CAST((
                              0
                            ) AS BIGINT) AS in_time_count,
                            CAST((
                              0
                            ) AS BIGINT) AS otif_count,
                            CAST((
                              0
                            ) AS BIGINT) AS coverage_stores_count,
                            CAST('0' AS VARCHAR) AS reactivate_stores_count,
                            CAST('0' AS VARCHAR) AS inactive_stores_count,
                            CAST('0' AS VARCHAR) AS active_stores_count,
                            CAST((
                              0
                            ) AS BIGINT) AS route_com_all,
                            CAST((
                              0
                            ) AS BIGINT) AS effective_call_all,
                            CAST((
                              0
                            ) AS BIGINT) AS planned_store,
                            CAST((
                              0
                            ) AS BIGINT) AS effective_store_in_plan,
                            CAST((
                              0
                            ) AS BIGINT) AS effective_store_all,
                            CAST((
                              0
                            ) AS BIGINT) AS total_skus,
                            CAST((
                              0
                            ) AS BIGINT) AS total_stores
                          FROM (
                            itg_th_gt_target_sales_re AS tgt
                              LEFT JOIN (
                                SELECT DISTINCT
                                  itg_th_dstrbtr_customer_dim.dstrbtr_id,
                                  itg_th_dstrbtr_customer_dim.store,
                                  itg_th_dstrbtr_customer_dim.re_nm
                                FROM itg_th_dstrbtr_customer_dim
                              ) AS cust
                                ON (
                                  (
                                    (
                                      UPPER(CAST((
                                        tgt.distributor_id
                                      ) AS TEXT)) = UPPER(CAST((
                                        cust.dstrbtr_id
                                      ) AS TEXT))
                                    )
                                    AND (
                                      UPPER(CAST((
                                        tgt.re_code
                                      ) AS TEXT)) = UPPER(CAST((
                                        cust.store
                                      ) AS TEXT))
                                    )
                                  )
                                )
                          )
                          WHERE
                            (
                              UPPER(CAST((
                                tgt.distributor_id
                              ) AS TEXT)) IN (
                                SELECT DISTINCT
                                  UPPER(CAST((
                                    itg_th_gt_dstrbtr_control.distributor_id
                                  ) AS TEXT)) AS distributor_id
                                FROM itg_th_gt_dstrbtr_control
                                WHERE
                                  (
                                    UPPER(CAST((
                                      itg_th_gt_dstrbtr_control.sellout_flag
                                    ) AS TEXT)) = CAST((
                                      CAST('Y' AS VARCHAR)
                                    ) AS TEXT)
                                  )
                              )
                            )
                          GROUP BY
                            tgt.cntry_cd,
                            tgt.period,
                            tgt.distributor_id,
                            cust.re_nm
            )

select * from foc_fact_sales_re





