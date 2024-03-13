with edw_vw_th_sellout_sales_foc_fact as(
 select * from dev_dna_core.nnaras01_workspace.thaedw_integration__edw_vw_th_sellout_sales_foc_fact 
),
edw_vw_os_time_dim as (
    select * from DEV_DNA_CORE.OSEEDW_ACCESS.EDW_VW_OS_TIME_DIM
),
itg_th_dstrbtr_customer_dim as (
    select * from dev_dna_core.ASING012_WORKSPACE.thaitg_integration__itg_th_dstrbtr_customer_dim
),
itg_th_gt_dstrbtr_control as (
    select * from {{ ref('thaitg_integration__itg_th_gt_dstrbtr_control') }}
),
itg_th_gt_target_sales_re as (
    select * from {{ ref('thaitg_integration__itg_th_gt_target_sales_re') }}
),
itg_th_target_sales as (
    select * from {{ ref('thaitg_integration__itg_th_target_sales') }} -- issue with model - niketh
),
edw_vw_th_gt_msl_distribution as (
    select * from dev_dna_core.snenav01_workspace.thaedw_integration__edw_vw_th_gt_msl_distribution
),
edw_vw_th_gt_schedule as (
    select * from dev_dna_core.snenav01_workspace.thaedw_integration__edw_vw_th_gt_schedule
),
edw_vw_th_gt_route as (
    select * from dev_dna_core.snenav01_workspace.thaedw_integration__edw_vw_th_gt_route
),
itg_th_dstrbtr_customer_dim_snapshot as (
    select * from  DEV_DNA_CORE.THAITG_INTEGRATION.ITG_TH_DSTRBTR_CUSTOMER_DIM_SNAPSHOT
),
edw_vw_th_gt_visit as (
    select * from dev_dna_core.snenav01_workspace.thaedw_integration__edw_vw_th_gt_visit
),
edw_vw_th_gt_sales_order as (
    select * from dev_dna_core.snenav01_workspace.thaedw_integration__edw_vw_th_gt_sales_order
),
itg_th_dstrbtr_material_dim as (
   select * from {{ ref('thaitg_integration__itg_th_dstrbtr_material_dim') }} 
),
itg_query_parameters as (
    select * from DEV_DNA_CORE.SNAPASPITG_INTEGRATION.ITG_QUERY_PARAMETERS
),

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
                              CONCAT(
                                  CAST((COALESCE(cust.sls_grp, CAST('N/A' AS VARCHAR))) AS TEXT) 
                                  , CAST((CAST('-' AS VARCHAR)) AS TEXT) 
                                  , CAST((COALESCE(cust.salesareaname, CAST('N/A' AS VARCHAR))) AS TEXT)
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
                                      to_date(sls.bill_date) = to_date(CAST((
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
                            SUM(tgt.target) AS sellout_target,
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
                    UNION ALL
                        SELECT
                          CAST('TARGET_SA' AS VARCHAR) AS identifier,
                          CAST('TH' AS VARCHAR) AS cntry_cd,
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
                          tgt.dstrbtr_id AS distributor_id,
                          CAST('Target' AS VARCHAR) AS store_type,
                          CAST(
                            CONCAT(
                                CAST((COALESCE(cust.sls_grp, CAST('N/A' AS VARCHAR))) AS TEXT)
                                ,CAST((CAST('-' AS VARCHAR)) AS TEXT)
                                ,CAST((COALESCE(cust.salesareaname, CAST('N/A' AS VARCHAR))) AS TEXT)
                          ) AS VARCHAR) AS sales_area,
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
                          SUM(tgt.target) AS sellout_target, --SUM(tgt."target") AS sellout_target,
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
                          itg_th_target_sales AS tgt
                            LEFT JOIN (
                              SELECT DISTINCT
                                derived_table1.dstrbtr_id,
                                derived_table1.sls_grp,
                                derived_table1.salesareaname
                              FROM (
                                SELECT DISTINCT
                                  itg_th_dstrbtr_customer_dim.dstrbtr_id,
                                  itg_th_dstrbtr_customer_dim.sls_grp,
                                  itg_th_dstrbtr_customer_dim.salesareaname,
                                  ROW_NUMBER() OVER (PARTITION BY itg_th_dstrbtr_customer_dim.dstrbtr_id, itg_th_dstrbtr_customer_dim.sls_grp 
                                  ORDER BY itg_th_dstrbtr_customer_dim.salesareaname DESC NULLS LAST) AS rn
                                FROM itg_th_dstrbtr_customer_dim
                              ) AS derived_table1
                              WHERE
                                (
                                  derived_table1.rn = 1
                                )
                            ) AS cust
                              ON (
                                (
                                  (
                                    UPPER(CAST((
                                      tgt.dstrbtr_id
                                    ) AS TEXT)) = UPPER(CAST((
                                      cust.dstrbtr_id
                                    ) AS TEXT))
                                  )
                                  AND (
                                    UPPER(CAST((
                                      tgt.sls_grp
                                    ) AS TEXT)) = UPPER(CAST((
                                      cust.sls_grp
                                    ) AS TEXT))
                                  )
                                )
                              )
                        )
                        WHERE
                          (
                            UPPER(CAST((
                              tgt.dstrbtr_id
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
                          tgt.period,
                          tgt.dstrbtr_id,
                          cust.sls_grp,
                          cust.salesareaname

            ),

msl_distribution as (
      SELECT
                        CAST('ACTUAL' AS VARCHAR) AS identifier,
                        msld.cntry_cd,
                        CAST('THB' AS VARCHAR) AS crncy_cd,
                        CAST('Thailand' AS VARCHAR) AS cntry_nm,
                        CAST((
                          cal.cal_mnth_id
                        ) AS VARCHAR) AS year_month,
                        cal.cal_year AS "year",
                        cal.cal_mnth_no AS "month",
                        msld.distributor_id,
                        COALESCE(cust.re_nm, CAST('N/A' AS VARCHAR)) AS store_type,
                        CAST(
                            CONCAT(
                                CAST((COALESCE(cust.sls_grp, CAST('N/A' AS VARCHAR))) AS TEXT)
                                ,CAST((CAST('-' AS VARCHAR)) AS TEXT)
                                ,CAST((COALESCE(cust.salesareaname, CAST('N/A' AS VARCHAR))) AS TEXT)
                          ) AS VARCHAR) AS sales_area,
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
                        CAST((
                          CAST((
                            0
                          ) AS DECIMAL)
                        ) AS DECIMAL(18, 0)) AS sellout_target,
                        COUNT(*) AS msld_actuals_count,
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
                          edw_vw_th_gt_msl_distribution AS msld
                            LEFT JOIN edw_vw_os_time_dim AS cal
                              ON (
                                (
                                  to_date(CAST((
                                    msld.survey_date
                                  ) AS TIMESTAMPNTZ)) = to_date(CAST((
                                    cal.cal_date
                                  ) AS TIMESTAMPNTZ))
                                )
                              )
                        )
                        LEFT JOIN (
                          SELECT DISTINCT
                            itg_th_dstrbtr_customer_dim.dstrbtr_id,
                            itg_th_dstrbtr_customer_dim.ar_cd,
                            itg_th_dstrbtr_customer_dim.sls_grp,
                            itg_th_dstrbtr_customer_dim.re_nm,
                            itg_th_dstrbtr_customer_dim.salesareaname
                          FROM itg_th_dstrbtr_customer_dim
                        ) AS cust
                          ON (
                            (
                              (
                                UPPER(CAST((
                                  msld.distributor_id
                                ) AS TEXT)) = UPPER(CAST((
                                  cust.dstrbtr_id
                                ) AS TEXT))
                              )
                              AND (
                                UPPER(CAST((
                                  msld.store_id
                                ) AS TEXT)) = UPPER(CAST((
                                  cust.ar_cd
                                ) AS TEXT))
                              )
                            )
                          )
                      )
                      WHERE
                        (
                          (
                            (
                              UPPER(CAST((
                                msld.osa
                              ) AS TEXT)) = CAST((
                                CAST('Y' AS VARCHAR)
                              ) AS TEXT)
                            )
                            AND (
                              UPPER(CAST((
                                msld.distributor_id
                              ) AS TEXT)) IN (
                                SELECT DISTINCT
                                  UPPER(CAST((
                                    itg_th_gt_dstrbtr_control.distributor_id
                                  ) AS TEXT)) AS distributor_id
                                FROM itg_th_gt_dstrbtr_control
                                WHERE
                                  (
                                    UPPER(CAST((
                                      itg_th_gt_dstrbtr_control.msl_flag
                                    ) AS TEXT)) = CAST((
                                      CAST('Y' AS VARCHAR)
                                    ) AS TEXT)
                                  )
                              )
                            )
                          )
                          AND (
                            UPPER(msld.valid_flag) = CAST((
                              CAST('Y' AS VARCHAR)
                            ) AS TEXT)
                          )
                        )
                      GROUP BY
                        msld.cntry_cd,
                        CAST((
                          cal.cal_mnth_id
                        ) AS VARCHAR),
                        cal.cal_year,
                        cal.cal_mnth_no,
                        msld.distributor_id,
                        cust.re_nm,
                        cust.sls_grp,
                        cust.salesareaname

                    UNION ALL

                    SELECT
                      CAST('ACTUAL' AS VARCHAR) AS identifier,
                      msld.cntry_cd,
                      CAST('THB' AS VARCHAR) AS crncy_cd,
                      CAST('Thailand' AS VARCHAR) AS cntry_nm,
                      CAST((
                        cal.cal_mnth_id
                      ) AS VARCHAR) AS year_month,
                      cal.cal_year AS "year",
                      cal.cal_mnth_no AS "month",
                      msld.distributor_id,
                      COALESCE(cust.re_nm, CAST('N/A' AS VARCHAR)) AS store_type,
                      CAST((
                              CONCAT(
                                  CAST((COALESCE(cust.sls_grp, CAST('N/A' AS VARCHAR))) AS TEXT) 
                                  , CAST((CAST('-' AS VARCHAR)) AS TEXT) 
                                  , CAST((COALESCE(cust.salesareaname, CAST('N/A' AS VARCHAR))) AS TEXT)
                              )) AS VARCHAR) AS sales_area,
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
                      CAST((
                        CAST((
                          0
                        ) AS DECIMAL)
                      ) AS DECIMAL(18, 0)) AS sellout_target,
                      CAST((
                        0
                      ) AS BIGINT) AS msld_actuals_count,
                      COUNT(*) AS msld_target_count,
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
                        edw_vw_th_gt_msl_distribution AS msld
                          LEFT JOIN edw_vw_os_time_dim AS cal
                            ON (
                              (
                                to_date(CAST((
                                  msld.survey_date
                                ) AS TIMESTAMPNTZ)) = to_date(CAST((
                                  cal.cal_date
                                ) AS TIMESTAMPNTZ))
                              )
                            )
                      )
                      LEFT JOIN (
                        SELECT DISTINCT
                          itg_th_dstrbtr_customer_dim.dstrbtr_id,
                          itg_th_dstrbtr_customer_dim.ar_cd,
                          itg_th_dstrbtr_customer_dim.sls_grp,
                          itg_th_dstrbtr_customer_dim.re_nm,
                          itg_th_dstrbtr_customer_dim.salesareaname
                        FROM itg_th_dstrbtr_customer_dim
                      ) AS cust
                        ON (
                          (
                            (
                              UPPER(CAST((
                                msld.distributor_id
                              ) AS TEXT)) = UPPER(CAST((
                                cust.dstrbtr_id
                              ) AS TEXT))
                            )
                            AND (
                              UPPER(CAST((
                                msld.store_id
                              ) AS TEXT)) = UPPER(CAST((
                                cust.ar_cd
                              ) AS TEXT))
                            )
                          )
                        )
                    )
                    WHERE
                      (
                        (
                          UPPER(CAST((
                            msld.distributor_id
                          ) AS TEXT)) IN (
                            SELECT DISTINCT
                              UPPER(CAST((
                                itg_th_gt_dstrbtr_control.distributor_id
                              ) AS TEXT)) AS distributor_id
                            FROM itg_th_gt_dstrbtr_control
                            WHERE
                              (
                                UPPER(CAST((
                                  itg_th_gt_dstrbtr_control.msl_flag
                                ) AS TEXT)) = CAST((
                                  CAST('Y' AS VARCHAR)
                                ) AS TEXT)
                              )
                          )
                        )
                        AND (
                          UPPER(msld.valid_flag) = CAST((
                            CAST('Y' AS VARCHAR)
                          ) AS TEXT)
                        )
                      )
                    GROUP BY
                      msld.cntry_cd,
                      CAST((
                        cal.cal_mnth_id
                      ) AS VARCHAR),
                      cal.cal_year,
                      cal.cal_mnth_no,
                      msld.distributor_id,
                      cust.re_nm,
                      cust.sls_grp,
                      cust.salesareaname
),

th_gt_schedule as 
      (    SELECT
                    CAST('ACTUAL' AS VARCHAR) AS identifier,
                    "call".cntry_cd,
                    CAST('THB' AS VARCHAR) AS crncy_cd,
                    CAST('Thailand' AS VARCHAR) AS cntry_nm,
                    CAST((
                      cal.cal_mnth_id
                    ) AS VARCHAR) AS year_month,
                    cal.cal_year AS "year",
                    cal.cal_mnth_no AS "month",
                    "call".distributor_id,
                    COALESCE(cust.re_nm, CAST('N/A' AS VARCHAR)) AS store_type,
                    CAST((
                              CONCAT(
                                  CAST((COALESCE(cust.sls_grp, CAST('N/A' AS VARCHAR))) AS TEXT) 
                                  , CAST((CAST('-' AS VARCHAR)) AS TEXT) 
                                  , CAST((COALESCE(cust.salesareaname, CAST('N/A' AS VARCHAR))) AS TEXT)
                              )) AS VARCHAR) AS sales_area,
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
                    "call".planned_call_count,
                    "call".visited_call_count,
                    "call".effective_call_count,
                    CAST((
                      0
                    ) AS BIGINT) AS sales_order_count,
                    CAST((
                      0
                    ) AS BIGINT) AS on_time_count,
                    CAST((
                      0
                    ) AS BIGINT) AS in_full_count,
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
                      (
                        SELECT
                          pln.cntry_cd,
                          pln.distributor_id,
                          pln.sales_rep_id,
                          pln.store_id,
                          pln.planned_call,
                          COUNT(DISTINCT pln.planned_call) AS planned_call_count,
                          COUNT(DISTINCT vst.visited_call) AS visited_call_count,
                          COUNT(DISTINCT eff.effective_call) AS effective_call_count
                        FROM (
                          (
                            (
                              SELECT
                                sch.cntry_cd,
                                sch.distributor_id,
                                sch.sales_rep_id,
                                rd.store_id,
                                sch.schedule_date AS planned_call
                              FROM (
                                (
                                  (
                                    edw_vw_th_gt_schedule AS sch
                                      JOIN (
                                        SELECT
                                          edw_vw_th_gt_route.cntry_cd,
                                          edw_vw_th_gt_route.crncy_cd,
                                          edw_vw_th_gt_route.route_id,
                                          edw_vw_th_gt_route.store_id,
                                          edw_vw_th_gt_route.routeno,
                                          edw_vw_th_gt_route.distributor_id,
                                          edw_vw_th_gt_route.ship_to,
                                          edw_vw_th_gt_route.contact_person,
                                          edw_vw_th_gt_route.route_description,
                                          edw_vw_th_gt_route.isactive,
                                          edw_vw_th_gt_route.sales_rep_id,
                                          edw_vw_th_gt_route.routecode,
                                          edw_vw_th_gt_route.route_code_desc,
                                          edw_vw_th_gt_route.valid_flag,
                                          edw_vw_th_gt_route.effective_start_date,
                                          MAX(edw_vw_th_gt_route.effective_end_date) AS effective_end_date
                                        FROM edw_vw_th_gt_route
                                        GROUP BY
                                          edw_vw_th_gt_route.cntry_cd,
                                          edw_vw_th_gt_route.crncy_cd,
                                          edw_vw_th_gt_route.route_id,
                                          edw_vw_th_gt_route.store_id,
                                          edw_vw_th_gt_route.routeno,
                                          edw_vw_th_gt_route.distributor_id,
                                          edw_vw_th_gt_route.ship_to,
                                          edw_vw_th_gt_route.contact_person,
                                          edw_vw_th_gt_route.route_description,
                                          edw_vw_th_gt_route.isactive,
                                          edw_vw_th_gt_route.sales_rep_id,
                                          edw_vw_th_gt_route.routecode,
                                          edw_vw_th_gt_route.route_code_desc,
                                          edw_vw_th_gt_route.valid_flag,
                                          edw_vw_th_gt_route.effective_start_date
                                      ) AS rd
                                        ON (
                                          (
                                            (
                                              (
                                                (
                                                  (
                                                    (
                                                      CAST((
                                                        sch.route_id
                                                      ) AS TEXT) = CAST((
                                                        rd.route_id
                                                      ) AS TEXT)
                                                    )
                                                    AND (
                                                      UPPER(CAST((
                                                        sch.distributor_id
                                                      ) AS TEXT)) = UPPER(CAST((
                                                        rd.distributor_id
                                                      ) AS TEXT))
                                                    )
                                                  )
                                                  AND (
                                                    UPPER(CAST((
                                                      sch.sales_rep_id
                                                    ) AS TEXT)) = UPPER(CAST((
                                                      rd.sales_rep_id
                                                    ) AS TEXT))
                                                  )
                                                )
                                                AND (
                                                  to_date(CAST((
                                                    sch.schedule_date
                                                  ) AS TIMESTAMPNTZ)) >= to_date(CAST((
                                                    rd.effective_start_date
                                                  ) AS TIMESTAMPNTZ))
                                                )
                                              )
                                              AND (
                                                to_date(CAST((
                                                  sch.schedule_date
                                                ) AS TIMESTAMPNTZ)) <= to_date(CAST((
                                                  rd.effective_end_date
                                                ) AS TIMESTAMPNTZ))
                                              )
                                            )
                                            AND (
                                              UPPER(rd.valid_flag) = CAST((
                                                CAST('Y' AS VARCHAR)
                                              ) AS TEXT)
                                            )
                                          )
                                        )
                                  )
                                  LEFT JOIN edw_vw_os_time_dim AS cal
                                    ON (
                                      (
                                        to_date(CAST((
                                          sch.schedule_date
                                        ) AS TIMESTAMPNTZ)) = to_date(CAST((
                                          cal.cal_date
                                        ) AS TIMESTAMPNTZ))
                                      )
                                    )
                                )
                                JOIN itg_th_dstrbtr_customer_dim_snapshot AS csnap
                                  ON (
                                    (
                                      (
                                        SUBSTRING(CAST((
                                          CAST((
                                            sch.schedule_date
                                          ) AS VARCHAR)
                                        ) AS TEXT), 1, 7) = SUBSTRING(CAST((
                                          CAST((
                                            csnap.snap_shot_dt
                                          ) AS VARCHAR)
                                        ) AS TEXT), 1, 7)
                                      )
                                      AND (
                                        (
                                          CAST((
                                            sch.distributor_id
                                          ) AS TEXT) || CAST((
                                            rd.store_id
                                          ) AS TEXT)
                                        ) = (
                                          CAST((
                                            csnap.dstrbtr_id
                                          ) AS TEXT) || CAST((
                                            csnap.ar_cd
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
                                      UPPER(CAST((
                                        sch.approved
                                      ) AS TEXT)) = CAST((
                                        CAST('Y' AS VARCHAR)
                                      ) AS TEXT)
                                    )
                                    AND (
                                      csnap.actv_status = 1
                                    )
                                  )
                                  AND (
                                    sch.schedule_date <= CONVERT_TIMEZONE('Asia/Bangkok', CURRENT_TIMESTAMP::timestamp_ntz(9))
                                  )
                                )
                            ) AS pln
                            LEFT JOIN (
                              SELECT DISTINCT
                                v1.cntry_cd,
                                v1.distributor_id,
                                v1.sales_rep_id,
                                v1.store_id,
                                v1.actual_date_visited AS visited_call
                              FROM edw_vw_th_gt_visit AS v1
                              WHERE
                                (
                                  (
                                    (
                                      (
                                        CAST((
                                          v1.time_plan
                                        ) AS TEXT) <> CAST((
                                          CAST('88888' AS VARCHAR)
                                        ) AS TEXT)
                                      )
                                      AND (
                                        CAST((
                                          v1.time_plan
                                        ) AS TEXT) <> CAST((
                                          CAST('99999' AS VARCHAR)
                                        ) AS TEXT)
                                      )
                                    )
                                    AND (
                                      NOT v1.actual_date_visited IS NULL
                                    )
                                  )
                                  AND (
                                    UPPER(v1.valid_flag) = CAST((
                                      CAST('Y' AS VARCHAR)
                                    ) AS TEXT)
                                  )
                                )
                            ) AS vst
                              ON (
                                (
                                  (
                                    (
                                      (
                                        UPPER(CAST((
                                          pln.distributor_id
                                        ) AS TEXT)) = UPPER(CAST((
                                          vst.distributor_id
                                        ) AS TEXT))
                                      )
                                      AND (
                                        UPPER(CAST((
                                          pln.store_id
                                        ) AS TEXT)) = UPPER(CAST((
                                          vst.store_id
                                        ) AS TEXT))
                                      )
                                    )
                                    AND (
                                      UPPER(CAST((
                                        pln.sales_rep_id
                                      ) AS TEXT)) = UPPER(CAST((
                                        vst.sales_rep_id
                                      ) AS TEXT))
                                    )
                                  )
                                  AND (
                                    to_date(CAST((
                                      pln.planned_call
                                    ) AS TIMESTAMPNTZ)) = to_date(CAST((
                                      vst.visited_call
                                    ) AS TIMESTAMPNTZ))
                                  )
                                )
                              )
                          )
                          LEFT JOIN (
                            SELECT DISTINCT
                              v1.cntry_cd,
                              v1.distributor_id,
                              v1.sales_rep_id,
                              v1.store_id,
                              v1.actual_date_visited AS effective_call
                            FROM edw_vw_th_gt_visit AS v1
                            WHERE
                              (
                                (
                                  (
                                    (
                                      EXISTS(
                                        SELECT
                                          1
                                        FROM edw_vw_th_gt_sales_order AS so
                                        WHERE
                                          (
                                            (
                                              (
                                                (
                                                  (
                                                    (
                                                      UPPER(CAST((
                                                        v1.distributor_id
                                                      ) AS TEXT)) = UPPER(TRIM(CAST((
                                                        so.distributor_id
                                                      ) AS TEXT)))
                                                    )
                                                    AND (
                                                      UPPER(CAST((
                                                        v1.store_id
                                                      ) AS TEXT)) = UPPER(TRIM(CAST((
                                                        so.store_id
                                                      ) AS TEXT)))
                                                    )
                                                  )
                                                  AND (
                                                    UPPER(RIGHT(CAST((
                                                      v1.sales_rep_id
                                                    ) AS TEXT), 4)) = UPPER(TRIM(CAST((
                                                      so.sales_rep_id
                                                    ) AS TEXT)))
                                                  )
                                                )
                                                AND (
                                                  to_date(CAST((
                                                    v1.actual_date_visited
                                                  ) AS TIMESTAMPNTZ)) = to_date(CAST((
                                                    so.sales_order_date
                                                  ) AS TIMESTAMPNTZ))
                                                )
                                              )
                                              AND (
                                                (
                                                  NOT TRIM(CAST((
                                                    so.invoice_no
                                                  ) AS TEXT)) IS NULL
                                                )
                                                OR (
                                                  TRIM(CAST((
                                                    so.invoice_no
                                                  ) AS TEXT)) <> CAST((
                                                    CAST('' AS VARCHAR)
                                                  ) AS TEXT)
                                                )
                                              )
                                            )
                                            AND (
                                              UPPER(so.valid_flag) = CAST((
                                                CAST('Y' AS VARCHAR)
                                              ) AS TEXT)
                                            )
                                          )
                                      )
                                    )
                                    AND (
                                      CAST((
                                        v1.time_plan
                                      ) AS TEXT) <> CAST((
                                        CAST('99999' AS VARCHAR)
                                      ) AS TEXT)
                                    )
                                  )
                                  AND (
                                    NOT v1.actual_date_visited IS NULL
                                  )
                                )
                                AND (
                                  UPPER(v1.valid_flag) = CAST((
                                    CAST('Y' AS VARCHAR)
                                  ) AS TEXT)
                                )
                              )
                          ) AS eff
                            ON (
                              (
                                (
                                  (
                                    (
                                      UPPER(CAST((
                                        pln.distributor_id
                                      ) AS TEXT)) = UPPER(CAST((
                                        eff.distributor_id
                                      ) AS TEXT))
                                    )
                                    AND (
                                      UPPER(CAST((
                                        pln.store_id
                                      ) AS TEXT)) = UPPER(CAST((
                                        eff.store_id
                                      ) AS TEXT))
                                    )
                                  )
                                  AND (
                                    UPPER(CAST((
                                      pln.sales_rep_id
                                    ) AS TEXT)) = UPPER(CAST((
                                      eff.sales_rep_id
                                    ) AS TEXT))
                                  )
                                )
                                AND (
                                  to_date(CAST((
                                    pln.planned_call
                                  ) AS TIMESTAMPNTZ)) = to_date(CAST((
                                    eff.effective_call
                                  ) AS TIMESTAMPNTZ))
                                )
                              )
                            )
                        )
                        GROUP BY
                          pln.cntry_cd,
                          pln.distributor_id,
                          pln.sales_rep_id,
                          pln.store_id,
                          pln.planned_call
                      ) AS "call"
                      LEFT JOIN edw_vw_os_time_dim AS cal
                        ON (
                          (
                            to_date(CAST((
                              "call".planned_call
                            ) AS TIMESTAMPNTZ)) = to_date(CAST((
                              cal.cal_date
                            ) AS TIMESTAMPNTZ))
                          )
                        )
                    )
                    LEFT JOIN (
                      SELECT DISTINCT
                        itg_th_dstrbtr_customer_dim.dstrbtr_id,
                        itg_th_dstrbtr_customer_dim.ar_cd,
                        itg_th_dstrbtr_customer_dim.sls_grp,
                        itg_th_dstrbtr_customer_dim.re_nm,
                        itg_th_dstrbtr_customer_dim.salesareaname
                      FROM itg_th_dstrbtr_customer_dim
                    ) AS cust
                      ON (
                        (
                          (
                            UPPER(CAST((
                              "call".distributor_id
                            ) AS TEXT)) = UPPER(CAST((
                              cust.dstrbtr_id
                            ) AS TEXT))
                          )
                          AND (
                            UPPER(CAST((
                              "call".store_id
                            ) AS TEXT)) = UPPER(CAST((
                              cust.ar_cd
                            ) AS TEXT))
                          )
                        )
                      )
                  )
                  WHERE
                    (
                      (
                        UPPER(CAST((
                          "call".distributor_id
                        ) AS TEXT)) IN (
                          SELECT DISTINCT
                            UPPER(CAST((
                              itg_th_gt_dstrbtr_control.distributor_id
                            ) AS TEXT)) AS distributor_id
                          FROM itg_th_gt_dstrbtr_control
                          WHERE
                            (
                              UPPER(CAST((
                                itg_th_gt_dstrbtr_control.call_flag
                              ) AS TEXT)) = CAST((
                                CAST('Y' AS VARCHAR)
                              ) AS TEXT)
                            )
                        )
                      )
                      AND (
                        UPPER(CAST((
                          COALESCE(cust.re_nm, CAST('N/A' AS VARCHAR))
                        ) AS TEXT)) <> CAST((
                          CAST('SUPERMARKET' AS VARCHAR)
                        ) AS TEXT)
                      )
                    )
                             
                UNION ALL             
                           
                SELECT
                  CAST('ACTUAL' AS VARCHAR) AS identifier,
                  "call".cntry_cd,
                  CAST('THB' AS VARCHAR) AS crncy_cd,
                  CAST('Thailand' AS VARCHAR) AS cntry_nm,
                  CAST((
                    cal.cal_mnth_id
                  ) AS VARCHAR) AS year_month,
                  cal.cal_year AS "year",
                  cal.cal_mnth_no AS "month",
                  "call".distributor_id,
                  COALESCE(cust.re_nm, CAST('N/A' AS VARCHAR)) AS store_type,
                  CAST((
                              CONCAT(
                                  CAST((COALESCE(cust.sls_grp, CAST('N/A' AS VARCHAR))) AS TEXT) 
                                  , CAST((CAST('-' AS VARCHAR)) AS TEXT) 
                                  , CAST((COALESCE(cust.salesareaname, CAST('N/A' AS VARCHAR))) AS TEXT)
                              )) AS VARCHAR
                        ) AS sales_area,
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
                  "call".planned_call_count,
                  "call".visited_call_count,
                  "call".effective_call_count,
                  CAST((
                    0
                  ) AS BIGINT) AS sales_order_count,
                  CAST((
                    0
                  ) AS BIGINT) AS on_time_count,
                  CAST((
                    0
                  ) AS BIGINT) AS in_full_count,
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
                    (
                      SELECT
                        pln.cntry_cd,
                        pln.distributor_id,
                        pln.sales_rep_id,
                        pln.store_id,
                        pln.planned_call,
                        COUNT(DISTINCT pln.planned_call) AS planned_call_count,
                        COUNT(DISTINCT vst.visited_call) AS visited_call_count,
                        COUNT(DISTINCT eff.effective_call) AS effective_call_count
                      FROM (
                        (
                          (
                            SELECT
                              sch.cntry_cd,
                              sch.distributor_id,
                              sch.sales_rep_id,
                              rd.store_id,
                              sch.schedule_date AS planned_call
                            FROM (
                              (
                                (
                                  edw_vw_th_gt_schedule AS sch
                                    JOIN (
                                      SELECT
                                        edw_vw_th_gt_route.cntry_cd,
                                        edw_vw_th_gt_route.crncy_cd,
                                        edw_vw_th_gt_route.route_id,
                                        edw_vw_th_gt_route.store_id,
                                        edw_vw_th_gt_route.routeno,
                                        edw_vw_th_gt_route.distributor_id,
                                        edw_vw_th_gt_route.ship_to,
                                        edw_vw_th_gt_route.contact_person,
                                        edw_vw_th_gt_route.route_description,
                                        edw_vw_th_gt_route.isactive,
                                        edw_vw_th_gt_route.sales_rep_id,
                                        edw_vw_th_gt_route.routecode,
                                        edw_vw_th_gt_route.route_code_desc,
                                        edw_vw_th_gt_route.valid_flag,
                                        edw_vw_th_gt_route.effective_start_date,
                                        MAX(edw_vw_th_gt_route.effective_end_date) AS effective_end_date
                                      FROM edw_vw_th_gt_route
                                      GROUP BY
                                        edw_vw_th_gt_route.cntry_cd,
                                        edw_vw_th_gt_route.crncy_cd,
                                        edw_vw_th_gt_route.route_id,
                                        edw_vw_th_gt_route.store_id,
                                        edw_vw_th_gt_route.routeno,
                                        edw_vw_th_gt_route.distributor_id,
                                        edw_vw_th_gt_route.ship_to,
                                        edw_vw_th_gt_route.contact_person,
                                        edw_vw_th_gt_route.route_description,
                                        edw_vw_th_gt_route.isactive,
                                        edw_vw_th_gt_route.sales_rep_id,
                                        edw_vw_th_gt_route.routecode,
                                        edw_vw_th_gt_route.route_code_desc,
                                        edw_vw_th_gt_route.valid_flag,
                                        edw_vw_th_gt_route.effective_start_date
                                    ) AS rd
                                      ON (
                                        (
                                          (
                                            (
                                              (
                                                (
                                                  (
                                                    CAST((
                                                      sch.route_id
                                                    ) AS TEXT) = CAST((
                                                      rd.route_id
                                                    ) AS TEXT)
                                                  )
                                                  AND (
                                                    UPPER(CAST((
                                                      sch.distributor_id
                                                    ) AS TEXT)) = UPPER(CAST((
                                                      rd.distributor_id
                                                    ) AS TEXT))
                                                  )
                                                )
                                                AND (
                                                  UPPER(CAST((
                                                    sch.sales_rep_id
                                                  ) AS TEXT)) = UPPER(CAST((
                                                    rd.sales_rep_id
                                                  ) AS TEXT))
                                                )
                                              )
                                              AND (
                                                to_date(CAST((
                                                  sch.schedule_date
                                                ) AS TIMESTAMPNTZ)) >= to_date(CAST((
                                                  rd.effective_start_date
                                                ) AS TIMESTAMPNTZ))
                                              )
                                            )
                                            AND (
                                              to_date(CAST((
                                                sch.schedule_date
                                              ) AS TIMESTAMPNTZ)) <= to_date(CAST((
                                                rd.effective_end_date
                                              ) AS TIMESTAMPNTZ))
                                            )
                                          )
                                          AND (
                                            UPPER(rd.valid_flag) = CAST((
                                              CAST('Y' AS VARCHAR)
                                            ) AS TEXT)
                                          )
                                        )
                                      )
                                )
                                LEFT JOIN edw_vw_os_time_dim AS cal
                                  ON (
                                    (
                                      to_date(CAST((
                                        sch.schedule_date
                                      ) AS TIMESTAMPNTZ)) = to_date(CAST((
                                        cal.cal_date
                                      ) AS TIMESTAMPNTZ))
                                    )
                                  )
                              )
                              JOIN itg_th_dstrbtr_customer_dim_snapshot AS csnap
                                ON (
                                  (
                                    (
                                      SUBSTRING(CAST((
                                        CAST((
                                          sch.schedule_date
                                        ) AS VARCHAR)
                                      ) AS TEXT), 1, 7) = SUBSTRING(CAST((
                                        CAST((
                                          csnap.snap_shot_dt
                                        ) AS VARCHAR)
                                      ) AS TEXT), 1, 7)
                                    )
                                    AND (
                                      (
                                        CAST((
                                          sch.distributor_id
                                        ) AS TEXT) || CAST((
                                          rd.store_id
                                        ) AS TEXT)
                                      ) = (
                                        CAST((
                                          csnap.dstrbtr_id
                                        ) AS TEXT) || CAST((
                                          csnap.ar_cd
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
                                    UPPER(CAST((
                                      sch.approved
                                    ) AS TEXT)) = CAST((
                                      CAST('Y' AS VARCHAR)
                                    ) AS TEXT)
                                  )
                                  AND (
                                    csnap.actv_status = 1
                                  )
                                )
                                AND (
                                  sch.schedule_date <= CONVERT_TIMEZONE('Asia/Bangkok', CURRENT_TIMESTAMP::timestamp_ntz(9))
                                )
                              )
                          ) AS pln
                          LEFT JOIN (
                            SELECT DISTINCT
                              v1.cntry_cd,
                              v1.distributor_id,
                              v1.sales_rep_id,
                              v1.store_id,
                              v1.actual_date_visited AS visited_call
                            FROM edw_vw_th_gt_visit AS v1
                            WHERE
                              (
                                (
                                  (
                                    (
                                      CAST((
                                        v1.time_plan
                                      ) AS TEXT) <> CAST((
                                        CAST('88888' AS VARCHAR)
                                      ) AS TEXT)
                                    )
                                    AND (
                                      CAST((
                                        v1.time_plan
                                      ) AS TEXT) <> CAST((
                                        CAST('99999' AS VARCHAR)
                                      ) AS TEXT)
                                    )
                                  )
                                  AND (
                                    NOT v1.actual_date_visited IS NULL
                                  )
                                )
                                AND (
                                  UPPER(v1.valid_flag) = CAST((
                                    CAST('Y' AS VARCHAR)
                                  ) AS TEXT)
                                )
                              )
                          ) AS vst
                            ON (
                              (
                                (
                                  (
                                    (
                                      UPPER(CAST((
                                        pln.distributor_id
                                      ) AS TEXT)) = UPPER(CAST((
                                        vst.distributor_id
                                      ) AS TEXT))
                                    )
                                    AND (
                                      UPPER(CAST((
                                        pln.store_id
                                      ) AS TEXT)) = UPPER(CAST((
                                        vst.store_id
                                      ) AS TEXT))
                                    )
                                  )
                                  AND (
                                    UPPER(CAST((
                                      pln.sales_rep_id
                                    ) AS TEXT)) = UPPER(CAST((
                                      vst.sales_rep_id
                                    ) AS TEXT))
                                  )
                                )
                                AND (
                                  to_date(CAST((
                                    pln.planned_call
                                  ) AS TIMESTAMPNTZ)) = to_date(CAST((
                                    vst.visited_call
                                  ) AS TIMESTAMPNTZ))
                                )
                              )
                            )
                        )
                        LEFT JOIN (
                          SELECT DISTINCT
                            v1.cntry_cd,
                            v1.distributor_id,
                            v1.sales_rep_id,
                            v1.store_id,
                            v1.actual_date_visited AS effective_call
                          FROM edw_vw_th_gt_visit AS v1
                          WHERE
                            (
                              (
                                (
                                  (
                                    EXISTS(
                                      SELECT
                                        1
                                      FROM edw_vw_th_gt_sales_order AS so
                                      WHERE
                                        (
                                          (
                                            (
                                              (
                                                (
                                                  (
                                                    UPPER(CAST((
                                                      v1.distributor_id
                                                    ) AS TEXT)) = UPPER(TRIM(CAST((
                                                      so.distributor_id
                                                    ) AS TEXT)))
                                                  )
                                                  AND (
                                                    UPPER(CAST((
                                                      v1.store_id
                                                    ) AS TEXT)) = UPPER(TRIM(CAST((
                                                      so.store_id
                                                    ) AS TEXT)))
                                                  )
                                                )
                                                AND (
                                                  UPPER(RIGHT(CAST((
                                                    v1.sales_rep_id
                                                  ) AS TEXT), 4)) = UPPER(TRIM(CAST((
                                                    so.sales_rep_id
                                                  ) AS TEXT)))
                                                )
                                              )
                                              AND (
                                                to_date(CAST((
                                                  v1.actual_date_visited
                                                ) AS TIMESTAMPNTZ)) = to_date(CAST((
                                                  so.sales_order_date
                                                ) AS TIMESTAMPNTZ))
                                              )
                                            )
                                            AND (
                                              (
                                                NOT TRIM(CAST((
                                                  so.invoice_no
                                                ) AS TEXT)) IS NULL
                                              )
                                              OR (
                                                TRIM(CAST((
                                                  so.invoice_no
                                                ) AS TEXT)) <> CAST((
                                                  CAST('' AS VARCHAR)
                                                ) AS TEXT)
                                              )
                                            )
                                          )
                                          AND (
                                            UPPER(so.valid_flag) = CAST((
                                              CAST('Y' AS VARCHAR)
                                            ) AS TEXT)
                                          )
                                        )
                                    )
                                  )
                                  AND (
                                    CAST((
                                      v1.time_plan
                                    ) AS TEXT) <> CAST((
                                      CAST('99999' AS VARCHAR)
                                    ) AS TEXT)
                                  )
                                )
                                AND (
                                  NOT v1.actual_date_visited IS NULL
                                )
                              )
                              AND (
                                UPPER(v1.valid_flag) = CAST((
                                  CAST('Y' AS VARCHAR)
                                ) AS TEXT)
                              )
                            )
                        ) AS eff
                          ON (
                            (
                              (
                                (
                                  (
                                    (
                                      UPPER(CAST((
                                        pln.distributor_id
                                      ) AS TEXT)) = UPPER(CAST((
                                        eff.distributor_id
                                      ) AS TEXT))
                                    )
                                    AND (
                                      UPPER(CAST((
                                        pln.store_id
                                      ) AS TEXT)) = UPPER(CAST((
                                        eff.store_id
                                      ) AS TEXT))
                                    )
                                  )
                                  AND (
                                    UPPER(CAST((
                                      pln.sales_rep_id
                                    ) AS TEXT)) = UPPER(CAST((
                                      eff.sales_rep_id
                                    ) AS TEXT))
                                  )
                                )
                                AND (
                                  DATEDIFF(
                                    DAY,
                                    CAST((
                                      to_date(CAST((
                                        pln.planned_call
                                      ) AS TIMESTAMPNTZ))
                                    ) AS TIMESTAMPNTZ),
                                    CAST((
                                      to_date(CAST((
                                        eff.effective_call
                                      ) AS TIMESTAMPNTZ))
                                    ) AS TIMESTAMPNTZ)
                                  ) >= 0
                                )
                              )
                              AND (
                                DATEDIFF(
                                  DAY,
                                  CAST((
                                    to_date(CAST((
                                      pln.planned_call
                                    ) AS TIMESTAMPNTZ))
                                  ) AS TIMESTAMPNTZ),
                                  CAST((
                                    to_date(CAST((
                                      eff.effective_call
                                    ) AS TIMESTAMPNTZ))
                                  ) AS TIMESTAMPNTZ)
                                ) <= 7
                              )
                            )
                          )
                      )
                      GROUP BY
                        pln.cntry_cd,
                        pln.distributor_id,
                        pln.sales_rep_id,
                        pln.store_id,
                        pln.planned_call
                    ) AS "call"
                    LEFT JOIN edw_vw_os_time_dim AS cal
                      ON (
                        (
                          to_date(CAST((
                            "call".planned_call
                          ) AS TIMESTAMPNTZ)) = to_date(CAST((
                            cal.cal_date
                          ) AS TIMESTAMPNTZ))
                        )
                      )
                  )
                  LEFT JOIN (
                    SELECT DISTINCT
                      itg_th_dstrbtr_customer_dim.dstrbtr_id,
                      itg_th_dstrbtr_customer_dim.ar_cd,
                      itg_th_dstrbtr_customer_dim.sls_grp,
                      itg_th_dstrbtr_customer_dim.re_nm,
                      itg_th_dstrbtr_customer_dim.salesareaname
                    FROM itg_th_dstrbtr_customer_dim
                  ) AS cust
                    ON (
                      (
                        (
                          UPPER(CAST((
                            "call".distributor_id
                          ) AS TEXT)) = UPPER(CAST((
                            cust.dstrbtr_id
                          ) AS TEXT))
                        )
                        AND (
                          UPPER(CAST((
                            "call".store_id
                          ) AS TEXT)) = UPPER(CAST((
                            cust.ar_cd
                          ) AS TEXT))
                        )
                      )
                    )
                )
                WHERE
                  (
                    (
                      UPPER(CAST((
                        "call".distributor_id
                      ) AS TEXT)) IN (
                        SELECT DISTINCT
                          UPPER(CAST((
                            itg_th_gt_dstrbtr_control.distributor_id
                          ) AS TEXT)) AS distributor_id
                        FROM itg_th_gt_dstrbtr_control
                        WHERE
                          (
                            UPPER(CAST((
                              itg_th_gt_dstrbtr_control.call_flag
                            ) AS TEXT)) = CAST((
                              CAST('Y' AS VARCHAR)
                            ) AS TEXT)
                          )
                      )
                    )
                    AND (
                      UPPER(CAST((
                        COALESCE(cust.re_nm, CAST('N/A' AS VARCHAR))
                      ) AS TEXT)) = CAST((
                        CAST('SUPERMARKET' AS VARCHAR)
                      ) AS TEXT)
                    )
                  )
      ),

     cte4 AS (        
              SELECT
                CAST('ACTUAL' AS VARCHAR) AS identifier,
                "call".cntry_cd,
                CAST('THB' AS VARCHAR) AS crncy_cd,
                CAST('Thailand' AS VARCHAR) AS cntry_nm,
                CAST((
                  "call".mnth_id
                ) AS VARCHAR) AS year_month,
                CAST((
                  "call"."year"
                ) AS INT) AS "year",
                CAST((
                  "call"."month"
                ) AS INT) AS "month",
                "call".distributor_id,
                COALESCE(cust.re_nm, CAST('N/A' AS VARCHAR)) AS store_type,
                CAST((
                        CONCAT(
                                  CAST((COALESCE(cust.sls_grp, CAST('N/A' AS VARCHAR))) AS TEXT) 
                                  , CAST((CAST('-' AS VARCHAR)) AS TEXT) 
                                  , CAST((COALESCE(cust.salesareaname, CAST('N/A' AS VARCHAR))) AS TEXT)
                              )) AS VARCHAR) AS sales_area,
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
                ) AS BIGINT) AS in_full_count,
                CAST((
                  0
                ) AS BIGINT) AS otif_count,
                CAST((
                  0
                ) AS BIGINT) AS coverage_stores_count,
                CAST('0' AS VARCHAR) AS reactivate_stores_count,
                CAST('0' AS VARCHAR) AS inactive_stores_count,
                CAST('0' AS VARCHAR) AS active_stores_count,
                "call".route_com_all,
                "call".effective_call_all,
                "call".planned_store,
                "call".effective_store_in_plan,
                "call".effective_store_all,
                CAST((
                  0
                ) AS BIGINT) AS total_skus,
                CAST((
                  0
                ) AS BIGINT) AS total_stores
              FROM (
                (
                  SELECT
                    pln.cntry_cd,
                    pln.distributor_id,
                    pln.sales_rep_id,
                    pln.store_id,
                    "REPLACE"(
                      SUBSTRING(CAST((
                        CAST((
                          pln.planned_call
                        ) AS VARCHAR)
                      ) AS TEXT), 1, 7),
                      CAST((
                        CAST('-' AS VARCHAR)
                      ) AS TEXT),
                      CAST((
                        CAST('' AS VARCHAR)
                      ) AS TEXT)
                    ) AS mnth_id,
                    SUBSTRING(CAST((
                      CAST((
                        pln.planned_call
                      ) AS VARCHAR)
                    ) AS TEXT), 1, 4) AS "year",
                      CAST((
                        SUBSTRING(CAST((
                          CAST((
                            pln.planned_call
                          ) AS VARCHAR)
                        ) AS TEXT), 6, 2)
                      ) AS DECIMAL(18, 0)) AS "month",
                    COUNT(DISTINCT vst.visited_call) AS route_com_all,
                    COUNT(DISTINCT eff.effective_call) AS effective_call_all,
                    MAX(
                      CASE
                        WHEN (
                          (
                            vst.visited_call = eff.effective_call
                          )
                          AND (
                            pln.planned_call = eff.effective_call
                          )
                        )
                        THEN 1
                        ELSE 0
                      END
                    ) AS effective_call_in_plan,
                    1 AS planned_store,
                    CASE WHEN (
                      COUNT(DISTINCT eff.effective_call) <> 0
                    ) THEN 1 ELSE 0 END AS effective_store_all,
                    CASE
                      WHEN (
                        MAX(
                          CASE
                            WHEN (
                              (
                                vst.visited_call = eff.effective_call
                              )
                              AND (
                                pln.planned_call = eff.effective_call
                              )
                            )
                            THEN 1
                            ELSE 0
                          END
                        ) <> 0
                      )
                      THEN 1
                      ELSE 0
                    END AS effective_store_in_plan
                  FROM (
                    (
                      (
                        SELECT
                          sch.cntry_cd,
                          sch.distributor_id,
                          sch.sales_rep_id,
                          rd.store_id,
                          sch.schedule_date AS planned_call
                        FROM (
                          (
                            (
                              edw_vw_th_gt_schedule AS sch
                                JOIN (
                                  SELECT
                                    edw_vw_th_gt_route.cntry_cd,
                                    edw_vw_th_gt_route.crncy_cd,
                                    edw_vw_th_gt_route.route_id,
                                    edw_vw_th_gt_route.store_id,
                                    edw_vw_th_gt_route.routeno,
                                    edw_vw_th_gt_route.distributor_id,
                                    edw_vw_th_gt_route.ship_to,
                                    edw_vw_th_gt_route.contact_person,
                                    edw_vw_th_gt_route.route_description,
                                    edw_vw_th_gt_route.isactive,
                                    edw_vw_th_gt_route.sales_rep_id,
                                    edw_vw_th_gt_route.routecode,
                                    edw_vw_th_gt_route.route_code_desc,
                                    edw_vw_th_gt_route.valid_flag,
                                    edw_vw_th_gt_route.effective_start_date,
                                    MAX(edw_vw_th_gt_route.effective_end_date) AS effective_end_date
                                  FROM edw_vw_th_gt_route
                                  GROUP BY
                                    edw_vw_th_gt_route.cntry_cd,
                                    edw_vw_th_gt_route.crncy_cd,
                                    edw_vw_th_gt_route.route_id,
                                    edw_vw_th_gt_route.store_id,
                                    edw_vw_th_gt_route.routeno,
                                    edw_vw_th_gt_route.distributor_id,
                                    edw_vw_th_gt_route.ship_to,
                                    edw_vw_th_gt_route.contact_person,
                                    edw_vw_th_gt_route.route_description,
                                    edw_vw_th_gt_route.isactive,
                                    edw_vw_th_gt_route.sales_rep_id,
                                    edw_vw_th_gt_route.routecode,
                                    edw_vw_th_gt_route.route_code_desc,
                                    edw_vw_th_gt_route.valid_flag,
                                    edw_vw_th_gt_route.effective_start_date
                                ) AS rd
                                  ON (
                                    (
                                      (
                                        (
                                          (
                                            (
                                              (
                                                CAST((
                                                  sch.route_id
                                                ) AS TEXT) = CAST((
                                                  rd.route_id
                                                ) AS TEXT)
                                              )
                                              AND (
                                                UPPER(CAST((
                                                  sch.distributor_id
                                                ) AS TEXT)) = UPPER(CAST((
                                                  rd.distributor_id
                                                ) AS TEXT))
                                              )
                                            )
                                            AND (
                                              UPPER(CAST((
                                                sch.sales_rep_id
                                              ) AS TEXT)) = UPPER(CAST((
                                                rd.sales_rep_id
                                              ) AS TEXT))
                                            )
                                          )
                                          AND (
                                            to_date(CAST((
                                              sch.schedule_date
                                            ) AS TIMESTAMPNTZ)) >= to_date(CAST((
                                              rd.effective_start_date
                                            ) AS TIMESTAMPNTZ))
                                          )
                                        )
                                        AND (
                                          to_date(CAST((
                                            sch.schedule_date
                                          ) AS TIMESTAMPNTZ)) <= to_date(CAST((
                                            rd.effective_end_date
                                          ) AS TIMESTAMPNTZ))
                                        )
                                      )
                                      AND (
                                        UPPER(rd.valid_flag) = CAST((
                                          CAST('Y' AS VARCHAR)
                                        ) AS TEXT)
                                      )
                                    )
                                  )
                            )
                            LEFT JOIN edw_vw_os_time_dim AS cal
                              ON (
                                (
                                  to_date(CAST((
                                    sch.schedule_date
                                  ) AS TIMESTAMPNTZ)) = to_date(CAST((
                                    cal.cal_date
                                  ) AS TIMESTAMPNTZ))
                                )
                              )
                          )
                          JOIN itg_th_dstrbtr_customer_dim_snapshot AS csnap
                            ON (
                              (
                                (
                                  SUBSTRING(CAST((
                                    CAST((
                                      sch.schedule_date
                                    ) AS VARCHAR)
                                  ) AS TEXT), 1, 7) = SUBSTRING(CAST((
                                    CAST((
                                      csnap.snap_shot_dt
                                    ) AS VARCHAR)
                                  ) AS TEXT), 1, 7)
                                )
                                AND (
                                  (
                                    CAST((
                                      sch.distributor_id
                                    ) AS TEXT) || CAST((
                                      rd.store_id
                                    ) AS TEXT)
                                  ) = (
                                    CAST((
                                      csnap.dstrbtr_id
                                    ) AS TEXT) || CAST((
                                      csnap.ar_cd
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
                                UPPER(CAST((
                                  sch.approved
                                ) AS TEXT)) = CAST((
                                  CAST('Y' AS VARCHAR)
                                ) AS TEXT)
                              )
                              AND (
                                csnap.actv_status = 1
                              )
                            )
                            AND (
                              sch.schedule_date <= CONVERT_TIMEZONE('Asia/Bangkok', CURRENT_TIMESTAMP::timestamp_ntz(9))
                            )
                          )
                      ) AS pln
                      LEFT JOIN (
                        SELECT DISTINCT
                          v1.cntry_cd,
                          v1.distributor_id,
                          v1.sales_rep_id,
                          v1.store_id,
                          v1.actual_date_visited AS visited_call
                        FROM edw_vw_th_gt_visit AS v1
                        WHERE
                          (
                            (
                              (
                                CAST((
                                  v1.time_plan
                                ) AS TEXT) <> CAST((
                                  CAST('99999' AS VARCHAR)
                                ) AS TEXT)
                              )
                              AND (
                                NOT v1.actual_date_visited IS NULL
                              )
                            )
                            AND (
                              UPPER(v1.valid_flag) = CAST((
                                CAST('Y' AS VARCHAR)
                              ) AS TEXT)
                            )
                          )
                      ) AS vst
                        ON (
                          (
                            (
                              (
                                (
                                  UPPER(CAST((
                                    pln.distributor_id
                                  ) AS TEXT)) = UPPER(CAST((
                                    vst.distributor_id
                                  ) AS TEXT))
                                )
                                AND (
                                  UPPER(CAST((
                                    pln.store_id
                                  ) AS TEXT)) = UPPER(CAST((
                                    vst.store_id
                                  ) AS TEXT))
                                )
                              )
                              AND (
                                UPPER(CAST((
                                  pln.sales_rep_id
                                ) AS TEXT)) = UPPER(CAST((
                                  vst.sales_rep_id
                                ) AS TEXT))
                              )
                            )
                            AND (
                              SUBSTRING(CAST((
                                CAST((
                                  pln.planned_call
                                ) AS VARCHAR)
                              ) AS TEXT), 1, 7) = SUBSTRING(CAST((
                                CAST((
                                  vst.visited_call
                                ) AS VARCHAR)
                              ) AS TEXT), 1, 7)
                            )
                          )
                        )
                    )
                    LEFT JOIN (
                      SELECT DISTINCT
                        v1.cntry_cd,
                        v1.distributor_id,
                        v1.sales_rep_id,
                        v1.store_id,
                        v1.actual_date_visited AS effective_call
                      FROM edw_vw_th_gt_visit AS v1
                      WHERE
                        (
                          (
                            (
                              (
                                EXISTS(
                                  SELECT
                                    1
                                  FROM edw_vw_th_gt_sales_order AS so
                                  WHERE
                                    (
                                      (
                                        (
                                          (
                                            (
                                              (
                                                UPPER(CAST((
                                                  v1.distributor_id
                                                ) AS TEXT)) = UPPER(TRIM(CAST((
                                                  so.distributor_id
                                                ) AS TEXT)))
                                              )
                                              AND (
                                                UPPER(CAST((
                                                  v1.store_id
                                                ) AS TEXT)) = UPPER(TRIM(CAST((
                                                  so.store_id
                                                ) AS TEXT)))
                                              )
                                            )
                                            AND (
                                              UPPER(RIGHT(CAST((
                                                v1.sales_rep_id
                                              ) AS TEXT), 4)) = UPPER(TRIM(CAST((
                                                so.sales_rep_id
                                              ) AS TEXT)))
                                            )
                                          )
                                          AND (
                                            to_date(CAST((
                                              v1.actual_date_visited
                                            ) AS TIMESTAMPNTZ)) = to_date(CAST((
                                              so.sales_order_date
                                            ) AS TIMESTAMPNTZ))
                                          )
                                        )
                                        AND (
                                          (
                                            NOT TRIM(CAST((
                                              so.invoice_no
                                            ) AS TEXT)) IS NULL
                                          )
                                          OR (
                                            TRIM(CAST((
                                              so.invoice_no
                                            ) AS TEXT)) <> CAST((
                                              CAST('' AS VARCHAR)
                                            ) AS TEXT)
                                          )
                                        )
                                      )
                                      AND (
                                        UPPER(so.valid_flag) = CAST((
                                          CAST('Y' AS VARCHAR)
                                        ) AS TEXT)
                                      )
                                    )
                                )
                              )
                              AND (
                                CAST((
                                  v1.time_plan
                                ) AS TEXT) <> CAST((
                                  CAST('99999' AS VARCHAR)
                                ) AS TEXT)
                              )
                            )
                            AND (
                              NOT v1.actual_date_visited IS NULL
                            )
                          )
                          AND (
                            UPPER(v1.valid_flag) = CAST((
                              CAST('Y' AS VARCHAR)
                            ) AS TEXT)
                          )
                        )
                    ) AS eff
                      ON (
                        (
                          (
                            (
                              (
                                UPPER(CAST((
                                  pln.distributor_id
                                ) AS TEXT)) = UPPER(CAST((
                                  eff.distributor_id
                                ) AS TEXT))
                              )
                              AND (
                                UPPER(CAST((
                                  pln.store_id
                                ) AS TEXT)) = UPPER(CAST((
                                  eff.store_id
                                ) AS TEXT))
                              )
                            )
                            AND (
                              UPPER(CAST((
                                pln.sales_rep_id
                              ) AS TEXT)) = UPPER(CAST((
                                eff.sales_rep_id
                              ) AS TEXT))
                            )
                          )
                          AND (
                            SUBSTRING(CAST((
                              CAST((
                                pln.planned_call
                              ) AS VARCHAR)
                            ) AS TEXT), 1, 7) = SUBSTRING(CAST((
                              CAST((
                                eff.effective_call
                              ) AS VARCHAR)
                            ) AS TEXT), 1, 7)
                          )
                        )
                      )
                  )
                  GROUP BY
                    "REPLACE"(
                      SUBSTRING(CAST((
                        CAST((
                          pln.planned_call
                        ) AS VARCHAR)
                      ) AS TEXT), 1, 7),
                      CAST((
                        CAST('-' AS VARCHAR)
                      ) AS TEXT),
                      CAST((
                        CAST('' AS VARCHAR)
                      ) AS TEXT)
                    ),
                    SUBSTRING(CAST((
                      CAST((
                        pln.planned_call
                      ) AS VARCHAR)
                    ) AS TEXT), 1, 4),
                      CAST((
                        SUBSTRING(CAST((
                          CAST((
                            pln.planned_call
                          ) AS VARCHAR)
                        ) AS TEXT), 6, 2)
                      ) AS DECIMAL(18, 0))
                    ,
                    pln.cntry_cd,
                    pln.distributor_id,
                    pln.sales_rep_id,
                    pln.store_id
                ) AS "call"
                LEFT JOIN (
                  SELECT DISTINCT
                    itg_th_dstrbtr_customer_dim.dstrbtr_id,
                    itg_th_dstrbtr_customer_dim.ar_cd,
                    itg_th_dstrbtr_customer_dim.sls_grp,
                    itg_th_dstrbtr_customer_dim.re_nm,
                    itg_th_dstrbtr_customer_dim.salesareaname
                  FROM itg_th_dstrbtr_customer_dim
                ) AS cust
                  ON (
                    (
                      (
                        UPPER(CAST((
                          "call".distributor_id
                        ) AS TEXT)) = UPPER(CAST((
                          cust.dstrbtr_id
                        ) AS TEXT))
                      )
                      AND (
                        UPPER(CAST((
                          "call".store_id
                        ) AS TEXT)) = UPPER(CAST((
                          cust.ar_cd
                        ) AS TEXT))
                      )
                    )
                  )
              )
              WHERE
                (
                  UPPER(CAST((
                    "call".distributor_id
                  ) AS TEXT)) IN (
                    SELECT DISTINCT
                      UPPER(CAST((
                        itg_th_gt_dstrbtr_control.distributor_id
                      ) AS TEXT)) AS distributor_id
                    FROM itg_th_gt_dstrbtr_control
                    WHERE
                      (
                        UPPER(CAST((
                          itg_th_gt_dstrbtr_control.call_flag
                        ) AS TEXT)) = CAST((
                          CAST('Y' AS VARCHAR)
                        ) AS TEXT)
                      )
                  )
                )

                UNION ALL

                SELECT
                  CAST('ACTUAL' AS VARCHAR) AS identifier,
                  "call".cntry_cd,
                  CAST('THB' AS VARCHAR) AS crncy_cd,
                  CAST('Thailand' AS VARCHAR) AS cntry_nm,
                  CAST((
                    cal.cal_mnth_id
                  ) AS VARCHAR) AS year_month,
                  cal.cal_year AS "year",
                  cal.cal_mnth_no AS "month",
                  "call".distributor_id,
                  COALESCE(cust.re_nm, CAST('N/A' AS VARCHAR)) AS store_type,
                  CAST((
                              CONCAT(
                                  CAST((COALESCE(cust.sls_grp, CAST('N/A' AS VARCHAR))) AS TEXT) 
                                  , CAST((CAST('-' AS VARCHAR)) AS TEXT) 
                                  , CAST((COALESCE(cust.salesareaname, CAST('N/A' AS VARCHAR))) AS TEXT)
                              )) AS VARCHAR) AS sales_area,
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
                  "call".planned_call_count,
                  "call".visited_call_count,
                  "call".effective_call_count,
                  CAST((
                    0
                  ) AS BIGINT) AS sales_order_count,
                  CAST((
                    0
                  ) AS BIGINT) AS on_time_count,
                  CAST((
                    0
                  ) AS BIGINT) AS in_full_count,
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
                    (
                      SELECT
                        pln.cntry_cd,
                        pln.distributor_id,
                        pln.sales_rep_id,
                        pln.store_id,
                        pln.planned_call,
                        COUNT(DISTINCT pln.planned_call) AS planned_call_count,
                        COUNT(DISTINCT vst.visited_call) AS visited_call_count,
                        COUNT(DISTINCT eff.effective_call) AS effective_call_count
                      FROM (
                        (
                          (
                            SELECT
                              sch.cntry_cd,
                              sch.distributor_id,
                              sch.sales_rep_id,
                              rd.store_id,
                              sch.schedule_date AS planned_call
                            FROM (
                              (
                                (
                                  edw_vw_th_gt_schedule AS sch
                                    JOIN (
                                      SELECT
                                        edw_vw_th_gt_route.cntry_cd,
                                        edw_vw_th_gt_route.crncy_cd,
                                        edw_vw_th_gt_route.route_id,
                                        edw_vw_th_gt_route.store_id,
                                        edw_vw_th_gt_route.routeno,
                                        edw_vw_th_gt_route.distributor_id,
                                        edw_vw_th_gt_route.ship_to,
                                        edw_vw_th_gt_route.contact_person,
                                        edw_vw_th_gt_route.route_description,
                                        edw_vw_th_gt_route.isactive,
                                        edw_vw_th_gt_route.sales_rep_id,
                                        edw_vw_th_gt_route.routecode,
                                        edw_vw_th_gt_route.route_code_desc,
                                        edw_vw_th_gt_route.valid_flag,
                                        edw_vw_th_gt_route.effective_start_date,
                                        MAX(edw_vw_th_gt_route.effective_end_date) AS effective_end_date
                                      FROM edw_vw_th_gt_route
                                      GROUP BY
                                        edw_vw_th_gt_route.cntry_cd,
                                        edw_vw_th_gt_route.crncy_cd,
                                        edw_vw_th_gt_route.route_id,
                                        edw_vw_th_gt_route.store_id,
                                        edw_vw_th_gt_route.routeno,
                                        edw_vw_th_gt_route.distributor_id,
                                        edw_vw_th_gt_route.ship_to,
                                        edw_vw_th_gt_route.contact_person,
                                        edw_vw_th_gt_route.route_description,
                                        edw_vw_th_gt_route.isactive,
                                        edw_vw_th_gt_route.sales_rep_id,
                                        edw_vw_th_gt_route.routecode,
                                        edw_vw_th_gt_route.route_code_desc,
                                        edw_vw_th_gt_route.valid_flag,
                                        edw_vw_th_gt_route.effective_start_date
                                    ) AS rd
                                      ON (
                                        (
                                          (
                                            (
                                              (
                                                (
                                                  (
                                                    CAST((
                                                      sch.route_id
                                                    ) AS TEXT) = CAST((
                                                      rd.route_id
                                                    ) AS TEXT)
                                                  )
                                                  AND (
                                                    UPPER(CAST((
                                                      sch.distributor_id
                                                    ) AS TEXT)) = UPPER(CAST((
                                                      rd.distributor_id
                                                    ) AS TEXT))
                                                  )
                                                )
                                                AND (
                                                  UPPER(CAST((
                                                    sch.sales_rep_id
                                                  ) AS TEXT)) = UPPER(CAST((
                                                    rd.sales_rep_id
                                                  ) AS TEXT))
                                                )
                                              )
                                              AND (
                                                to_date(CAST((
                                                  sch.schedule_date
                                                ) AS TIMESTAMPNTZ)) >= to_date(CAST((
                                                  rd.effective_start_date
                                                ) AS TIMESTAMPNTZ))
                                              )
                                            )
                                            AND (
                                              to_date(CAST((
                                                sch.schedule_date
                                              ) AS TIMESTAMPNTZ)) <= to_date(CAST((
                                                rd.effective_end_date
                                              ) AS TIMESTAMPNTZ))
                                            )
                                          )
                                          AND (
                                            UPPER(rd.valid_flag) = CAST((
                                              CAST('Y' AS VARCHAR)
                                            ) AS TEXT)
                                          )
                                        )
                                      )
                                )
                                LEFT JOIN edw_vw_os_time_dim AS cal
                                  ON (
                                    (
                                      to_date(CAST((
                                        sch.schedule_date
                                      ) AS TIMESTAMPNTZ)) = to_date(CAST((
                                        cal.cal_date
                                      ) AS TIMESTAMPNTZ))
                                    )
                                  )
                              )
                              JOIN itg_th_dstrbtr_customer_dim_snapshot AS csnap
                                ON (
                                  (
                                    (
                                      SUBSTRING(CAST((
                                        CAST((
                                          sch.schedule_date
                                        ) AS VARCHAR)
                                      ) AS TEXT), 1, 7) = SUBSTRING(CAST((
                                        CAST((
                                          csnap.snap_shot_dt
                                        ) AS VARCHAR)
                                      ) AS TEXT), 1, 7)
                                    )
                                    AND (
                                      (
                                        CAST((
                                          sch.distributor_id
                                        ) AS TEXT) || CAST((
                                          rd.store_id
                                        ) AS TEXT)
                                      ) = (
                                        CAST((
                                          csnap.dstrbtr_id
                                        ) AS TEXT) || CAST((
                                          csnap.ar_cd
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
                                    UPPER(CAST((
                                      sch.approved
                                    ) AS TEXT)) = CAST((
                                      CAST('Y' AS VARCHAR)
                                    ) AS TEXT)
                                  )
                                  AND (
                                    csnap.actv_status = 1
                                  )
                                )
                                AND (
                                  sch.schedule_date <= CONVERT_TIMEZONE('Asia/Bangkok', CURRENT_TIMESTAMP::timestamp_ntz(9))
                                )
                              )
                          ) AS pln
                          LEFT JOIN (
                            SELECT DISTINCT
                              v1.cntry_cd,
                              v1.distributor_id,
                              v1.sales_rep_id,
                              v1.store_id,
                              v1.actual_date_visited AS visited_call
                            FROM edw_vw_th_gt_visit AS v1
                            WHERE
                              (
                                (
                                  (
                                    (
                                      CAST((
                                        v1.time_plan
                                      ) AS TEXT) <> CAST((
                                        CAST('88888' AS VARCHAR)
                                      ) AS TEXT)
                                    )
                                    AND (
                                      CAST((
                                        v1.time_plan
                                      ) AS TEXT) <> CAST((
                                        CAST('99999' AS VARCHAR)
                                      ) AS TEXT)
                                    )
                                  )
                                  AND (
                                    NOT v1.actual_date_visited IS NULL
                                  )
                                )
                                AND (
                                  UPPER(v1.valid_flag) = CAST((
                                    CAST('Y' AS VARCHAR)
                                  ) AS TEXT)
                                )
                              )
                          ) AS vst
                            ON (
                              (
                                (
                                  (
                                    (
                                      UPPER(CAST((
                                        pln.distributor_id
                                      ) AS TEXT)) = UPPER(CAST((
                                        vst.distributor_id
                                      ) AS TEXT))
                                    )
                                    AND (
                                      UPPER(CAST((
                                        pln.store_id
                                      ) AS TEXT)) = UPPER(CAST((
                                        vst.store_id
                                      ) AS TEXT))
                                    )
                                  )
                                  AND (
                                    UPPER(CAST((
                                      pln.sales_rep_id
                                    ) AS TEXT)) = UPPER(CAST((
                                      vst.sales_rep_id
                                    ) AS TEXT))
                                  )
                                )
                                AND (
                                  to_date(CAST((
                                    pln.planned_call
                                  ) AS TIMESTAMPNTZ)) = to_date(CAST((
                                    vst.visited_call
                                  ) AS TIMESTAMPNTZ))
                                )
                              )
                            )
                        )
                        LEFT JOIN (
                          SELECT DISTINCT
                            v1.cntry_cd,
                            v1.distributor_id,
                            v1.sales_rep_id,
                            v1.store_id,
                            v1.actual_date_visited AS effective_call
                          FROM edw_vw_th_gt_visit AS v1
                          WHERE
                            (
                              (
                                (
                                  (
                                    EXISTS(
                                      SELECT
                                        1
                                      FROM edw_vw_th_gt_sales_order AS so
                                      WHERE
                                        (
                                          (
                                            (
                                              (
                                                (
                                                  (
                                                    UPPER(CAST((
                                                      v1.distributor_id
                                                    ) AS TEXT)) = UPPER(TRIM(CAST((
                                                      so.distributor_id
                                                    ) AS TEXT)))
                                                  )
                                                  AND (
                                                    UPPER(CAST((
                                                      v1.store_id
                                                    ) AS TEXT)) = UPPER(TRIM(CAST((
                                                      so.store_id
                                                    ) AS TEXT)))
                                                  )
                                                )
                                                AND (
                                                  UPPER(RIGHT(CAST((
                                                    v1.sales_rep_id
                                                  ) AS TEXT), 4)) = UPPER(TRIM(CAST((
                                                    so.sales_rep_id
                                                  ) AS TEXT)))
                                                )
                                              )
                                              AND (
                                                to_date(CAST((
                                                  v1.actual_date_visited
                                                ) AS TIMESTAMPNTZ)) = to_date(CAST((
                                                  so.sales_order_date
                                                ) AS TIMESTAMPNTZ))
                                              )
                                            )
                                            AND (
                                              (
                                                NOT TRIM(CAST((
                                                  so.invoice_no
                                                ) AS TEXT)) IS NULL
                                              )
                                              OR (
                                                TRIM(CAST((
                                                  so.invoice_no
                                                ) AS TEXT)) <> CAST((
                                                  CAST('' AS VARCHAR)
                                                ) AS TEXT)
                                              )
                                            )
                                          )
                                          AND (
                                            UPPER(so.valid_flag) = CAST((
                                              CAST('Y' AS VARCHAR)
                                            ) AS TEXT)
                                          )
                                        )
                                    )
                                  )
                                  AND (
                                    CAST((
                                      v1.time_plan
                                    ) AS TEXT) <> CAST((
                                      CAST('99999' AS VARCHAR)
                                    ) AS TEXT)
                                  )
                                )
                                AND (
                                  NOT v1.actual_date_visited IS NULL
                                )
                              )
                              AND (
                                UPPER(v1.valid_flag) = CAST((
                                  CAST('Y' AS VARCHAR)
                                ) AS TEXT)
                              )
                            )
                        ) AS eff
                          ON (
                            (
                              (
                                (
                                  (
                                    (
                                      UPPER(CAST((
                                        pln.distributor_id
                                      ) AS TEXT)) = UPPER(CAST((
                                        eff.distributor_id
                                      ) AS TEXT))
                                    )
                                    AND (
                                      UPPER(CAST((
                                        pln.store_id
                                      ) AS TEXT)) = UPPER(CAST((
                                        eff.store_id
                                      ) AS TEXT))
                                    )
                                  )
                                  AND (
                                    UPPER(CAST((
                                      pln.sales_rep_id
                                    ) AS TEXT)) = UPPER(CAST((
                                      eff.sales_rep_id
                                    ) AS TEXT))
                                  )
                                )
                                AND (
                                  DATEDIFF(
                                    DAY,
                                    CAST((
                                      to_date(CAST((
                                        pln.planned_call
                                      ) AS TIMESTAMPNTZ))
                                    ) AS TIMESTAMPNTZ),
                                    CAST((
                                      to_date(CAST((
                                        eff.effective_call
                                      ) AS TIMESTAMPNTZ))
                                    ) AS TIMESTAMPNTZ)
                                  ) >= 0
                                )
                              )
                              AND (
                                DATEDIFF(
                                  DAY,
                                  CAST((
                                    to_date(CAST((
                                      pln.planned_call
                                    ) AS TIMESTAMPNTZ))
                                  ) AS TIMESTAMPNTZ),
                                  CAST((
                                    to_date(CAST((
                                      eff.effective_call
                                    ) AS TIMESTAMPNTZ))
                                  ) AS TIMESTAMPNTZ)
                                ) <= 7
                              )
                            )
                          )
                      )
                      GROUP BY
                        pln.cntry_cd,
                        pln.distributor_id,
                        pln.sales_rep_id,
                        pln.store_id,
                        pln.planned_call
                    ) AS "call"
                    LEFT JOIN edw_vw_os_time_dim AS cal
                      ON (
                        (
                          to_date(CAST((
                            "call".planned_call
                          ) AS TIMESTAMPNTZ)) = to_date(CAST((
                            cal.cal_date
                          ) AS TIMESTAMPNTZ))
                        )
                      )
                  )
                  LEFT JOIN (
                    SELECT DISTINCT
                      itg_th_dstrbtr_customer_dim.dstrbtr_id,
                      itg_th_dstrbtr_customer_dim.ar_cd,
                      itg_th_dstrbtr_customer_dim.sls_grp,
                      itg_th_dstrbtr_customer_dim.re_nm,
                      itg_th_dstrbtr_customer_dim.salesareaname
                    FROM itg_th_dstrbtr_customer_dim
                  ) AS cust
                    ON (
                      (
                        (
                          UPPER(CAST((
                            "call".distributor_id
                          ) AS TEXT)) = UPPER(CAST((
                            cust.dstrbtr_id
                          ) AS TEXT))
                        )
                        AND (
                          UPPER(CAST((
                            "call".store_id
                          ) AS TEXT)) = UPPER(CAST((
                            cust.ar_cd
                          ) AS TEXT))
                        )
                      )
                    )
                )
                WHERE
                  (
                    (
                      UPPER(CAST((
                        "call".distributor_id
                      ) AS TEXT)) IN (
                        SELECT DISTINCT
                          UPPER(CAST((
                            itg_th_gt_dstrbtr_control.distributor_id
                          ) AS TEXT)) AS distributor_id
                        FROM itg_th_gt_dstrbtr_control
                        WHERE
                          (
                            UPPER(CAST((
                              itg_th_gt_dstrbtr_control.call_flag
                            ) AS TEXT)) = CAST((
                              CAST('Y' AS VARCHAR)
                            ) AS TEXT)
                          )
                      )
                    )
                    AND (
                      UPPER(CAST((
                        COALESCE(cust.re_nm, CAST('N/A' AS VARCHAR))
                      ) AS TEXT)) = CAST((
                        CAST('SUPERMARKET' AS VARCHAR)
                      ) AS TEXT)
                    )
                  )
     ), 

cte5 as (
               SELECT
                CAST('ACTUAL' AS VARCHAR) AS identifier,
                "call".cntry_cd,
                CAST('THB' AS VARCHAR) AS crncy_cd,
                CAST('Thailand' AS VARCHAR) AS cntry_nm,
                CAST((
                  "call".mnth_id
                ) AS VARCHAR) AS year_month,
                CAST((
                  "call"."year"
                ) AS INT) AS "year",
                CAST((
                  "call"."month"
                ) AS INT) AS "month",
                "call".distributor_id,
                COALESCE(cust.re_nm, CAST('N/A' AS VARCHAR)) AS store_type,
                CAST((
                         CONCAT(
                                  CAST((COALESCE(cust.sls_grp, CAST('N/A' AS VARCHAR))) AS TEXT) 
                                  , CAST((CAST('-' AS VARCHAR)) AS TEXT) 
                                  , CAST((COALESCE(cust.salesareaname, CAST('N/A' AS VARCHAR))) AS TEXT)
                              )) AS VARCHAR) AS sales_area,
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
                ) AS BIGINT) AS in_full_count,
                CAST((
                  0
                ) AS BIGINT) AS otif_count,
                CAST((
                  0
                ) AS BIGINT) AS coverage_stores_count,
                CAST('0' AS VARCHAR) AS reactivate_stores_count,
                CAST('0' AS VARCHAR) AS inactive_stores_count,
                CAST('0' AS VARCHAR) AS active_stores_count,
                "call".route_com_all,
                "call".effective_call_all,
                "call".planned_store,
                "call".effective_store_in_plan,
                "call".effective_store_all,
                CAST((
                  0
                ) AS BIGINT) AS total_skus,
                CAST((
                  0
                ) AS BIGINT) AS total_stores
              FROM (
                (
                  SELECT
                    pln.cntry_cd,
                    pln.distributor_id,
                    pln.sales_rep_id,
                    pln.store_id,
                    "REPLACE"(
                      SUBSTRING(CAST((
                        CAST((
                          pln.planned_call
                        ) AS VARCHAR)
                      ) AS TEXT), 1, 7),
                      CAST((
                        CAST('-' AS VARCHAR)
                      ) AS TEXT),
                      CAST((
                        CAST('' AS VARCHAR)
                      ) AS TEXT)
                    ) AS mnth_id,
                    SUBSTRING(CAST((
                      CAST((
                        pln.planned_call
                      ) AS VARCHAR)
                    ) AS TEXT), 1, 4) AS "year",
                      CAST((
                        SUBSTRING(CAST((
                          CAST((
                            pln.planned_call
                          ) AS VARCHAR)
                        ) AS TEXT), 6, 2)
                      ) AS DECIMAL(18, 0))
                     AS "month",
                    COUNT(DISTINCT vst.visited_call) AS route_com_all,
                    COUNT(DISTINCT eff.effective_call) AS effective_call_all,
                    MAX(
                      CASE
                        WHEN (
                          (
                            vst.visited_call = eff.effective_call
                          )
                          AND (
                            pln.planned_call = eff.effective_call
                          )
                        )
                        THEN 1
                        ELSE 0
                      END
                    ) AS effective_call_in_plan,
                    1 AS planned_store,
                    CASE WHEN (
                      COUNT(DISTINCT eff.effective_call) <> 0
                    ) THEN 1 ELSE 0 END AS effective_store_all,
                    CASE
                      WHEN (
                        MAX(
                          CASE
                            WHEN (
                              (
                                vst.visited_call = eff.effective_call
                              )
                              AND (
                                pln.planned_call = eff.effective_call
                              )
                            )
                            THEN 1
                            ELSE 0
                          END
                        ) <> 0
                      )
                      THEN 1
                      ELSE 0
                    END AS effective_store_in_plan
                  FROM (
                    (
                      (
                        SELECT
                          sch.cntry_cd,
                          sch.distributor_id,
                          sch.sales_rep_id,
                          rd.store_id,
                          sch.schedule_date AS planned_call
                        FROM (
                          (
                            (
                              edw_vw_th_gt_schedule AS sch
                                JOIN (
                                  SELECT
                                    edw_vw_th_gt_route.cntry_cd,
                                    edw_vw_th_gt_route.crncy_cd,
                                    edw_vw_th_gt_route.route_id,
                                    edw_vw_th_gt_route.store_id,
                                    edw_vw_th_gt_route.routeno,
                                    edw_vw_th_gt_route.distributor_id,
                                    edw_vw_th_gt_route.ship_to,
                                    edw_vw_th_gt_route.contact_person,
                                    edw_vw_th_gt_route.route_description,
                                    edw_vw_th_gt_route.isactive,
                                    edw_vw_th_gt_route.sales_rep_id,
                                    edw_vw_th_gt_route.routecode,
                                    edw_vw_th_gt_route.route_code_desc,
                                    edw_vw_th_gt_route.valid_flag,
                                    edw_vw_th_gt_route.effective_start_date,
                                    MAX(edw_vw_th_gt_route.effective_end_date) AS effective_end_date
                                  FROM edw_vw_th_gt_route
                                  GROUP BY
                                    edw_vw_th_gt_route.cntry_cd,
                                    edw_vw_th_gt_route.crncy_cd,
                                    edw_vw_th_gt_route.route_id,
                                    edw_vw_th_gt_route.store_id,
                                    edw_vw_th_gt_route.routeno,
                                    edw_vw_th_gt_route.distributor_id,
                                    edw_vw_th_gt_route.ship_to,
                                    edw_vw_th_gt_route.contact_person,
                                    edw_vw_th_gt_route.route_description,
                                    edw_vw_th_gt_route.isactive,
                                    edw_vw_th_gt_route.sales_rep_id,
                                    edw_vw_th_gt_route.routecode,
                                    edw_vw_th_gt_route.route_code_desc,
                                    edw_vw_th_gt_route.valid_flag,
                                    edw_vw_th_gt_route.effective_start_date
                                ) AS rd
                                  ON (
                                    (
                                      (
                                        (
                                          (
                                            (
                                              (
                                                CAST((
                                                  sch.route_id
                                                ) AS TEXT) = CAST((
                                                  rd.route_id
                                                ) AS TEXT)
                                              )
                                              AND (
                                                UPPER(CAST((
                                                  sch.distributor_id
                                                ) AS TEXT)) = UPPER(CAST((
                                                  rd.distributor_id
                                                ) AS TEXT))
                                              )
                                            )
                                            AND (
                                              UPPER(CAST((
                                                sch.sales_rep_id
                                              ) AS TEXT)) = UPPER(CAST((
                                                rd.sales_rep_id
                                              ) AS TEXT))
                                            )
                                          )
                                          AND (
                                            TO_DATE(CAST((
                                              sch.schedule_date
                                            ) AS TIMESTAMPNTZ)) >= TO_DATE(CAST((
                                              rd.effective_start_date
                                            ) AS TIMESTAMPNTZ))
                                          )
                                        )
                                        AND (
                                          TO_DATE(CAST((
                                            sch.schedule_date
                                          ) AS TIMESTAMPNTZ)) <= TO_DATE(CAST((
                                            rd.effective_end_date
                                          ) AS TIMESTAMPNTZ))
                                        )
                                      )
                                      AND (
                                        UPPER(rd.valid_flag) = CAST((
                                          CAST('Y' AS VARCHAR)
                                        ) AS TEXT)
                                      )
                                    )
                                  )
                            )
                            LEFT JOIN edw_vw_os_time_dim AS cal
                              ON (
                                (
                                  TO_DATE(CAST((
                                    sch.schedule_date
                                  ) AS TIMESTAMPNTZ)) = TO_DATE(CAST((
                                    cal.cal_date
                                  ) AS TIMESTAMPNTZ))
                                )
                              )
                          )
                          JOIN itg_th_dstrbtr_customer_dim_snapshot AS csnap
                            ON (
                              (
                                (
                                  SUBSTRING(CAST((
                                    CAST((
                                      sch.schedule_date
                                    ) AS VARCHAR)
                                  ) AS TEXT), 1, 7) = SUBSTRING(CAST((
                                    CAST((
                                      csnap.snap_shot_dt
                                    ) AS VARCHAR)
                                  ) AS TEXT), 1, 7)
                                )
                                AND (
                                  (
                                    CAST((
                                      sch.distributor_id
                                    ) AS TEXT) || CAST((
                                      rd.store_id
                                    ) AS TEXT)
                                  ) = (
                                    CAST((
                                      csnap.dstrbtr_id
                                    ) AS TEXT) || CAST((
                                      csnap.ar_cd
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
                                UPPER(CAST((
                                  sch.approved
                                ) AS TEXT)) = CAST((
                                  CAST('Y' AS VARCHAR)
                                ) AS TEXT)
                              )
                              AND (
                                csnap.actv_status = 1
                              )
                            )
                            AND (
                              sch.schedule_date <= CONVERT_TIMEZONE('Asia/Bangkok', CURRENT_TIMESTAMP::timestamp_ntz(9))
                            )
                          )
                      ) AS pln
                      LEFT JOIN (
                        SELECT DISTINCT
                          v1.cntry_cd,
                          v1.distributor_id,
                          v1.sales_rep_id,
                          v1.store_id,
                          v1.actual_date_visited AS visited_call
                        FROM edw_vw_th_gt_visit AS v1
                        WHERE
                          (
                            (
                              (
                                CAST((
                                  v1.time_plan
                                ) AS TEXT) <> CAST((
                                  CAST('99999' AS VARCHAR)
                                ) AS TEXT)
                              )
                              AND (
                                NOT v1.actual_date_visited IS NULL
                              )
                            )
                            AND (
                              UPPER(v1.valid_flag) = CAST((
                                CAST('Y' AS VARCHAR)
                              ) AS TEXT)
                            )
                          )
                      ) AS vst
                        ON (
                          (
                            (
                              (
                                (
                                  UPPER(CAST((
                                    pln.distributor_id
                                  ) AS TEXT)) = UPPER(CAST((
                                    vst.distributor_id
                                  ) AS TEXT))
                                )
                                AND (
                                  UPPER(CAST((
                                    pln.store_id
                                  ) AS TEXT)) = UPPER(CAST((
                                    vst.store_id
                                  ) AS TEXT))
                                )
                              )
                              AND (
                                UPPER(CAST((
                                  pln.sales_rep_id
                                ) AS TEXT)) = UPPER(CAST((
                                  vst.sales_rep_id
                                ) AS TEXT))
                              )
                            )
                            AND (
                              SUBSTRING(CAST((
                                CAST((
                                  pln.planned_call
                                ) AS VARCHAR)
                              ) AS TEXT), 1, 7) = SUBSTRING(CAST((
                                CAST((
                                  vst.visited_call
                                ) AS VARCHAR)
                              ) AS TEXT), 1, 7)
                            )
                          )
                        )
                    )
                    LEFT JOIN (
                      SELECT DISTINCT
                        v1.cntry_cd,
                        v1.distributor_id,
                        v1.sales_rep_id,
                        v1.store_id,
                        v1.actual_date_visited AS effective_call
                      FROM edw_vw_th_gt_visit AS v1
                      WHERE
                        (
                          (
                            (
                              (
                                EXISTS(
                                  SELECT
                                    1
                                  FROM edw_vw_th_gt_sales_order AS so
                                  WHERE
                                    (
                                      (
                                        (
                                          (
                                            (
                                              (
                                                UPPER(CAST((
                                                  v1.distributor_id
                                                ) AS TEXT)) = UPPER(TRIM(CAST((
                                                  so.distributor_id
                                                ) AS TEXT)))
                                              )
                                              AND (
                                                UPPER(CAST((
                                                  v1.store_id
                                                ) AS TEXT)) = UPPER(TRIM(CAST((
                                                  so.store_id
                                                ) AS TEXT)))
                                              )
                                            )
                                            AND (
                                              UPPER(RIGHT(CAST((
                                                v1.sales_rep_id
                                              ) AS TEXT), 4)) = UPPER(TRIM(CAST((
                                                so.sales_rep_id
                                              ) AS TEXT)))
                                            )
                                          )
                                          AND (
                                            TO_DATE(CAST((
                                              v1.actual_date_visited
                                            ) AS TIMESTAMPNTZ)) = TO_DATE(CAST((
                                              so.sales_order_date
                                            ) AS TIMESTAMPNTZ))
                                          )
                                        )
                                        AND (
                                          (
                                            NOT TRIM(CAST((
                                              so.invoice_no
                                            ) AS TEXT)) IS NULL
                                          )
                                          OR (
                                            TRIM(CAST((
                                              so.invoice_no
                                            ) AS TEXT)) <> CAST((
                                              CAST('' AS VARCHAR)
                                            ) AS TEXT)
                                          )
                                        )
                                      )
                                      AND (
                                        UPPER(so.valid_flag) = CAST((
                                          CAST('Y' AS VARCHAR)
                                        ) AS TEXT)
                                      )
                                    )
                                )
                              )
                              AND (
                                CAST((
                                  v1.time_plan
                                ) AS TEXT) <> CAST((
                                  CAST('99999' AS VARCHAR)
                                ) AS TEXT)
                              )
                            )
                            AND (
                              NOT v1.actual_date_visited IS NULL
                            )
                          )
                          AND (
                            UPPER(v1.valid_flag) = CAST((
                              CAST('Y' AS VARCHAR)
                            ) AS TEXT)
                          )
                        )
                    ) AS eff
                      ON (
                        (
                          (
                            (
                              (
                                UPPER(CAST((
                                  pln.distributor_id
                                ) AS TEXT)) = UPPER(CAST((
                                  eff.distributor_id
                                ) AS TEXT))
                              )
                              AND (
                                UPPER(CAST((
                                  pln.store_id
                                ) AS TEXT)) = UPPER(CAST((
                                  eff.store_id
                                ) AS TEXT))
                              )
                            )
                            AND (
                              UPPER(CAST((
                                pln.sales_rep_id
                              ) AS TEXT)) = UPPER(CAST((
                                eff.sales_rep_id
                              ) AS TEXT))
                            )
                          )
                          AND (
                            SUBSTRING(CAST((
                              CAST((
                                pln.planned_call
                              ) AS VARCHAR)
                            ) AS TEXT), 1, 7) = SUBSTRING(CAST((
                              CAST((
                                eff.effective_call
                              ) AS VARCHAR)
                            ) AS TEXT), 1, 7)
                          )
                        )
                      )
                  )
                  GROUP BY
                    "REPLACE"(
                      SUBSTRING(CAST((
                        CAST((
                          pln.planned_call
                        ) AS VARCHAR)
                      ) AS TEXT), 1, 7),
                      CAST((
                        CAST('-' AS VARCHAR)
                      ) AS TEXT),
                      CAST((
                        CAST('' AS VARCHAR)
                      ) AS TEXT)
                    ),
                    SUBSTRING(CAST((
                      CAST((
                        pln.planned_call
                      ) AS VARCHAR)
                    ) AS TEXT), 1, 4),
                    
                      CAST((
                        SUBSTRING(CAST((
                          CAST((
                            pln.planned_call
                          ) AS VARCHAR)
                        ) AS TEXT), 6, 2)
                      ) AS DECIMAL(18, 0))
                    ,
                    pln.cntry_cd,
                    pln.distributor_id,
                    pln.sales_rep_id,
                    pln.store_id
                ) AS "call"
                LEFT JOIN (
                  SELECT DISTINCT
                    itg_th_dstrbtr_customer_dim.dstrbtr_id,
                    itg_th_dstrbtr_customer_dim.ar_cd,
                    itg_th_dstrbtr_customer_dim.sls_grp,
                    itg_th_dstrbtr_customer_dim.re_nm,
                    itg_th_dstrbtr_customer_dim.salesareaname
                  FROM itg_th_dstrbtr_customer_dim
                ) AS cust
                  ON (
                    (
                      (
                        UPPER(CAST((
                          "call".distributor_id
                        ) AS TEXT)) = UPPER(CAST((
                          cust.dstrbtr_id
                        ) AS TEXT))
                      )
                      AND (
                        UPPER(CAST((
                          "call".store_id
                        ) AS TEXT)) = UPPER(CAST((
                          cust.ar_cd
                        ) AS TEXT))
                      )
                    )
                  )
              )
              WHERE
                (
                  UPPER(CAST((
                    "call".distributor_id
                  ) AS TEXT)) IN (
                    SELECT DISTINCT
                      UPPER(CAST((
                        itg_th_gt_dstrbtr_control.distributor_id
                      ) AS TEXT)) AS distributor_id
                    FROM itg_th_gt_dstrbtr_control
                    WHERE
                      (
                        UPPER(CAST((
                          itg_th_gt_dstrbtr_control.call_flag
                        ) AS TEXT)) = CAST((
                          CAST('Y' AS VARCHAR)
                        ) AS TEXT)
                      )
                  )
                )
      UNION ALL

SELECT
              CAST('ACTUAL' AS VARCHAR) AS identifier,
              otif.cntry_cd,
              CAST('THB' AS VARCHAR) AS crncy_cd,
              CAST('Thailand' AS VARCHAR) AS cntry_nm,
              CAST((
                cal.cal_mnth_id
              ) AS VARCHAR) AS year_month,
              cal.cal_year AS "year",
              cal.cal_mnth_no AS "month",
              otif.distributor_id,
              COALESCE(cust.re_nm, CAST('N/A' AS VARCHAR)) AS store_type,
              CAST((
                        CONCAT(
                                  CAST((COALESCE(cust.sls_grp, CAST('N/A' AS VARCHAR))) AS TEXT) 
                                  , CAST((CAST('-' AS VARCHAR)) AS TEXT) 
                                  , CAST((COALESCE(cust.salesareaname, CAST('N/A' AS VARCHAR))) AS TEXT)
                              )) AS VARCHAR) AS sales_area,
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
              otif.sales_order_count,
              otif.on_time_count,
              otif.in_full_count,
              otif.otif_count,
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
                (
                  SELECT
                    odc.cntry_cd,
                    odc.distributor_id,
                    odc.product_code,
                    COUNT(odc.sales_order_no) AS sales_order_count,
                    odc.invoice_no,
                    odc.order_dt,
                    odc.store_id,
                    SUM(
                      CASE
                        WHEN (
                          CAST((
                            odc.on_time
                          ) AS TEXT) = CAST((
                            CAST('Y' AS VARCHAR)
                          ) AS TEXT)
                        )
                        THEN 1
                        ELSE 0
                      END
                    ) AS on_time_count,
                    SUM(
                      CASE
                        WHEN (
                          CAST((
                            odc.in_full
                          ) AS TEXT) = CAST((
                            CAST('Y' AS VARCHAR)
                          ) AS TEXT)
                        )
                        THEN 1
                        ELSE 0
                      END
                    ) AS in_full_count,
                    SUM(
                      CASE
                        WHEN (
                          CAST((
                            odc.otif
                          ) AS TEXT) = CAST((
                            CAST('Y' AS VARCHAR)
                          ) AS TEXT)
                        )
                        THEN 1
                        ELSE 0
                      END
                    ) AS otif_count
                  FROM (
                    SELECT
                      odr.cntry_cd,
                      odr.distributor_id,
                      odr.product_code,
                      odr.sales_order_no,
                      odr.invoice_no,
                      TO_DATE(CAST((
                        odr.sales_order_date
                      ) AS TIMESTAMPNTZ)) AS order_dt,
                      odr.store_id,
                      on_time.on_time,
                      in_full.in_full,
                      CASE
                        WHEN (
                          (
                            CAST((
                              on_time.on_time
                            ) AS TEXT) = CAST((
                              CAST('Y' AS VARCHAR)
                            ) AS TEXT)
                          )
                          AND (
                            CAST((
                              in_full.in_full
                            ) AS TEXT) = CAST((
                              CAST('Y' AS VARCHAR)
                            ) AS TEXT)
                          )
                        )
                        THEN CAST('Y' AS VARCHAR)
                        WHEN (
                          (
                            on_time.on_time IS NULL
                          ) AND (
                            in_full.in_full IS NULL
                          )
                        )
                        THEN CAST('NA' AS VARCHAR)
                        ELSE CAST('N' AS VARCHAR)
                      END AS otif
                    FROM (
                      (
                        edw_vw_th_gt_sales_order AS odr
                          LEFT JOIN (
                            SELECT
                              CAST('ON_TIME' AS VARCHAR) AS kpi,
                              so.cntry_cd,
                              so.distributor_id,
                              so.product_code,
                              so.sales_order_no,
                              so.invoice_no,
                              TO_DATE(CAST((
                                so.sales_order_date
                              ) AS TIMESTAMPNTZ)) AS order_dt,
                              so.store_id,
                              CASE
                                WHEN (
                                  (
                                    NOT TO_DATE(CAST((
                                      sls.bill_date
                                    ) AS TIMESTAMPNTZ)) IS NULL
                                  )
                                  AND (
                                    DATEDIFF(
                                      DAY,
                                      CAST((
                                        TO_DATE(CAST((
                                          so.sales_order_date
                                        ) AS TIMESTAMPNTZ))
                                      ) AS TIMESTAMPNTZ),
                                      CAST((
                                        TO_DATE(CAST((
                                          sls.bill_date
                                        ) AS TIMESTAMPNTZ))
                                      ) AS TIMESTAMPNTZ)
                                    ) <= 3
                                  )
                                )
                                THEN CAST('Y' AS VARCHAR)
                                ELSE CAST('N' AS VARCHAR)
                              END AS on_time
                            FROM (
                              edw_vw_th_gt_sales_order AS so
                                LEFT JOIN (
                                  SELECT
                                    inv.bill_doc AS invoice_no,
                                    inv.cust_cd,
                                    inv.dstrbtr_grp_cd,
                                    TO_DATE(inv.bill_date) AS bill_date,
                                    sku.parentcode,
                                    SUM(inv.sls_qty) AS sls_qty
                                  FROM (
                                    edw_vw_th_sellout_sales_foc_fact AS inv
                                      LEFT JOIN itg_th_dstrbtr_material_dim AS sku
                                        ON (
                                          (
                                            CAST((
                                              inv.dstrbtr_matl_num
                                            ) AS TEXT) = CAST((
                                              sku.item_cd
                                            ) AS TEXT)
                                          )
                                        )
                                  )
                                  GROUP BY
                                    inv.bill_doc,
                                    TO_DATE(inv.bill_date),
                                    sku.parentcode,
                                    inv.cust_cd,
                                    inv.dstrbtr_grp_cd
                                ) AS sls
                                  ON (
                                    (
                                      (
                                        (
                                          (
                                            UPPER(CAST((
                                              COALESCE(so.invoice_no, CAST('NA' AS VARCHAR))
                                            ) AS TEXT)) = UPPER(CAST((
                                              COALESCE(sls.invoice_no, CAST('NA' AS VARCHAR))
                                            ) AS TEXT))
                                          )
                                          AND (
                                            UPPER(CAST((
                                              so.product_code
                                            ) AS TEXT)) = UPPER(CAST((
                                              sls.parentcode
                                            ) AS TEXT))
                                          )
                                        )
                                        AND (
                                          UPPER(CAST((
                                            so.distributor_id
                                          ) AS TEXT)) = UPPER(CAST((
                                            sls.dstrbtr_grp_cd
                                          ) AS TEXT))
                                        )
                                      )
                                      AND (
                                        UPPER(CAST((
                                          so.store_id
                                        ) AS TEXT)) = UPPER(CAST((
                                          sls.cust_cd
                                        ) AS TEXT))
                                      )
                                    )
                                  )
                            )
                            WHERE
                              (
                                (
                                  CASE
                                    WHEN (
                                      (
                                        NOT TO_DATE(CAST((
                                          sls.bill_date
                                        ) AS TIMESTAMPNTZ)) IS NULL
                                      )
                                      AND (
                                        TO_DATE(CAST((
                                          sls.bill_date
                                        ) AS TIMESTAMPNTZ)) >= TO_DATE(CAST((
                                          so.sales_order_date
                                        ) AS TIMESTAMPNTZ))
                                      )
                                    )
                                    THEN 1
                                    WHEN (
                                      (
                                        TO_DATE(CAST((
                                          sls.bill_date
                                        ) AS TIMESTAMPNTZ)) IS NULL
                                      )
                                      AND (
                                        DATEDIFF(
                                          DAY,
                                          CAST((
                                            TO_DATE(CAST((
                                              so.sales_order_date
                                            ) AS TIMESTAMPNTZ))
                                          ) AS TIMESTAMPNTZ),
                                          CAST((
                                            TO_DATE(
                                              COALESCE(
                                                CAST((
                                                  sls.bill_date
                                                ) AS TIMESTAMPNTZ),
                                                CONVERT_TIMEZONE('Asia/Bangkok', CURRENT_TIMESTAMP::timestamp_ntz(9))
                                              )
                                            )
                                          ) AS TIMESTAMPNTZ)
                                        ) > 3
                                      )
                                    )
                                    THEN 1
                                    ELSE 0
                                  END = 1
                                )
                                AND (
                                  UPPER(so.valid_flag) = CAST((
                                    CAST('Y' AS VARCHAR)
                                  ) AS TEXT)
                                )
                              )
                          ) AS on_time
                            ON (
                              (
                                (
                                  (
                                    (
                                      (
                                        (
                                          (
                                            (
                                              UPPER(CAST((
                                                odr.cntry_cd
                                              ) AS TEXT)) = UPPER(CAST((
                                                on_time.cntry_cd
                                              ) AS TEXT))
                                            )
                                            AND (
                                              UPPER(CAST((
                                                odr.distributor_id
                                              ) AS TEXT)) = UPPER(CAST((
                                                on_time.distributor_id
                                              ) AS TEXT))
                                            )
                                          )
                                          AND (
                                            UPPER(CAST((
                                              odr.product_code
                                            ) AS TEXT)) = UPPER(CAST((
                                              on_time.product_code
                                            ) AS TEXT))
                                          )
                                        )
                                        AND (
                                          UPPER(CAST((
                                            odr.sales_order_no
                                          ) AS TEXT)) = UPPER(CAST((
                                            on_time.sales_order_no
                                          ) AS TEXT))
                                        )
                                      )
                                      AND (
                                        UPPER(CAST((
                                          COALESCE(odr.invoice_no, CAST('NA' AS VARCHAR))
                                        ) AS TEXT)) = UPPER(CAST((
                                          COALESCE(on_time.invoice_no, CAST('NA' AS VARCHAR))
                                        ) AS TEXT))
                                      )
                                    )
                                    AND (
                                      TO_DATE(CAST((
                                        odr.sales_order_date
                                      ) AS TIMESTAMPNTZ)) = TO_DATE(CAST((
                                        on_time.order_dt
                                      ) AS TIMESTAMPNTZ))
                                    )
                                  )
                                  AND (
                                    UPPER(CAST((
                                      odr.store_id
                                    ) AS TEXT)) = UPPER(CAST((
                                      on_time.store_id
                                    ) AS TEXT))
                                  )
                                )
                                AND (
                                  UPPER(odr.valid_flag) = CAST((
                                    CAST('Y' AS VARCHAR)
                                  ) AS TEXT)
                                )
                              )
                            )
                      )
                      LEFT JOIN (
                        SELECT
                          CAST('IN_FULL' AS VARCHAR) AS kpi,
                          so.cntry_cd,
                          so.distributor_id,
                          so.product_code,
                          so.sales_order_no,
                          so.invoice_no,
                          TO_DATE(CAST((
                            so.sales_order_date
                          ) AS TIMESTAMPNTZ)) AS order_dt,
                          so.store_id,
                          CASE
                            WHEN (
                              (
                                COALESCE(so.qty, CAST((
                                  CAST((
                                    0
                                  ) AS DECIMAL)
                                ) AS DECIMAL(18, 0))) > CAST((
                                  CAST((
                                    0
                                  ) AS DECIMAL)
                                ) AS DECIMAL(18, 0))
                              )
                              AND (
                                ROUND(
                                  COALESCE(sls.sls_qty, CAST((
                                    CAST((
                                      0
                                    ) AS DECIMAL)
                                  ) AS DECIMAL(18, 0))),
                                  2
                                ) >= ROUND(COALESCE(so.qty, CAST((
                                  CAST((
                                    0
                                  ) AS DECIMAL)
                                ) AS DECIMAL(18, 0))), 2)
                              )
                            )
                            THEN CAST('Y' AS VARCHAR)
                            ELSE CAST('N' AS VARCHAR)
                          END AS in_full
                        FROM (
                          edw_vw_th_gt_sales_order AS so
                            LEFT JOIN (
                              SELECT
                                inv.bill_doc AS invoice_no,
                                inv.cust_cd,
                                inv.dstrbtr_grp_cd,
                                TO_DATE(inv.bill_date) AS bill_date,
                                sku.parentcode,
                                SUM(
                                  COALESCE(inv.sls_qty, CAST((
                                    CAST((
                                      0
                                    ) AS DECIMAL)
                                  ) AS DECIMAL(18, 0)))
                                ) AS sls_qty
                              FROM (
                                edw_vw_th_sellout_sales_foc_fact AS inv
                                  LEFT JOIN itg_th_dstrbtr_material_dim AS sku
                                    ON (
                                      (
                                        CAST((
                                          inv.dstrbtr_matl_num
                                        ) AS TEXT) = CAST((
                                          sku.item_cd
                                        ) AS TEXT)
                                      )
                                    )
                              )
                              GROUP BY
                                inv.bill_doc,
                                inv.cust_cd,
                                inv.dstrbtr_grp_cd,
                                TO_DATE(inv.bill_date),
                                sku.parentcode
                            ) AS sls
                              ON (
                                (
                                  (
                                    (
                                      (
                                        UPPER(CAST((
                                          COALESCE(so.invoice_no, CAST('NA' AS VARCHAR))
                                        ) AS TEXT)) = UPPER(CAST((
                                          COALESCE(sls.invoice_no, CAST('NA' AS VARCHAR))
                                        ) AS TEXT))
                                      )
                                      AND (
                                        UPPER(CAST((
                                          so.product_code
                                        ) AS TEXT)) = UPPER(CAST((
                                          sls.parentcode
                                        ) AS TEXT))
                                      )
                                    )
                                    AND (
                                      UPPER(CAST((
                                        so.distributor_id
                                      ) AS TEXT)) = UPPER(CAST((
                                        sls.dstrbtr_grp_cd
                                      ) AS TEXT))
                                    )
                                  )
                                  AND (
                                    UPPER(CAST((
                                      so.store_id
                                    ) AS TEXT)) = UPPER(CAST((
                                      sls.cust_cd
                                    ) AS TEXT))
                                  )
                                )
                              )
                        )
                        WHERE
                          (
                            (
                              CASE
                                WHEN (
                                  (
                                    NOT TO_DATE(CAST((
                                      sls.bill_date
                                    ) AS TIMESTAMPNTZ)) IS NULL
                                  )
                                  AND (
                                    TO_DATE(CAST((
                                      sls.bill_date
                                    ) AS TIMESTAMPNTZ)) >= TO_DATE(CAST((
                                      so.sales_order_date
                                    ) AS TIMESTAMPNTZ))
                                  )
                                )
                                THEN 1
                                WHEN (
                                  (
                                    TO_DATE(CAST((
                                      sls.bill_date
                                    ) AS TIMESTAMPNTZ)) IS NULL
                                  )
                                  AND (
                                    DATEDIFF(
                                      DAY,
                                      CAST((
                                        TO_DATE(CAST((
                                          so.sales_order_date
                                        ) AS TIMESTAMPNTZ))
                                      ) AS TIMESTAMPNTZ),
                                      CAST((
                                        TO_DATE(
                                          COALESCE(
                                            CAST((
                                              sls.bill_date
                                            ) AS TIMESTAMPNTZ),
                                            CONVERT_TIMEZONE('Asia/Bangkok', CURRENT_TIMESTAMP::timestamp_ntz(9))
                                          )
                                        )
                                      ) AS TIMESTAMPNTZ)
                                    ) > 3
                                  )
                                )
                                THEN 1
                                ELSE 0
                              END = 1
                            )
                            AND (
                              UPPER(so.valid_flag) = CAST((
                                CAST('Y' AS VARCHAR)
                              ) AS TEXT)
                            )
                          )
                      ) AS in_full
                        ON (
                          (
                            (
                              (
                                (
                                  (
                                    (
                                      (
                                        (
                                          UPPER(CAST((
                                            odr.cntry_cd
                                          ) AS TEXT)) = UPPER(CAST((
                                            in_full.cntry_cd
                                          ) AS TEXT))
                                        )
                                        AND (
                                          UPPER(CAST((
                                            odr.distributor_id
                                          ) AS TEXT)) = UPPER(CAST((
                                            in_full.distributor_id
                                          ) AS TEXT))
                                        )
                                      )
                                      AND (
                                        UPPER(CAST((
                                          odr.product_code
                                        ) AS TEXT)) = UPPER(CAST((
                                          in_full.product_code
                                        ) AS TEXT))
                                      )
                                    )
                                    AND (
                                      UPPER(CAST((
                                        odr.sales_order_no
                                      ) AS TEXT)) = UPPER(CAST((
                                        in_full.sales_order_no
                                      ) AS TEXT))
                                    )
                                  )
                                  AND (
                                    UPPER(CAST((
                                      COALESCE(odr.invoice_no, CAST('NA' AS VARCHAR))
                                    ) AS TEXT)) = UPPER(CAST((
                                      COALESCE(in_full.invoice_no, CAST('NA' AS VARCHAR))
                                    ) AS TEXT))
                                  )
                                )
                                AND (
                                  TO_DATE(CAST((
                                    odr.sales_order_date
                                  ) AS TIMESTAMPNTZ)) = TO_DATE(CAST((
                                    in_full.order_dt
                                  ) AS TIMESTAMPNTZ))
                                )
                              )
                              AND (
                                UPPER(CAST((
                                  odr.store_id
                                ) AS TEXT)) = UPPER(CAST((
                                  in_full.store_id
                                ) AS TEXT))
                              )
                            )
                            AND (
                              UPPER(odr.valid_flag) = CAST((
                                CAST('Y' AS VARCHAR)
                              ) AS TEXT)
                            )
                          )
                        )
                    )
                  ) AS odc
                  WHERE
                    (
                      CAST((
                        odc.otif
                      ) AS TEXT) <> CAST((
                        CAST('NA' AS VARCHAR)
                      ) AS TEXT)
                    )
                  GROUP BY
                    odc.cntry_cd,
                    odc.distributor_id,
                    odc.product_code,
                    odc.invoice_no,
                    odc.order_dt,
                    odc.store_id
                ) AS otif
                LEFT JOIN edw_vw_os_time_dim AS cal
                  ON (
                    (
                      TO_DATE(CAST((
                        otif.order_dt
                      ) AS TIMESTAMPNTZ)) = TO_DATE(CAST((
                        cal.cal_date
                      ) AS TIMESTAMPNTZ))
                    )
                  )
              )
              LEFT JOIN (
                SELECT DISTINCT
                  itg_th_dstrbtr_customer_dim.dstrbtr_id,
                  itg_th_dstrbtr_customer_dim.ar_cd,
                  itg_th_dstrbtr_customer_dim.sls_grp,
                  itg_th_dstrbtr_customer_dim.re_nm,
                  itg_th_dstrbtr_customer_dim.salesareaname
                FROM itg_th_dstrbtr_customer_dim
              ) AS cust
                ON (
                  (
                    (
                      UPPER(CAST((
                        otif.distributor_id
                      ) AS TEXT)) = UPPER(CAST((
                        cust.dstrbtr_id
                      ) AS TEXT))
                    )
                    AND (
                      UPPER(CAST((
                        otif.store_id
                      ) AS TEXT)) = UPPER(CAST((
                        cust.ar_cd
                      ) AS TEXT))
                    )
                  )
                )
            )
            WHERE
              (
                UPPER(CAST((
                  otif.distributor_id
                ) AS TEXT)) IN (
                  SELECT DISTINCT
                    UPPER(CAST((
                      itg_th_gt_dstrbtr_control.distributor_id
                    ) AS TEXT)) AS distributor_id
                  FROM itg_th_gt_dstrbtr_control
                  WHERE
                    (
                      UPPER(CAST((
                        itg_th_gt_dstrbtr_control.otif_flag
                      ) AS TEXT)) = CAST((
                        CAST('Y' AS VARCHAR)
                      ) AS TEXT)
                    )
                )
              )
),

cte6 as (
      SELECT
            CAST('ACTUAL' AS VARCHAR) AS identifier,
            CAST('TH' AS VARCHAR) AS cntry_cd,
            CAST('THB' AS VARCHAR) AS crncy_cd,
            CAST('Thailand' AS VARCHAR) AS cntry_nm,
            CAST((
              cal.cal_mnth_id
            ) AS VARCHAR) AS year_month,
            cal.cal_year AS "year",
            cal.cal_mnth_no AS "month",
            cust.dstrbtr_id AS distributor_id,
            COALESCE(cust.re_nm, CAST('N/A' AS VARCHAR)) AS store_type,
            CAST((
                              CONCAT(
                                  CAST((COALESCE(cust.sls_grp, CAST('N/A' AS VARCHAR))) AS TEXT) 
                                  , CAST((CAST('-' AS VARCHAR)) AS TEXT) 
                                  , CAST((COALESCE(cust.salesareaname, CAST('N/A' AS VARCHAR))) AS TEXT)
                              )) AS VARCHAR) AS sales_area,
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
            ) AS BIGINT) AS in_full_count,
            CAST((
              0
            ) AS BIGINT) AS otif_count,
            COUNT(*) AS coverage_stores_count,
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
            itg_th_dstrbtr_customer_dim_snapshot AS cust
              LEFT JOIN edw_vw_os_time_dim AS cal
                ON (
                  (
                    to_date(cust.snap_shot_dt) = to_date(CAST((
                      cal.cal_date
                    ) AS TIMESTAMPNTZ))
                  )
                )
          )
          WHERE
            (
              (
                TRIM(CAST((
                  CAST((
                    cust.actv_status
                  ) AS VARCHAR)
                ) AS TEXT)) = CAST((
                  CAST('1' AS VARCHAR)
                ) AS TEXT)
              )
              AND (
                UPPER(CAST((
                  cust.dstrbtr_id
                ) AS TEXT)) IN (
                  SELECT DISTINCT
                    UPPER(CAST((
                      itg_th_gt_dstrbtr_control.distributor_id
                    ) AS TEXT)) AS distributor_id
                  FROM itg_th_gt_dstrbtr_control
                  WHERE
                    (
                      UPPER(CAST((
                        itg_th_gt_dstrbtr_control.stores_flag
                      ) AS TEXT)) = CAST((
                        CAST('Y' AS VARCHAR)
                      ) AS TEXT)
                    )
                )
              )
            )
          GROUP BY
            CAST((
              cal.cal_mnth_id
            ) AS VARCHAR),
            cal.cal_year,
            cal.cal_mnth_no,
            cust.dstrbtr_id,
            cust.re_nm,
            cust.sls_grp,
            cust.salesareaname
      
      UNION ALL

      SELECT
          CAST('ACTUAL' AS VARCHAR) AS identifier,
          CAST('TH' AS VARCHAR) AS cntry_cd,
          CAST('THB' AS VARCHAR) AS crncy_cd,
          CAST('Thailand' AS VARCHAR) AS cntry_nm,
          CAST((
            stores.year_month
          ) AS VARCHAR) AS year_month,
          time_dim.cal_year AS "year",
          time_dim.cal_mnth_no AS "month",
          stores.distributor_id,
          COALESCE(cust_dim.re_nm, CAST('N/A' AS VARCHAR)) AS store_type,
          CAST((
            concat (
            concat(
                CAST((
                  COALESCE(cust_dim.sls_grp, CAST('N/A' AS VARCHAR))
                ) AS TEXT) , CAST((
                  CAST('-' AS VARCHAR)
                ) AS TEXT)
              ) , CAST((
                COALESCE(cust_dim.salesareaname, CAST('N/A' AS VARCHAR))
              ) AS TEXT)
            )
          ) AS VARCHAR) AS sales_area,
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
          CAST((
            CAST((
              0
            ) AS DECIMAL)
          ) AS DECIMAL(18, 0)) AS sellout_target,
          CAST((0) AS BIGINT) AS msld_actuals_count,
          CAST((0) AS BIGINT) AS msld_target_count,
          CAST((0) AS BIGINT) AS planned_call_count,
          CAST((0) AS BIGINT) AS visited_call_count,
          CAST((0) AS BIGINT) AS effective_call_count,
          CAST((0) AS BIGINT) AS sales_order_count,
          CAST((0) AS BIGINT) AS on_time_count,
          CAST((0) AS BIGINT) AS in_full_count,
          CAST((0) AS BIGINT) AS otif_count,
          CAST((0) AS BIGINT) AS coverage_stores_count,
          stores.reactivate_stores AS reactivate_stores_count,
          stores.inactive_store AS inactive_stores_count,
          CAST('0' AS VARCHAR) AS active_stores_count,
          CAST((0) AS BIGINT) AS route_com_all,
          CAST((0) AS BIGINT) AS effective_call_all,
          CAST((0) AS BIGINT) AS planned_store,
          CAST((0) AS BIGINT) AS effective_store_in_plan,
          CAST((0) AS BIGINT) AS effective_store_all,
          CAST((0) AS BIGINT) AS total_skus,
          CAST((0) AS BIGINT) AS total_stores
        FROM (
          (
            (
              SELECT
                cust_flag.year_month,
                cust_flag.distributor_id,
                cust_flag.cust_cd,
                cust_flag.curr_actv_status,
                cust_flag.curr_net_sell_out,
                cust_flag.prev_actv_status,
                cust_flag.prev_net_sell_out,
                CASE
                  WHEN (
                    (
                      (
                        (
                          cust_flag.curr_net_sell_out > CAST((
                            CAST((
                              0
                            ) AS DECIMAL)
                          ) AS DECIMAL(18, 0))
                        )
                        AND (
                          cust_flag.prev_net_sell_out <= CAST((
                            CAST((
                              0
                            ) AS DECIMAL)
                          ) AS DECIMAL(18, 0))
                        )
                      )
                      AND (
                        cust_flag.curr_actv_status = 1
                      )
                    )
                    AND (
                      cust_flag.prev_actv_status = 1
                    )
                  )
                  THEN CAST('1' AS VARCHAR)
                  ELSE CAST('0' AS VARCHAR)
                END AS reactivate_stores,
                CASE
                  WHEN (
                    (
                      cust_flag.prev_net_sell_out <= CAST((
                        CAST((
                          0
                        ) AS DECIMAL)
                      ) AS DECIMAL(18, 0))
                    )
                    AND (
                      cust_flag.prev_actv_status = 1
                    )
                  )
                  THEN CAST('1' AS VARCHAR)
                  ELSE CAST('0' AS VARCHAR)
                END AS inactive_store
              FROM (
                SELECT
                  curr_mon_stores.year_month,
                  curr_mon_stores.distributor_id,
                  curr_mon_stores.cust_cd,
                  curr_mon_stores.curr_actv_status,
                  COALESCE(
                    curr_mon_stores.curr_net_sell_out,
                    CAST((
                      CAST((
                        0
                      ) AS DECIMAL)
                    ) AS DECIMAL(18, 0))
                  ) AS curr_net_sell_out,
                  prev_mon_stores.prev_actv_status,
                  COALESCE(
                    prev_mon_stores.prev_net_sell_out,
                    CAST((
                      CAST((
                        0
                      ) AS DECIMAL)
                    ) AS DECIMAL(18, 0))
                  ) AS prev_net_sell_out
                FROM (
                  (
                    SELECT
                      cust.snap_shot_month AS year_month,
                      cust.dstrbtr_id AS distributor_id,
                      cust.ar_cd AS cust_cd,
                      cust.actv_status AS curr_actv_status,
                      COALESCE(
                        sales.curr_net_sell_out,
                        CAST((
                          CAST((
                            0
                          ) AS DECIMAL)
                        ) AS DECIMAL(18, 0))
                      ) AS curr_net_sell_out
                    FROM (
                      (
                        SELECT DISTINCT
                          TO_CHAR(
                            CAST(itg_th_dstrbtr_customer_dim_snapshot.snap_shot_dt AS TIMESTAMPNTZ),
                            CAST((
                              CAST('YYYYMM' AS VARCHAR)
                            ) AS TEXT)
                          ) AS snap_shot_month,
                          itg_th_dstrbtr_customer_dim_snapshot.dstrbtr_id,
                          itg_th_dstrbtr_customer_dim_snapshot.ar_cd,
                          itg_th_dstrbtr_customer_dim_snapshot.sls_grp,
                          itg_th_dstrbtr_customer_dim_snapshot.re_nm,
                          itg_th_dstrbtr_customer_dim_snapshot.salesareaname,
                          itg_th_dstrbtr_customer_dim_snapshot.actv_status
                        FROM itg_th_dstrbtr_customer_dim_snapshot
                      ) AS cust
                      LEFT JOIN (
                        SELECT
                          cal.cal_mnth_id,
                          sls.dstrbtr_grp_cd,
                          sls.cust_cd,
                          SUM(sls.jj_net_trd_sls) AS curr_net_sell_out
                        FROM (
                          edw_vw_th_sellout_sales_foc_fact AS sls
                            LEFT JOIN edw_vw_os_time_dim AS cal
                              ON (
                                (
                                  to_date(sls.bill_date) = to_date(CAST((
                                    cal.cal_date
                                  ) AS TIMESTAMPNTZ))
                                )
                              )
                        )
                        WHERE
                          (
                            sls.jj_net_trd_sls > CAST((
                              CAST((
                                CAST((0)  AS DECIMAL)
                              ) AS DECIMAL(18, 0))
                            ) AS DECIMAL(19, 6))
                          )
                        GROUP BY
                          cal.cal_mnth_id,
                          sls.dstrbtr_grp_cd,
                          sls.cust_cd
                      ) AS sales
                        ON (
                          (
                            (
                              (
                                UPPER(CAST((
                                  cust.dstrbtr_id
                                ) AS TEXT)) = UPPER(CAST((
                                  sales.dstrbtr_grp_cd
                                ) AS TEXT))
                              )
                              AND (
                                UPPER(CAST((
                                  cust.ar_cd
                                ) AS TEXT)) = UPPER(CAST((
                                  sales.cust_cd
                                ) AS TEXT))
                              )
                            )
                            AND (
                              cust.snap_shot_month = CAST((
                                CAST((
                                  sales.cal_mnth_id
                                ) AS VARCHAR)
                              ) AS TEXT)
                            )
                          )
                        )
                    )
                  ) AS curr_mon_stores
                  LEFT JOIN (
                    SELECT
                      cust_status.cal_mnth_id AS year_month,
                      cust_status.dstrbtr_id AS distributor_id,
                      cust_status.ar_cd AS cust_cd,
                      cust_status.actv_status AS prev_actv_status,
                      COALESCE(
                        sales.prev_net_sell_out,
                        CAST((
                          CAST((
                            0
                          ) AS DECIMAL)
                        ) AS DECIMAL(18, 0))
                      ) AS prev_net_sell_out
                    FROM (
                      (
                        SELECT
                          CAST((
                            cal.cal_mnth_id
                          ) AS VARCHAR) AS cal_mnth_id,
                          cal.l1_month,
                          cal.l3_month,
                          cust.dstrbtr_id,
                          cust.ar_cd,
                          MAX(cust.actv_status) AS actv_status
                        FROM (
                          itg_th_dstrbtr_customer_dim_snapshot AS cust
                            LEFT JOIN (
                              SELECT DISTINCT
                                edw_vw_os_time_dim.cal_mnth_id,
                                TO_CHAR(
                                  CAST(DATEADD(
                                    MONTH,
                                    (
                                      -CAST((
                                        1
                                      ) AS BIGINT)
                                    ),
                                    CAST(CAST((
                                      TO_DATE(
                                        CAST((
                                          CAST((
                                            edw_vw_os_time_dim.cal_mnth_id
                                          ) AS VARCHAR)
                                        ) AS TEXT),
                                        CAST((
                                          CAST('YYYYMM' AS VARCHAR)
                                        ) AS TEXT)
                                      )
                                    ) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ)
                                  ) AS TIMESTAMPNTZ),
                                  CAST((
                                    CAST('YYYYMM' AS VARCHAR)
                                  ) AS TEXT)
                                ) AS l1_month,
                                TO_CHAR(
                                  CAST(DATEADD(
                                    MONTH,
                                    (
                                      -CAST((
                                        3
                                      ) AS BIGINT)
                                    ),
                                    CAST(CAST((
                                      TO_DATE(
                                        CAST((
                                          CAST((
                                            edw_vw_os_time_dim.cal_mnth_id
                                          ) AS VARCHAR)
                                        ) AS TEXT),
                                        CAST((
                                          CAST('YYYYMM' AS VARCHAR)
                                        ) AS TEXT)
                                      )
                                    ) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ)
                                  ) AS TIMESTAMPNTZ),
                                  CAST((
                                    CAST('YYYYMM' AS VARCHAR)
                                  ) AS TEXT)
                                ) AS l3_month
                              FROM edw_vw_os_time_dim
                            ) AS cal
                              ON (
                                (
                                  (
                                    TO_CHAR(
                                      CAST(cust.snap_shot_dt AS TIMESTAMPNTZ),
                                      CAST((
                                        CAST('YYYYMM' AS VARCHAR)
                                      ) AS TEXT)
                                    ) >= cal.l3_month
                                  )
                                  AND (
                                    TO_CHAR(
                                      CAST(cust.snap_shot_dt AS TIMESTAMPNTZ),
                                      CAST((
                                        CAST('YYYYMM' AS VARCHAR)
                                      ) AS TEXT)
                                    ) <= cal.l1_month
                                  )
                                )
                              )
                        )
                        GROUP BY
                          CAST((
                            cal.cal_mnth_id
                          ) AS VARCHAR),
                          cal.l1_month,
                          cal.l3_month,
                          cust.dstrbtr_id,
                          cust.ar_cd
                      ) AS cust_status
                      LEFT JOIN (
                        SELECT
                          CAST((
                            cal.cal_mnth_id
                          ) AS VARCHAR) AS cal_mnth_id,
                          cal.l1_month,
                          cal.l3_month,
                          sls.dstrbtr_grp_cd,
                          sls.cust_cd,
                          SUM(sls.jj_net_trd_sls) AS prev_net_sell_out
                        FROM (
                          edw_vw_th_sellout_sales_foc_fact AS sls
                            LEFT JOIN (
                              SELECT DISTINCT
                                edw_vw_os_time_dim.cal_mnth_id,
                                TO_CHAR(
                                  CAST(DATEADD(
                                    MONTH,
                                    (
                                      -CAST((
                                        1
                                      ) AS BIGINT)
                                    ),
                                    CAST(CAST((
                                      TO_DATE(
                                        CAST((
                                          CAST((
                                            edw_vw_os_time_dim.cal_mnth_id
                                          ) AS VARCHAR)
                                        ) AS TEXT),
                                        CAST((
                                          CAST('YYYYMM' AS VARCHAR)
                                        ) AS TEXT)
                                      )
                                    ) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ)
                                  ) AS TIMESTAMPNTZ),
                                  CAST((
                                    CAST('YYYYMM' AS VARCHAR)
                                  ) AS TEXT)
                                ) AS l1_month,
                                TO_CHAR(
                                  CAST(DATEADD(
                                    MONTH,
                                    (
                                      -CAST((
                                        3
                                      ) AS BIGINT)
                                    ),
                                    CAST(CAST((
                                      TO_DATE(
                                        CAST((
                                          CAST((
                                            edw_vw_os_time_dim.cal_mnth_id
                                          ) AS VARCHAR)
                                        ) AS TEXT),
                                        CAST((
                                          CAST('YYYYMM' AS VARCHAR)
                                        ) AS TEXT)
                                      )
                                    ) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ)
                                  ) AS TIMESTAMPNTZ),
                                  CAST((
                                    CAST('YYYYMM' AS VARCHAR)
                                  ) AS TEXT)
                                ) AS l3_month
                              FROM edw_vw_os_time_dim
                            ) AS cal
                              ON (
                                (
                                  (
                                    TO_CHAR(
                                      CAST(CAST((
                                        to_date(sls.bill_date)
                                      ) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
                                      CAST((
                                        CAST('YYYYMM' AS VARCHAR)
                                      ) AS TEXT)
                                    ) >= cal.l3_month
                                  )
                                  AND (
                                    TO_CHAR(
                                      CAST(CAST((
                                        to_date(sls.bill_date)
                                      ) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
                                      CAST((
                                        CAST('YYYYMM' AS VARCHAR)
                                      ) AS TEXT)
                                    ) <= cal.l1_month
                                  )
                                )
                              )
                        )
                        WHERE
                          (
                            sls.jj_net_trd_sls > CAST((
                              CAST((
                                CAST((0)  AS DECIMAL)
                              ) AS DECIMAL(18, 0))
                            ) AS DECIMAL(19, 6))
                          )
                        GROUP BY
                          CAST((
                            cal.cal_mnth_id
                          ) AS VARCHAR),
                          cal.l1_month,
                          cal.l3_month,
                          sls.dstrbtr_grp_cd,
                          sls.cust_cd
                      ) AS sales
                        ON (
                          (
                            (
                              (
                                (
                                  (
                                    CAST((
                                      cust_status.cal_mnth_id
                                    ) AS TEXT) = CAST((
                                      sales.cal_mnth_id
                                    ) AS TEXT)
                                  )
                                  AND (
                                    cust_status.l1_month = sales.l1_month
                                  )
                                )
                                AND (
                                  cust_status.l3_month = sales.l3_month
                                )
                              )
                              AND (
                                UPPER(CAST((
                                  cust_status.dstrbtr_id
                                ) AS TEXT)) = UPPER(CAST((
                                  sales.dstrbtr_grp_cd
                                ) AS TEXT))
                              )
                            )
                            AND (
                              UPPER(CAST((
                                cust_status.ar_cd
                              ) AS TEXT)) = UPPER(CAST((
                                sales.cust_cd
                              ) AS TEXT))
                            )
                          )
                        )
                    )
                  ) AS prev_mon_stores
                    ON (
                      (
                        (
                          (
                            curr_mon_stores.year_month = CAST((
                              prev_mon_stores.year_month
                            ) AS TEXT)
                          )
                          AND (
                            UPPER(CAST((
                              curr_mon_stores.distributor_id
                            ) AS TEXT)) = UPPER(CAST((
                              prev_mon_stores.distributor_id
                            ) AS TEXT))
                          )
                        )
                        AND (
                          UPPER(CAST((
                            curr_mon_stores.cust_cd
                          ) AS TEXT)) = UPPER(CAST((
                            prev_mon_stores.cust_cd
                          ) AS TEXT))
                        )
                      )
                    )
                )
              ) AS cust_flag
            ) AS stores
            LEFT JOIN (
              SELECT DISTINCT
                itg_th_dstrbtr_customer_dim.dstrbtr_id,
                itg_th_dstrbtr_customer_dim.ar_cd,
                itg_th_dstrbtr_customer_dim.sls_grp,
                itg_th_dstrbtr_customer_dim.re_nm,
                itg_th_dstrbtr_customer_dim.salesareaname
              FROM itg_th_dstrbtr_customer_dim
            ) AS cust_dim
              ON (
                (
                  (
                    UPPER(CAST((
                      stores.distributor_id
                    ) AS TEXT)) = UPPER(CAST((
                      cust_dim.dstrbtr_id
                    ) AS TEXT))
                  )
                  AND (
                    UPPER(CAST((
                      stores.cust_cd
                    ) AS TEXT)) = UPPER(CAST((
                      cust_dim.ar_cd
                    ) AS TEXT))
                  )
                )
              )
          )
          LEFT JOIN (
            SELECT DISTINCT
              edw_vw_os_time_dim.cal_mnth_id,
              edw_vw_os_time_dim.cal_year,
              edw_vw_os_time_dim.cal_mnth_no
            FROM edw_vw_os_time_dim
          ) AS time_dim
            ON (
              (
                stores.year_month = CAST((
                  CAST((
                    time_dim.cal_mnth_id
                  ) AS VARCHAR)
                ) AS TEXT)
              )
            )
        )
        WHERE
          (
            UPPER(CAST((
              stores.distributor_id
            ) AS TEXT)) IN (
              SELECT DISTINCT
                UPPER(CAST((
                  itg_th_gt_dstrbtr_control.distributor_id
                ) AS TEXT)) AS distributor_id
              FROM itg_th_gt_dstrbtr_control
              WHERE
                (
                  UPPER(CAST((
                    itg_th_gt_dstrbtr_control.stores_flag
                  ) AS TEXT)) = CAST((
                    CAST('Y' AS VARCHAR)
                  ) AS TEXT)
                )
            )
          )
      UNION ALL

      SELECT
        CAST('ACTUAL' AS VARCHAR) AS identifier,
        sls.cntry_cd,
        CAST('THB' AS VARCHAR) AS crncy_cd,
        sls.cntry_nm,
        CAST((
          cal.cal_mnth_id
        ) AS VARCHAR) AS year_month,
        cal.cal_year AS "year",
        cal.cal_mnth_no AS "month",
        sls.dstrbtr_grp_cd AS distributor_id,
        COALESCE(cust.re_nm, CAST('N/A' AS VARCHAR)) AS store_type,
        CAST((
                              CONCAT(
                                  CAST((COALESCE(cust.sls_grp, CAST('N/A' AS VARCHAR))) AS TEXT) 
                                  , CAST((CAST('-' AS VARCHAR)) AS TEXT) 
                                  , CAST((COALESCE(cust.salesareaname, CAST('N/A' AS VARCHAR))) AS TEXT)
                              )) AS VARCHAR) AS sales_area,
        CAST((0) AS DECIMAL(18, 0)) AS sell_out,
        CAST((0) AS DECIMAL(18, 0)) AS gross_sell_out,
        CAST((0) AS DECIMAL(18, 0)) AS net_sell_out,
        CAST((0) AS DECIMAL(18, 0)) AS sellout_target,
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
        COUNT(DISTINCT cust.ar_cd) AS total_stores
      FROM (
        (
          edw_vw_th_sellout_sales_foc_fact AS sls
            LEFT JOIN edw_vw_os_time_dim AS cal
              ON (
                (
                  to_date(sls.bill_date) = to_date(CAST((
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
        CAST((
          cal.cal_mnth_id
        ) AS VARCHAR),
        cal.cal_year,
        cal.cal_mnth_no,
        sls.dstrbtr_grp_cd,
        cust.re_nm,
        cust.sls_grp,
        cust.salesareaname
   UNION ALL
        
    SELECT
      abc.identifier,
      abc.cntry_cd,
      abc.crncy_cd,
      abc.cntry_nm,
      abc.year_month,
      abc."year",
      abc."month",
      abc.distributor_id,
      abc.store_type,
      abc.sales_area,
      SUM(abc.sell_out) AS sell_out,
      SUM(abc.gross_sell_out) AS gross_sell_out,
      SUM(abc.net_sell_out) AS net_sell_out,
      SUM(abc.sellout_target) AS sellout_target,
      SUM(abc.msld_actuals_count) AS msld_actuals_count,
      SUM(abc.msld_target_count) AS msld_target_count,
      SUM(abc.planned_call_count) AS planned_call_count,
      SUM(abc.visited_call_count) AS visited_call_count,
      SUM(abc.effective_call_count) AS effective_call_count,
      SUM(abc.sales_order_count) AS sales_order_count,
      SUM(abc.on_time_count) AS on_time_count,
      SUM(abc.in_time_count) AS in_time_count,
      SUM(abc.otif_count) AS otif_count,
      SUM(abc.coverage_stores_count) AS coverage_stores_count,
      CAST((
        SUM(CAST((
          abc.reactivate_stores_count
        ) AS DECIMAL))
      ) AS VARCHAR) AS reactivate_stores_count,
      CAST((
        SUM(CAST((
          abc.inactive_stores_count
        ) AS DECIMAL))
      ) AS VARCHAR) AS inactive_stores_count,
      CAST((
        SUM(CAST((
          abc.active_stores_count
        ) AS DECIMAL))
      ) AS VARCHAR) AS active_stores_count,
      SUM(abc.route_com_all) AS route_com_all,
      SUM(abc.effective_call_all) AS effective_call_all,
      SUM(abc.planned_store) AS planned_store,
      SUM(abc.effective_store_in_plan) AS effective_store_in_plan,
      SUM(abc.effective_store_all) AS effective_store_all,
      SUM(COALESCE(abc.total_skus, CAST((
        0
      ) AS BIGINT))) AS total_skus,
      SUM(abc.total_stores) AS total_stores
    FROM (
   SELECT
        CAST('ACTUAL' AS VARCHAR) AS identifier,
        sls.cntry_cd,
        CAST('THB' AS VARCHAR) AS crncy_cd,
        sls.cntry_nm,
        CAST((
          cal.cal_mnth_id
        ) AS VARCHAR) AS year_month,
        cal.cal_year AS "year",
        cal.cal_mnth_no AS "month",
        sls.dstrbtr_grp_cd AS distributor_id,
        COALESCE(cust.re_nm, CAST('N/A' AS VARCHAR)) AS store_type,
        CAST((
                              CONCAT(
                                  CAST((COALESCE(cust.sls_grp, CAST('N/A' AS VARCHAR))) AS TEXT) 
                                  , CAST((CAST('-' AS VARCHAR)) AS TEXT) 
                                  , CAST((COALESCE(cust.salesareaname, CAST('N/A' AS VARCHAR))) AS TEXT)
                              )) AS VARCHAR) AS sales_area,
        CAST((0) AS DECIMAL(18, 0)) AS sell_out,
        CAST((0) AS DECIMAL(18, 0)) AS gross_sell_out,
        CAST((0) AS DECIMAL(18, 0)) AS net_sell_out,
        CAST((0) AS DECIMAL(18, 0)) AS sellout_target,
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
        COUNT(DISTINCT sku.parentcode) AS total_skus,
        CAST((
          0
        ) AS BIGINT) AS total_stores,
        sls.cust_cd
      FROM (
        (
          (
            edw_vw_th_sellout_sales_foc_fact AS sls
              LEFT JOIN edw_vw_os_time_dim AS cal
                ON (
                  (
                    to_date(sls.bill_date) = to_date(CAST((
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
        LEFT JOIN itg_th_dstrbtr_material_dim AS sku
          ON (
            (
              CAST((
                sls.dstrbtr_matl_num
              ) AS TEXT) = CAST((
                sku.item_cd
              ) AS TEXT)
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
        CAST((
          cal.cal_mnth_id
        ) AS VARCHAR),
        cal.cal_year,
        cal.cal_mnth_no,
        sls.dstrbtr_grp_cd,
        cust.re_nm,
        cust.sls_grp,
        cust.salesareaname,
        sls.cust_cd
    ) AS abc
    GROUP BY
      abc.identifier,
      abc.cntry_cd,
      abc.crncy_cd,
      abc.cntry_nm,
      abc.year_month,
      abc."year",
      abc."month",
      abc.distributor_id,
      abc.store_type,
      abc.sales_area
),

final as 
(
SELECT
  derived_table3.identifier,
  derived_table3.cntry_cd,
  derived_table3.crncy_cd,
  derived_table3.cntry_nm,
  derived_table3.year_month,
  derived_table3."year",
  derived_table3."month",
  derived_table3.distributor_id,
  derived_table3.store_type,
  derived_table3.sales_area,
  derived_table3.sell_out_actual,
  derived_table3.gross_sell_out_actual,
  derived_table3.net_sell_out_actual,
  derived_table3.sell_out_target,
  derived_table3.msl_actual_count,
  derived_table3.msl_target_count,
  derived_table3.planned_call_count,
  derived_table3.visited_call_count,
  derived_table3.effective_call_count,
  derived_table3.sales_order_count,
  derived_table3.on_time_count,
  derived_table3.in_time_count,
  derived_table3.otif_count,
  derived_table3.coverage_stores_count,
  derived_table3.reactivate_stores_count,
  derived_table3.inactive_stores_count,
  derived_table3.active_stores_count,
  derived_table3.route_com_all,
  derived_table3.effective_call_all,
  derived_table3.planned_store,
  derived_table3.effective_store_in_plan,
  derived_table3.effective_store_all,
  derived_table3.total_skus,
  derived_table3.total_stores
FROM (
  SELECT
    derived_table2.identifier,
    derived_table2.cntry_cd,
    derived_table2.crncy_cd,
    derived_table2.cntry_nm,
    derived_table2.year_month,
    derived_table2."year",
    derived_table2."month",
    derived_table2.distributor_id,
    derived_table2.store_type,
    derived_table2.sales_area,
    SUM(
      COALESCE(
        derived_table2.sell_out,
        CAST((0) AS DECIMAL(18, 0))
      )
    ) AS sell_out_actual,
    SUM(
      COALESCE(
        derived_table2.gross_sell_out,
        CAST((0) AS DECIMAL(18, 0))
      )
    ) AS gross_sell_out_actual,
    SUM(
      COALESCE(
        derived_table2.net_sell_out,
        CAST((0) AS DECIMAL(18, 0))
      )
    ) AS net_sell_out_actual,
    SUM(
      COALESCE(
        derived_table2.sellout_target,
        CAST((0) AS DECIMAL(18, 0))
      )
    ) AS sell_out_target,
    SUM(COALESCE(derived_table2.msld_actuals_count, CAST((0) AS BIGINT))) AS msl_actual_count,
    SUM(COALESCE(derived_table2.msld_target_count, CAST((0) AS BIGINT))) AS msl_target_count,
    SUM(COALESCE(derived_table2.planned_call_count, CAST((0) AS BIGINT))) AS planned_call_count,
    SUM(COALESCE(derived_table2.visited_call_count, CAST((0) AS BIGINT))) AS visited_call_count,
    SUM(COALESCE(derived_table2.effective_call_count, CAST((0) AS BIGINT))) AS effective_call_count,
    SUM(COALESCE(derived_table2.sales_order_count, CAST((0) AS BIGINT))) AS sales_order_count,
    SUM(COALESCE(derived_table2.on_time_count, CAST((0) AS BIGINT))) AS on_time_count,
    SUM(COALESCE(derived_table2.in_time_count, CAST((0) AS BIGINT))) AS in_time_count,
    SUM(COALESCE(derived_table2.otif_count, CAST((0) AS BIGINT))) AS otif_count,
    SUM(COALESCE(derived_table2.coverage_stores_count, CAST((0) AS BIGINT))) AS coverage_stores_count,
    SUM(
      CAST((
        COALESCE(derived_table2.reactivate_stores_count, CAST('0' AS VARCHAR))
      ) AS DECIMAL(18, 0))
    ) AS reactivate_stores_count,
    SUM(
      CAST((
        COALESCE(derived_table2.inactive_stores_count, CAST('0' AS VARCHAR))
      ) AS DECIMAL(18, 0))
    ) AS inactive_stores_count,
    (
      (
        CAST((
          CAST((
            SUM(COALESCE(derived_table2.coverage_stores_count, CAST((
              0
            ) AS BIGINT)))
          ) AS DECIMAL)
        ) AS DECIMAL(18, 0)) - SUM(
          CAST((
            COALESCE(derived_table2.inactive_stores_count, CAST('0' AS VARCHAR))
          ) AS DECIMAL(18, 0))
        )
      ) + SUM(
        CAST((
          COALESCE(derived_table2.reactivate_stores_count, CAST('0' AS VARCHAR))
        ) AS DECIMAL(18, 0))
      )
    ) AS active_stores_count,
    SUM(COALESCE(derived_table2.route_com_all, CAST((0) AS BIGINT))) AS route_com_all,
    SUM(COALESCE(derived_table2.effective_call_all, CAST((0) AS BIGINT))) AS effective_call_all,
    SUM(COALESCE(derived_table2.planned_store, CAST((0) AS BIGINT))) AS planned_store,
    SUM(COALESCE(derived_table2.effective_store_in_plan, CAST((0) AS BIGINT))) AS effective_store_in_plan,
    SUM(COALESCE(derived_table2.effective_store_all, CAST((0) AS BIGINT))) AS effective_store_all,
    SUM(COALESCE(derived_table2.total_skus, CAST((0) AS BIGINT))) AS total_skus,
    SUM(COALESCE(derived_table2.total_stores, CAST((0) AS BIGINT))) AS total_stores
  FROM (
        select * from foc_fact_sales_re
        UNION ALL
        select * from msl_distribution
        UNION ALL
        select * from th_gt_schedule
        UNION ALL 
        select * from cte4
        UNION ALL 
        select * from cte5
        UNION ALL 
        select * from cte6
  ) AS derived_table2
  WHERE
    (
      CAST((
        derived_table2."year"
      ) AS DOUBLE) >= (
        PGDATE_PART(
          CAST((
            CAST('year' AS VARCHAR)
          ) AS TEXT),
          CAST((
            to_date(CAST((
              CAST(current_timestamp AS VARCHAR)
            ) AS TIMESTAMPNTZ))
          ) AS TIMESTAMPNTZ)
        ) - CAST((
          (
            SELECT
              CAST((
                itg_query_parameters.parameter_value
              ) AS INT) AS parameter_value
            FROM itg_query_parameters
            WHERE
              (
                (
                  UPPER(CAST((
                    itg_query_parameters.country_code
                  ) AS TEXT)) = CAST((
                    CAST('TH' AS VARCHAR)
                  ) AS TEXT)
                )
                AND (
                  UPPER(CAST((
                    itg_query_parameters.parameter_name
                  ) AS TEXT)) = CAST((
                    CAST('GT_SCORECARD_TDE_DATA_RETENTION_YEARS' AS VARCHAR)
                  ) AS TEXT)
                )
              )
          )
        ) AS DOUBLE)
      )
    )
  GROUP BY
    derived_table2.identifier,
    derived_table2.cntry_cd,
    derived_table2.crncy_cd,
    derived_table2.cntry_nm,
    derived_table2.year_month,
    derived_table2."year",
    derived_table2."month",
    derived_table2.distributor_id,
    derived_table2.store_type,
    derived_table2.sales_area
) AS derived_table3
WHERE
  (
    (
      (
        (
          (
            (
              (
                (
                  (
                    (
                      (
                        (
                          (
                            (
                              (
                                (
                                  (
                                    (
                                      (
                                        (
                                          (
                                            (
                                              (
                                                (
                                                  derived_table3.sell_out_actual <> CAST((
                                                    CAST((0) AS DECIMAL)
                                                  ) AS DECIMAL(18, 0))
                                                )
                                                OR (
                                                  derived_table3.gross_sell_out_actual <> CAST((
                                                    CAST((0) AS DECIMAL)
                                                  ) AS DECIMAL(18, 0))
                                                )
                                              )
                                              OR (
                                                derived_table3.net_sell_out_actual <> CAST((
                                                  CAST((
                                                    0
                                                  ) AS DECIMAL)
                                                ) AS DECIMAL(18, 0))
                                              )
                                            )
                                            OR (
                                              derived_table3.sell_out_target <> CAST((
                                                CAST((
                                                  0
                                                ) AS DECIMAL)
                                              ) AS DECIMAL(18, 0))
                                            )
                                          )
                                          OR (
                                            derived_table3.msl_actual_count <> 0
                                          )
                                        )
                                        OR (
                                          derived_table3.msl_target_count <> 0
                                        )
                                      )
                                      OR (
                                        derived_table3.planned_call_count <> 0
                                      )
                                    )
                                    OR (
                                      derived_table3.visited_call_count <> 0
                                    )
                                  )
                                  OR (
                                    derived_table3.effective_call_count <> 0
                                  )
                                )
                                OR (
                                  derived_table3.sales_order_count <> 0
                                )
                              )
                              OR (
                                derived_table3.on_time_count <> 0
                              )
                            )
                            OR (
                              derived_table3.sell_out_target <> CAST((
                                CAST((0)  AS DECIMAL)
                              ) AS DECIMAL(18, 0))
                            )
                          )
                          OR (
                            derived_table3.msl_actual_count <> 0
                          )
                        )
                        OR (
                          derived_table3.msl_target_count <> 0
                        )
                      )
                      OR (
                        derived_table3.planned_call_count <> 0
                      )
                    )
                    OR (
                      derived_table3.visited_call_count <> 0
                    )
                  )
                  OR (
                    derived_table3.effective_call_count <> 0
                  )
                )
                OR (
                  derived_table3.sales_order_count <> 0
                )
              )
              OR (
                derived_table3.on_time_count <> 0
              )
            )
            OR (
              derived_table3.in_time_count <> 0
            )
          )
          OR (
            derived_table3.otif_count <> 0
          )
        )
        OR (
          derived_table3.coverage_stores_count <> 0
        )
      )
      OR (
        derived_table3.reactivate_stores_count <> CAST((0) AS DECIMAL(18, 0))
      )
    )
    OR (
      (
        derived_table3.inactive_stores_count <> CAST((0) AS DECIMAL(18, 0))
      )
      AND (
        derived_table3.active_stores_count <> CAST((0) AS DECIMAL(18, 0))
      )
    )
  )
)

select * from final


