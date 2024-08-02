with edw_vw_sales_rep_so_target_fact as (
select * from {{ ref('ntaedw_integration__edw_vw_sales_rep_so_target_fact') }}
),
edw_sales_rep_route_plan as (
select * from {{ ref('ntaedw_integration__edw_sales_rep_route_plan') }}
),
v_intrm_calendar_ims as (
select * from {{ ref('ntaedw_integration__v_intrm_calendar_ims') }}
),
edw_ims_fact as (
select * from {{ ref('ntaedw_integration__edw_ims_fact') }}
),
final as (
SELECT 
  DISTINCT COALESCE(tgt.cal_day, b.cal_day) AS cal_day, 
  COALESCE(tgt.ctry_cd, pln.ctry_cd) AS ctry_cd, 
  COALESCE(tgt.dstr_cd, pln.dstr_cd) AS dstr_cd, 
  COALESCE(tgt.sls_rep_cd, pln.sls_rep_cd) AS sls_rep_cd, 
  pln.sls_rep_typ, 
  tgt.brand, 
  (
    (
      (tgt.sls_trgt_val):: double precision * (
        regexp_replace(
          a.allocation, 
          (
            '[^.*[:digit:]]' :: character varying
          ):: text, 
          ('' :: character varying):: text
        )
      ):: real
    )
  ):: real AS dly_store_tgt, 
  COALESCE(
    tgt.crncy_cd, 'HKD' :: character varying
  ) AS crncy_cd, 
  pln.store_cd, 
  pln.store_class, 
  pln.visit_jj_mnth_id, 
  pln.visit_jj_wk_no, 
  pln.visit_jj_wkdy_no, 
  pln.visit_dt, 
  pln.visit_end_dt 
FROM 
  (
    (
      (
        edw_vw_sales_rep_so_target_fact tgt FULL 
        JOIN edw_sales_rep_route_plan pln ON (
          (
            (
              (
                (
                  (
                    (
                      rtrim(pln.ctry_cd):: text = rtrim(tgt.ctry_cd):: text
                    ) 
                    AND (
                      rtrim(pln.dstr_cd):: text = rtrim(tgt.dstr_cd):: text
                    )
                  ) 
                  AND (
                    rtrim(pln.sls_rep_cd):: text = rtrim(tgt.sls_rep_cd):: text
                  )
                ) 
                AND (tgt.cal_day >= pln.visit_dt)
              ) 
              AND (
                tgt.cal_day <= COALESCE(
                  pln.visit_end_dt, 
                  convert_timezone('UTC',current_timestamp()):: date
                )
              )
            ) 
            AND (
              (tgt.brand):: text = ('ALL' :: character varying):: text
            )
          )
        )
      ) 
      LEFT JOIN v_intrm_calendar_ims b ON (
        (
          (
            (b.cal_day >= pln.visit_dt) 
            AND (
              b.cal_day <= COALESCE(
                pln.visit_end_dt, 
                convert_timezone('UTC',current_timestamp()):: date
              )
            )
          ) 
          AND (b.wkday <> 7)
        )
      )
    ) 
    LEFT JOIN (
      SELECT 
        a.fisc_per, 
        a.jj_mnth_id, 
        a.ctry_cd, 
        a.dstr_cd, 
        a.sls_rep_cd, 
        a.store_cd, 
        a.store_sls, 
        a.stores_cls_total, 
        CASE WHEN (
          a.stores_cls_total <= (
            (0):: numeric
          ):: numeric(18, 0)
        ) THEN ('0' :: character varying):: text ELSE "substring"(
          (
            (
              (
                (a.store_sls):: real / (a.stores_cls_total):: real
              )
            ):: character varying
          ):: text, 
          1, 
          10
        ) END AS allocation 
      FROM 
        (
          SELECT 
            a.fisc_per, 
            a.jj_mnth_id, 
            a.st_month, 
            a.st_year, 
            a.end_month, 
            a.end_year, 
            a.st_dt, 
            a.end_dt, 
            a.ctry_cd, 
            a.dstr_cd, 
            a.sls_rep_cd, 
            a.store_cd, 
            a.store_sls, 
            sum(a.store_sls) OVER(
              PARTITION BY a.fisc_per, 
              a.sls_rep_cd order by null ROWS BETWEEN UNBOUNDED PRECEDING 
              AND UNBOUNDED FOLLOWING
            ) AS stores_cls_total 
          FROM 
            (
              SELECT 
                a.fisc_per, 
                a.jj_mnth_id, 
                a.st_month, 
                a.st_year, 
                a.end_month, 
                a.end_year, 
                a.st_dt, 
                a.end_dt, 
                a.ctry_cd, 
                a.dstr_cd, 
                a.sls_rep_cd, 
                a.store_cd, 
                COALESCE(
                  sum(
                    (b.sls_amt - b.rtrn_amt)
                  ), 
                  (
                    (0):: numeric
                  ):: numeric(18, 0)
                ) AS store_sls 
              FROM 
                (
                  (
                    SELECT 
                      a.fisc_per, 
                      a.jj_mnth_id, 
                      a.st_month, 
                      a.st_year, 
                      a.end_month, 
                      a.end_year, 
                      st_cal.st_dt, 
                      end_cal.end_dt, 
                      pln.ctry_cd, 
                      pln.dstr_cd, 
                      pln.sls_rep_cd, 
                      pln.store_cd 
                    FROM 
                      (
                        (
                          (
                            (
                              SELECT 
                                b.fisc_per, 
                                (
                                  (
                                    "substring"(
                                      (
                                        (b.fisc_per):: character varying
                                      ):: text, 
                                      1, 
                                      4
                                    ) || "substring"(
                                      (
                                        (b.fisc_per):: character varying
                                      ):: text, 
                                      6, 
                                      2
                                    )
                                  )
                                ):: integer AS jj_mnth_id, 
                                CASE WHEN (
                                  (
                                    (
                                      "substring"(
                                        (
                                          (b.fisc_per):: character varying
                                        ):: text, 
                                        6, 
                                        2
                                      )
                                    ):: integer - 6
                                  ) < 1
                                ) THEN (
                                  (
                                    "substring"(
                                      (
                                        (b.fisc_per):: character varying
                                      ):: text, 
                                      6, 
                                      2
                                    )
                                  ):: integer + 6
                                ) ELSE (
                                  (
                                    "substring"(
                                      (
                                        (b.fisc_per):: character varying
                                      ):: text, 
                                      6, 
                                      2
                                    )
                                  ):: integer - 6
                                ) END AS st_month, 
                                CASE WHEN (
                                  (
                                    (
                                      "substring"(
                                        (
                                          (b.fisc_per):: character varying
                                        ):: text, 
                                        6, 
                                        2
                                      )
                                    ):: integer - 6
                                  ) < 1
                                ) THEN (
                                  (
                                    "substring"(
                                      (
                                        (b.fisc_per):: character varying
                                      ):: text, 
                                      1, 
                                      4
                                    )
                                  ):: integer - 1
                                ) ELSE (
                                  "substring"(
                                    (
                                      (b.fisc_per):: character varying
                                    ):: text, 
                                    1, 
                                    4
                                  )
                                ):: integer END AS st_year, 
                                CASE WHEN (
                                  (
                                    (
                                      "substring"(
                                        (
                                          (b.fisc_per):: character varying
                                        ):: text, 
                                        6, 
                                        2
                                      )
                                    ):: integer - 1
                                  ) < 1
                                ) THEN (
                                  (
                                    "substring"(
                                      (
                                        (b.fisc_per):: character varying
                                      ):: text, 
                                      6, 
                                      2
                                    )
                                  ):: integer + 11
                                ) ELSE (
                                  (
                                    "substring"(
                                      (
                                        (b.fisc_per):: character varying
                                      ):: text, 
                                      6, 
                                      2
                                    )
                                  ):: integer - 1
                                ) END AS end_month, 
                                CASE WHEN (
                                  (
                                    (
                                      "substring"(
                                        (
                                          (b.fisc_per):: character varying
                                        ):: text, 
                                        6, 
                                        2
                                      )
                                    ):: integer - 1
                                  ) < 1
                                ) THEN (
                                  (
                                    "substring"(
                                      (
                                        (b.fisc_per):: character varying
                                      ):: text, 
                                      1, 
                                      4
                                    )
                                  ):: integer - 1
                                ) ELSE (
                                  "substring"(
                                    (
                                      (b.fisc_per):: character varying
                                    ):: text, 
                                    1, 
                                    4
                                  )
                                ):: integer END AS end_year 
                              FROM 
                                v_intrm_calendar_ims b 
                              GROUP BY 
                                b.fisc_per
                            ) a 
                            LEFT JOIN (
                              SELECT 
                                b.fisc_per, 
                                (
                                  "substring"(
                                    (
                                      (b.fisc_per):: character varying
                                    ):: text, 
                                    1, 
                                    4
                                  )
                                ):: integer AS yr, 
                                (
                                  "substring"(
                                    (
                                      (b.fisc_per):: character varying
                                    ):: text, 
                                    6, 
                                    2
                                  )
                                ):: integer AS mnth, 
                                min(b.cal_day) AS st_dt, 
                                "max"(b.cal_day) AS end_dt 
                              FROM 
                                v_intrm_calendar_ims b 
                              GROUP BY 
                                b.fisc_per
                            ) st_cal ON (
                              (
                                (a.st_year = st_cal.yr) 
                                AND (a.st_month = st_cal.mnth)
                              )
                            )
                          ) 
                          LEFT JOIN (
                            SELECT 
                              b.fisc_per, 
                              (
                                "substring"(
                                  (
                                    (b.fisc_per):: character varying
                                  ):: text, 
                                  1, 
                                  4
                                )
                              ):: integer AS yr, 
                              (
                                "substring"(
                                  (
                                    (b.fisc_per):: character varying
                                  ):: text, 
                                  6, 
                                  2
                                )
                              ):: integer AS mnth, 
                              min(b.cal_day) AS st_dt, 
                              "max"(b.cal_day) AS end_dt 
                            FROM 
                              v_intrm_calendar_ims b 
                            GROUP BY 
                              b.fisc_per
                          ) end_cal ON (
                            (
                              (a.end_year = end_cal.yr) 
                              AND (a.end_month = end_cal.mnth)
                            )
                          )
                        ) 
                        LEFT JOIN (
                          SELECT 
                            DISTINCT edw_sales_rep_route_plan.visit_jj_mnth_id, 
                            (
                              "substring"(
                                (
                                  (
                                    edw_sales_rep_route_plan.visit_jj_mnth_id
                                  ):: character varying
                                ):: text, 
                                1, 
                                4
                              )
                            ):: integer AS yr, 
                            (
                              "substring"(
                                (
                                  (
                                    edw_sales_rep_route_plan.visit_jj_mnth_id
                                  ):: character varying
                                ):: text, 
                                5, 
                                2
                              )
                            ):: integer AS mnth, 
                            edw_sales_rep_route_plan.ctry_cd, 
                            edw_sales_rep_route_plan.dstr_cd, 
                            edw_sales_rep_route_plan.sls_rep_cd, 
                            edw_sales_rep_route_plan.store_cd 
                          FROM 
                            edw_sales_rep_route_plan
                        ) pln ON (
                          (
                            a.jj_mnth_id = pln.visit_jj_mnth_id
                          )
                        )
                      )
                  ) a 
                  LEFT JOIN edw_ims_fact b ON (
                    (
                      (
                        (
                          (
                            (
                              rtrim(a.dstr_cd):: text = rtrim(b.dstr_cd):: text
                            ) 
                            AND (
                              rtrim(b.cust_cd):: text = rtrim(a.store_cd):: text
                            )
                          ) 
                          AND (
                            rtrim(a.ctry_cd):: text = rtrim(b.ctry_cd):: text
                          )
                        ) 
                        AND (b.ims_txn_dt >= a.st_dt)
                      ) 
                      AND (b.ims_txn_dt <= a.end_dt)
                    )
                  )
                ) 
              GROUP BY 
                a.fisc_per, 
                a.jj_mnth_id, 
                a.st_month, 
                a.st_year, 
                a.end_month, 
                a.end_year, 
                a.st_dt, 
                a.end_dt, 
                a.ctry_cd, 
                a.dstr_cd, 
                a.sls_rep_cd, 
                a.store_cd
            ) a
        ) a
    ) a ON (
      (
        (
          (
            (
              (
                rtrim(pln.ctry_cd):: text = rtrim(a.ctry_cd):: text
              ) 
              AND (
                rtrim(pln.dstr_cd):: text = rtrim(a.dstr_cd):: text
              )
            ) 
            AND (
              rtrim(pln.sls_rep_cd):: text = rtrim(a.sls_rep_cd):: text
            )
          ) 
          AND (
            rtrim(pln.store_cd):: text = rtrim(a.store_cd):: text
          )
        ) 
        AND rtrim(a.jj_mnth_id) = rtrim(tgt.jj_mnth_id)
      )
    )
  ) 
WHERE 
  (
    COALESCE(tgt.cal_day, b.cal_day) <= (
      SELECT 
        "max"(edw_ims_fact.ims_txn_dt) AS "max" 
      FROM 
        edw_ims_fact
    )
  )
)select * from final