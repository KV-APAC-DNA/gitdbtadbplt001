{{
    config(
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";"
    )
}}
with v_intrm_sls_pln_actl as (
select * from {{ ref('ntaedw_integration__v_intrm_sls_pln_actl') }}
),
v_intrm_curr_copa_plan_flat as (
select * from {{ ref('ntaedw_integration__v_intrm_curr_copa_plan_flat') }}
),
derived_table1 as  (
    SELECT 
      COALESCE(a.ctry_nm, b.ctry_nm) AS ctry_nm, 
      COALESCE(a.cust_num, b.cust_num) AS cust_num, 
      COALESCE(a.cust_nm, b.cust_nm) AS cust_nm, 
      COALESCE(a.matl_num, b.matl_num) AS matl_num, 
      COALESCE(a.matl_desc, b.matl_desc) AS matl_desc, 
      COALESCE(
        a.mega_brnd_desc, b.mega_brnd_desc
      ) AS mega_brnd_desc, 
      COALESCE(a.brnd_desc, b.brnd_desc) AS brnd_desc, 
      CASE WHEN (
        (
          COALESCE(a.ctry_nm, b.ctry_nm)
        ):: text = ('Hong Kong' :: character varying):: text
      ) THEN 'Hong Kong' :: character varying WHEN (
        (
          COALESCE(a.ctry_nm, b.ctry_nm)
        ):: text = ('Taiwan' :: character varying):: text
      ) THEN 'Taiwan' :: character varying WHEN (
        (
          COALESCE(a.ctry_nm, b.ctry_nm)
        ):: text = (
          'South Korea' :: character varying
        ):: text
      ) THEN 'South Korea' :: character varying ELSE 'NA' :: character varying END AS prod_hier_l1, 
      COALESCE(a.prod_hier_l2, b.prod_hier_l2) AS prod_hier_l2, 
      COALESCE(a.prod_hier_l3, b.prod_hier_l3) AS prod_hier_l3, 
      COALESCE(a.prod_hier_l4, b.prod_hier_l4) AS prod_hier_l4, 
      COALESCE(a.prod_hier_l5, b.prod_hier_l5) AS prod_hier_l5, 
      COALESCE(a.prod_hier_l6, b.prod_hier_l6) AS prod_hier_l6, 
      COALESCE(a.prod_hier_l7, b.prod_hier_l7) AS prod_hier_l7, 
      COALESCE(a.prod_hier_l8, b.prod_hier_l8) AS prod_hier_l8, 
      COALESCE(a.prod_hier_l9, b.prod_hier_l9) AS prod_hier_l9, 
      COALESCE(a.cust_hier_l1, b.cust_hier_l1) AS cust_hier_l1, 
      COALESCE(a.cust_hier_l2, b.cust_hier_l2) AS cust_hier_l2, 
      COALESCE(a.cust_hier_l3, b.cust_hier_l3) AS cust_hier_l3, 
      COALESCE(a.cust_hier_l4, b.cust_hier_l4) AS cust_hier_l4, 
      COALESCE(a.cust_hier_l5, b.cust_hier_l5) AS cust_hier_l5, 
      COALESCE(a.sls_grp, b.sls_grp) AS sls_grp, 
      COALESCE(a.sls_ofc_desc, b.sls_ofc_desc) AS sls_ofc_desc, 
      COALESCE(a.channel, b.channel) AS channel, 
      COALESCE(a.store_type, b.store_type) AS store_type, 
      COALESCE(
        a.fisc_yr_per, 
        (b.fisc_yr_per):: character varying(10)
      ) AS fisc_yr_per, 
      COALESCE(
        a.acct_hier_shrt_desc, b.acct_hier_shrt_desc
      ) AS acct_hier_shrt_desc, 
      COALESCE(a.from_crncy, b.from_crncy) AS from_crncy, 
      COALESCE(a.to_crncy, b.to_crncy) AS to_crncy, 
      COALESCE(
        a.ex_rt_typ, 'BWAR' :: character varying
      ) AS ex_rt_typ, 
      COALESCE(a.ex_rt, b.ex_rt) AS ex_rt, 
      COALESCE(a.copa_val, 0.0) AS copa_val, 
      COALESCE(a.net_bill_val, 0.0) AS net_bill_val, 
      COALESCE(a.ord_qty_pc, 0.0) AS ord_qty_pc, 
      COALESCE(b.rf_total, 0.0) AS rf_total, 
      COALESCE(b.bp_total, 0.0) AS bp_total, 
      COALESCE(b.le_total, 0.0) AS le_total, 
      a.timegone 
    FROM 
      (
        v_intrm_sls_pln_actl a FULL 
        JOIN (
          SELECT 
            v_intrm_copa_plan_flat.ctry_nm, 
            v_intrm_copa_plan_flat.cust_num, 
            v_intrm_copa_plan_flat.cust_nm, 
            v_intrm_copa_plan_flat.matl_num, 
            v_intrm_copa_plan_flat.matl_desc, 
            v_intrm_copa_plan_flat.mega_brnd_desc, 
            v_intrm_copa_plan_flat.brnd_desc, 
            CASE WHEN (
              v_intrm_copa_plan_flat.prod_hier_l1 IS NULL
            ) THEN '#' :: character varying WHEN (
              (
                v_intrm_copa_plan_flat.prod_hier_l1
              ):: text = ('' :: character varying):: text
            ) THEN '#' :: character varying ELSE v_intrm_copa_plan_flat.prod_hier_l1 END AS prod_hier_l1, 
            CASE WHEN (
              v_intrm_copa_plan_flat.prod_hier_l2 IS NULL
            ) THEN '#' :: character varying WHEN (
              (
                v_intrm_copa_plan_flat.prod_hier_l2
              ):: text = ('' :: character varying):: text
            ) THEN '#' :: character varying ELSE v_intrm_copa_plan_flat.prod_hier_l2 END AS prod_hier_l2, 
            CASE WHEN (
              v_intrm_copa_plan_flat.prod_hier_l3 IS NULL
            ) THEN '#' :: character varying WHEN (
              (
                v_intrm_copa_plan_flat.prod_hier_l3
              ):: text = ('' :: character varying):: text
            ) THEN '#' :: character varying ELSE v_intrm_copa_plan_flat.prod_hier_l3 END AS prod_hier_l3, 
            CASE WHEN (
              v_intrm_copa_plan_flat.prod_hier_l4 IS NULL
            ) THEN '#' :: character varying WHEN (
              (
                v_intrm_copa_plan_flat.prod_hier_l4
              ):: text = ('' :: character varying):: text
            ) THEN '#' :: character varying ELSE v_intrm_copa_plan_flat.prod_hier_l4 END AS prod_hier_l4, 
            CASE WHEN (
              v_intrm_copa_plan_flat.prod_hier_l5 IS NULL
            ) THEN '#' :: character varying WHEN (
              (
                v_intrm_copa_plan_flat.prod_hier_l5
              ):: text = ('' :: character varying):: text
            ) THEN '#' :: character varying ELSE v_intrm_copa_plan_flat.prod_hier_l5 END AS prod_hier_l5, 
            CASE WHEN (
              v_intrm_copa_plan_flat.prod_hier_l6 IS NULL
            ) THEN '#' :: character varying WHEN (
              (
                v_intrm_copa_plan_flat.prod_hier_l6
              ):: text = ('' :: character varying):: text
            ) THEN '#' :: character varying ELSE v_intrm_copa_plan_flat.prod_hier_l6 END AS prod_hier_l6, 
            CASE WHEN (
              v_intrm_copa_plan_flat.prod_hier_l7 IS NULL
            ) THEN '#' :: character varying WHEN (
              (
                v_intrm_copa_plan_flat.prod_hier_l7
              ):: text = ('' :: character varying):: text
            ) THEN '#' :: character varying ELSE v_intrm_copa_plan_flat.prod_hier_l7 END AS prod_hier_l7, 
            CASE WHEN (
              v_intrm_copa_plan_flat.prod_hier_l8 IS NULL
            ) THEN '#' :: character varying WHEN (
              (
                v_intrm_copa_plan_flat.prod_hier_l8
              ):: text = ('' :: character varying):: text
            ) THEN '#' :: character varying ELSE v_intrm_copa_plan_flat.prod_hier_l8 END AS prod_hier_l8, 
            CASE WHEN (
              v_intrm_copa_plan_flat.prod_hier_l9 IS NULL
            ) THEN '#' :: character varying WHEN (
              (
                v_intrm_copa_plan_flat.prod_hier_l9
              ):: text = ('' :: character varying):: text
            ) THEN '#' :: character varying ELSE v_intrm_copa_plan_flat.prod_hier_l9 END AS prod_hier_l9, 
            CASE WHEN (
              v_intrm_copa_plan_flat.cust_hier_l1 IS NULL
            ) THEN '#' :: character varying WHEN (
              (
                v_intrm_copa_plan_flat.cust_hier_l1
              ):: text = ('' :: character varying):: text
            ) THEN '#' :: character varying ELSE v_intrm_copa_plan_flat.cust_hier_l1 END AS cust_hier_l1, 
            CASE WHEN (
              v_intrm_copa_plan_flat.cust_hier_l2 IS NULL
            ) THEN '#' :: character varying WHEN (
              (
                v_intrm_copa_plan_flat.cust_hier_l2
              ):: text = ('' :: character varying):: text
            ) THEN '#' :: character varying ELSE v_intrm_copa_plan_flat.cust_hier_l2 END AS cust_hier_l2, 
            CASE WHEN (
              v_intrm_copa_plan_flat.cust_hier_l3 IS NULL
            ) THEN '#' :: character varying WHEN (
              (
                v_intrm_copa_plan_flat.cust_hier_l3
              ):: text = ('' :: character varying):: text
            ) THEN '#' :: character varying ELSE v_intrm_copa_plan_flat.cust_hier_l3 END AS cust_hier_l3, 
            CASE WHEN (
              v_intrm_copa_plan_flat.cust_hier_l4 IS NULL
            ) THEN '#' :: character varying WHEN (
              (
                v_intrm_copa_plan_flat.cust_hier_l4
              ):: text = ('' :: character varying):: text
            ) THEN '#' :: character varying ELSE v_intrm_copa_plan_flat.cust_hier_l4 END AS cust_hier_l4, 
            CASE WHEN (
              v_intrm_copa_plan_flat.cust_hier_l5 IS NULL
            ) THEN '#' :: character varying WHEN (
              (
                v_intrm_copa_plan_flat.cust_hier_l5
              ):: text = ('' :: character varying):: text
            ) THEN '#' :: character varying ELSE v_intrm_copa_plan_flat.cust_hier_l5 END AS cust_hier_l5, 
            COALESCE(
              v_intrm_copa_plan_flat.sls_grp, 
              '#' :: character varying
            ) AS sls_grp, 
            COALESCE(
              v_intrm_copa_plan_flat.sls_ofc_desc, 
              '#' :: character varying
            ) AS sls_ofc_desc, 
            COALESCE(
              v_intrm_copa_plan_flat.channel, 
              '#' :: character varying
            ) AS channel, 
            COALESCE(
              v_intrm_copa_plan_flat.store_type, 
              '#' :: character varying
            ) AS store_type, 
            v_intrm_copa_plan_flat.fisc_yr_per, 
            v_intrm_copa_plan_flat.acct_hier_shrt_desc, 
            v_intrm_copa_plan_flat.from_crncy, 
            v_intrm_copa_plan_flat.to_crncy, 
            v_intrm_copa_plan_flat.ex_rt, 
            sum(
              v_intrm_copa_plan_flat.rf_total
            ) AS rf_total, 
            sum(
              v_intrm_copa_plan_flat.bp_total
            ) AS bp_total, 
            sum(
              v_intrm_copa_plan_flat.le_total
            ) AS le_total 
          FROM 
            v_intrm_curr_copa_plan_flat v_intrm_copa_plan_flat 
          GROUP BY 
            v_intrm_copa_plan_flat.ctry_nm, 
            v_intrm_copa_plan_flat.cust_num, 
            v_intrm_copa_plan_flat.cust_nm, 
            v_intrm_copa_plan_flat.matl_num, 
            v_intrm_copa_plan_flat.matl_desc, 
            v_intrm_copa_plan_flat.mega_brnd_desc, 
            v_intrm_copa_plan_flat.brnd_desc, 
            CASE WHEN (
              v_intrm_copa_plan_flat.prod_hier_l1 IS NULL
            ) THEN '#' :: character varying WHEN (
              (
                v_intrm_copa_plan_flat.prod_hier_l1
              ):: text = ('' :: character varying):: text
            ) THEN '#' :: character varying ELSE v_intrm_copa_plan_flat.prod_hier_l1 END, 
            CASE WHEN (
              v_intrm_copa_plan_flat.prod_hier_l2 IS NULL
            ) THEN '#' :: character varying WHEN (
              (
                v_intrm_copa_plan_flat.prod_hier_l2
              ):: text = ('' :: character varying):: text
            ) THEN '#' :: character varying ELSE v_intrm_copa_plan_flat.prod_hier_l2 END, 
            CASE WHEN (
              v_intrm_copa_plan_flat.prod_hier_l3 IS NULL
            ) THEN '#' :: character varying WHEN (
              (
                v_intrm_copa_plan_flat.prod_hier_l3
              ):: text = ('' :: character varying):: text
            ) THEN '#' :: character varying ELSE v_intrm_copa_plan_flat.prod_hier_l3 END, 
            CASE WHEN (
              v_intrm_copa_plan_flat.prod_hier_l4 IS NULL
            ) THEN '#' :: character varying WHEN (
              (
                v_intrm_copa_plan_flat.prod_hier_l4
              ):: text = ('' :: character varying):: text
            ) THEN '#' :: character varying ELSE v_intrm_copa_plan_flat.prod_hier_l4 END, 
            CASE WHEN (
              v_intrm_copa_plan_flat.prod_hier_l5 IS NULL
            ) THEN '#' :: character varying WHEN (
              (
                v_intrm_copa_plan_flat.prod_hier_l5
              ):: text = ('' :: character varying):: text
            ) THEN '#' :: character varying ELSE v_intrm_copa_plan_flat.prod_hier_l5 END, 
            CASE WHEN (
              v_intrm_copa_plan_flat.prod_hier_l6 IS NULL
            ) THEN '#' :: character varying WHEN (
              (
                v_intrm_copa_plan_flat.prod_hier_l6
              ):: text = ('' :: character varying):: text
            ) THEN '#' :: character varying ELSE v_intrm_copa_plan_flat.prod_hier_l6 END, 
            CASE WHEN (
              v_intrm_copa_plan_flat.prod_hier_l7 IS NULL
            ) THEN '#' :: character varying WHEN (
              (
                v_intrm_copa_plan_flat.prod_hier_l7
              ):: text = ('' :: character varying):: text
            ) THEN '#' :: character varying ELSE v_intrm_copa_plan_flat.prod_hier_l7 END, 
            CASE WHEN (
              v_intrm_copa_plan_flat.prod_hier_l8 IS NULL
            ) THEN '#' :: character varying WHEN (
              (
                v_intrm_copa_plan_flat.prod_hier_l8
              ):: text = ('' :: character varying):: text
            ) THEN '#' :: character varying ELSE v_intrm_copa_plan_flat.prod_hier_l8 END, 
            CASE WHEN (
              v_intrm_copa_plan_flat.prod_hier_l9 IS NULL
            ) THEN '#' :: character varying WHEN (
              (
                v_intrm_copa_plan_flat.prod_hier_l9
              ):: text = ('' :: character varying):: text
            ) THEN '#' :: character varying ELSE v_intrm_copa_plan_flat.prod_hier_l9 END, 
            CASE WHEN (
              v_intrm_copa_plan_flat.cust_hier_l1 IS NULL
            ) THEN '#' :: character varying WHEN (
              (
                v_intrm_copa_plan_flat.cust_hier_l1
              ):: text = ('' :: character varying):: text
            ) THEN '#' :: character varying ELSE v_intrm_copa_plan_flat.cust_hier_l1 END, 
            CASE WHEN (
              v_intrm_copa_plan_flat.cust_hier_l2 IS NULL
            ) THEN '#' :: character varying WHEN (
              (
                v_intrm_copa_plan_flat.cust_hier_l2
              ):: text = ('' :: character varying):: text
            ) THEN '#' :: character varying ELSE v_intrm_copa_plan_flat.cust_hier_l2 END, 
            CASE WHEN (
              v_intrm_copa_plan_flat.cust_hier_l3 IS NULL
            ) THEN '#' :: character varying WHEN (
              (
                v_intrm_copa_plan_flat.cust_hier_l3
              ):: text = ('' :: character varying):: text
            ) THEN '#' :: character varying ELSE v_intrm_copa_plan_flat.cust_hier_l3 END, 
            CASE WHEN (
              v_intrm_copa_plan_flat.cust_hier_l4 IS NULL
            ) THEN '#' :: character varying WHEN (
              (
                v_intrm_copa_plan_flat.cust_hier_l4
              ):: text = ('' :: character varying):: text
            ) THEN '#' :: character varying ELSE v_intrm_copa_plan_flat.cust_hier_l4 END, 
            CASE WHEN (
              v_intrm_copa_plan_flat.cust_hier_l5 IS NULL
            ) THEN '#' :: character varying WHEN (
              (
                v_intrm_copa_plan_flat.cust_hier_l5
              ):: text = ('' :: character varying):: text
            ) THEN '#' :: character varying ELSE v_intrm_copa_plan_flat.cust_hier_l5 END, 
            COALESCE(
              v_intrm_copa_plan_flat.sls_grp, 
              '#' :: character varying
            ), 
            COALESCE(
              v_intrm_copa_plan_flat.sls_ofc_desc, 
              '#' :: character varying
            ), 
            COALESCE(
              v_intrm_copa_plan_flat.channel, 
              '#' :: character varying
            ), 
            COALESCE(
              v_intrm_copa_plan_flat.store_type, 
              '#' :: character varying
            ), 
            v_intrm_copa_plan_flat.fisc_yr_per, 
            v_intrm_copa_plan_flat.acct_hier_shrt_desc, 
            v_intrm_copa_plan_flat.from_crncy, 
            v_intrm_copa_plan_flat.to_crncy, 
            v_intrm_copa_plan_flat.ex_rt
        ) b ON (
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
                                                          (
                                                            (
                                                              (
                                                                (
                                                                  (
                                                                    (a.ctry_nm):: text = (b.ctry_nm):: text
                                                                  ) 
                                                                  AND (
                                                                    trim(
                                                                      ltrim(
                                                                        (a.cust_num):: text, 
                                                                        (
                                                                          (0):: character varying
                                                                        ):: text
                                                                      )
                                                                    ) = trim(
                                                                      ltrim(
                                                                        (b.cust_num):: text, 
                                                                        (
                                                                          (0):: character varying
                                                                        ):: text
                                                                      )
                                                                    )
                                                                  )
                                                                ) 
                                                                AND (
                                                                  trim(
                                                                    (a.cust_nm):: text
                                                                  ) = trim(
                                                                    (b.cust_nm):: text
                                                                  )
                                                                )
                                                              ) 
                                                              AND (
                                                                trim(
                                                                  ltrim(
                                                                    (a.matl_num):: text, 
                                                                    (
                                                                      (0):: character varying
                                                                    ):: text
                                                                  )
                                                                ) = trim(
                                                                  ltrim(
                                                                    (b.matl_num):: text, 
                                                                    (
                                                                      (0):: character varying
                                                                    ):: text
                                                                  )
                                                                )
                                                              )
                                                            ) 
                                                            AND (
                                                              trim(
                                                                (a.matl_desc):: text
                                                              ) = trim(
                                                                (b.matl_desc):: text
                                                              )
                                                            )
                                                          ) 
                                                          AND (
                                                            (a.mega_brnd_desc):: text = (b.mega_brnd_desc):: text
                                                          )
                                                        ) 
                                                        AND (
                                                          (a.brnd_desc):: text = (b.brnd_desc):: text
                                                        )
                                                      ) 
                                                      AND (
                                                        (a.prod_hier_l1):: text = (b.prod_hier_l1):: text
                                                      )
                                                    ) 
                                                    AND (
                                                      (a.prod_hier_l2):: text = (b.prod_hier_l2):: text
                                                    )
                                                  ) 
                                                  AND (
                                                    (a.prod_hier_l3):: text = (b.prod_hier_l3):: text
                                                  )
                                                ) 
                                                AND (
                                                  (a.prod_hier_l4):: text = (b.prod_hier_l4):: text
                                                )
                                              ) 
                                              AND (
                                                (a.prod_hier_l5):: text = (b.prod_hier_l5):: text
                                              )
                                            ) 
                                            AND (
                                              (a.prod_hier_l6):: text = (b.prod_hier_l6):: text
                                            )
                                          ) 
                                          AND (
                                            (a.prod_hier_l7):: text = (b.prod_hier_l7):: text
                                          )
                                        ) 
                                        AND (
                                          (a.prod_hier_l8):: text = (b.prod_hier_l8):: text
                                        )
                                      ) 
                                      AND (
                                        (a.prod_hier_l9):: text = (b.prod_hier_l9):: text
                                      )
                                    ) 
                                    AND (
                                      (a.cust_hier_l1):: text = (b.cust_hier_l1):: text
                                    )
                                  ) 
                                  AND (
                                    (a.cust_hier_l2):: text = (b.cust_hier_l2):: text
                                  )
                                ) 
                                AND (
                                  (a.cust_hier_l3):: text = (b.cust_hier_l3):: text
                                )
                              ) 
                              AND (
                                (a.cust_hier_l4):: text = (b.cust_hier_l4):: text
                              )
                            ) 
                            AND (
                              (a.cust_hier_l5):: text = (b.cust_hier_l5):: text
                            )
                          ) 
                          AND (
                            (a.sls_grp):: text = (b.sls_grp):: text
                          )
                        ) 
                        AND (
                          (a.sls_ofc_desc):: text = (b.sls_ofc_desc):: text
                        )
                      ) 
                      AND (
                        (a.channel):: text = (b.channel):: text
                      )
                    ) 
                    AND (
                      (a.fisc_yr_per):: text = (
                        (b.fisc_yr_per):: character varying
                      ):: text
                    )
                  ) 
                  AND (
                    (a.from_crncy):: text = (b.from_crncy):: text
                  )
                ) 
                AND (
                  (a.acct_hier_shrt_desc):: text = (b.acct_hier_shrt_desc):: text
                )
              ) 
              AND (
                (a.to_crncy):: text = (b.to_crncy):: text
              )
            ) 
            AND (a.ex_rt = b.ex_rt)
          )
        )
      )
  ),
final as (
SELECT 
  derived_table1.ctry_nm, 
  derived_table1.cust_num, 
  derived_table1.cust_nm, 
  derived_table1.matl_num, 
  derived_table1.matl_desc, 
  derived_table1.mega_brnd_desc, 
  derived_table1.brnd_desc, 
  derived_table1.prod_hier_l1, 
  derived_table1.prod_hier_l2, 
  derived_table1.prod_hier_l3, 
  derived_table1.prod_hier_l4, 
  derived_table1.prod_hier_l5, 
  derived_table1.prod_hier_l6, 
  derived_table1.prod_hier_l7, 
  derived_table1.prod_hier_l8, 
  derived_table1.prod_hier_l9, 
  derived_table1.cust_hier_l1, 
  derived_table1.cust_hier_l2, 
  derived_table1.cust_hier_l3, 
  derived_table1.cust_hier_l4, 
  derived_table1.cust_hier_l5, 
  derived_table1.sls_grp, 
  derived_table1.sls_ofc_desc, 
  derived_table1.channel, 
  derived_table1.store_type, 
  derived_table1.fisc_yr_per, 
  derived_table1.acct_hier_shrt_desc, 
  derived_table1.from_crncy, 
  derived_table1.to_crncy, 
  derived_table1.ex_rt_typ, 
  derived_table1.ex_rt, 
  derived_table1.copa_val, 
  derived_table1.net_bill_val, 
  derived_table1.ord_qty_pc, 
  CASE WHEN (
    derived_table1.rf_total = (
      (0):: numeric
    ):: numeric(18, 0)
  ) THEN (NULL :: numeric):: numeric(18, 0) ELSE derived_table1.rf_total END AS rf_total, 
  CASE WHEN (
    derived_table1.bp_total = (
      (0):: numeric
    ):: numeric(18, 0)
  ) THEN (NULL :: numeric):: numeric(18, 0) ELSE derived_table1.bp_total END AS bp_total, 
  CASE WHEN (
    derived_table1.le_total = (
      (0):: numeric
    ):: numeric(18, 0)
  ) THEN (NULL :: numeric):: numeric(18, 0) ELSE derived_table1.le_total END AS le_total, 
  CASE WHEN (
    (
      (
        "substring"(
          (derived_table1.fisc_yr_per):: text, 
          1, 
          4
        )
      ):: integer = "date_part"(
        year, 
        CONVERT_TIMEZONE('UTC',current_timestamp()):: timestamp without time zone
      )
    ) 
    AND (
      (
        "substring"(
          (derived_table1.fisc_yr_per):: text, 
          6, 
          2
        )
      ):: integer = "date_part"(
        month, 
        CONVERT_TIMEZONE('UTC',current_timestamp()):: timestamp without time zone
      )
    )
  ) THEN (
    (
      (
        (
          (
            "date_part"(
              day, 
              convert_timezone('UTC',current_timestamp()):: timestamp without time zone
            )
          ):: numeric
        ):: numeric(18, 0)
      ):: numeric(15, 0) / (
        (
          "date_part"(
            day, 
            last_day(
              (
                (
                  to_date(
                    (
                      "substring"(
                        (derived_table1.fisc_yr_per):: text, 
                        1, 
                        4
                      ) || "substring"(
                        (derived_table1.fisc_yr_per):: text, 
                        6, 
                        7
                      )
                    ) || ('01' :: character varying):: text,'yyyymmdd'
                  )
                )
              ):: timestamp without time zone
            )
          )
        ):: numeric
      ):: numeric(18, 0)
    ) * (
      (100):: numeric
    ):: numeric(18, 0)
  ) WHEN (
    (
      (
        "substring"(
          (derived_table1.fisc_yr_per):: text, 
          1, 
          4
        )
      ):: integer = "date_part"(
        year, 
        CONVERT_TIMEZONE('UTC',current_timestamp()):: timestamp without time zone
      )
    ) 
    AND (
      (
        "substring"(
          (derived_table1.fisc_yr_per):: text, 
          6, 
          2
        )
      ):: integer > "date_part"(
        month, 
        CONVERT_TIMEZONE('UTC',current_timestamp()):: timestamp without time zone
      )
    )
  ) THEN (
    (999):: numeric
  ):: numeric(18, 0) ELSE (
    (100):: numeric
  ):: numeric(18, 0) END AS timegone 
FROM 
 derived_table1
WHERE 
  (
    (
      (derived_table1.fisc_yr_per):: text <> (
        (0):: character varying
      ):: text
    ) 
    AND (
      "substring"(
        (derived_table1.fisc_yr_per):: text, 
        1, 
        4
      ) > (
        (
          (
            "date_part"(
              year, 
              convert_timezone('UTC',current_timestamp())::date
            ) -3
          )
        ):: character varying
      ):: text
    )
  )
)
select * from final





