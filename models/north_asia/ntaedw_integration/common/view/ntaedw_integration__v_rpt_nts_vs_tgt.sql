with v_intrm_copa_trans as (
select * from DEV_DNA_CORE.NTAEDW_INTEGRATION.V_INTRM_COPA_TRANS limit 10
),
edw_kr_sales_tgt as (
select * from DEV_DNA_CORE.NTAEDW_INTEGRATION.EDW_KR_SALES_TGT
),
V_INTRM_CRNCY_EXCH as (
select * from DEV_DNA_CORE.NTAEDW_INTEGRATION.V_INTRM_CRNCY_EXCH
),
EDW_CALENDAR_DIM as (
select * from DEV_DNA_CORE.ASPEDW_INTEGRATION.EDW_CALENDAR_DIM
),
EDW_CUSTOMER_ATTR_FLAT_DIM as (
select * from DEV_DNA_CORE.ASPEDW_INTEGRATION.EDW_CUSTOMER_ATTR_FLAT_DIM
),
final as (
SELECT 
  main.fisc_yr_per, 
  to_date(
    (
      (
        "substring"(
          (
            (main.fisc_yr_per):: character varying
          ):: text, 
          6, 
          8
        ) || ('01' :: character varying):: text
      ) || "substring"(
        (
          (main.fisc_yr_per):: character varying
        ):: text, 
        1, 
        4
      )
    ), 
    ('MMDDYYYY' :: character varying):: text
  ) AS fisc_day, 
  main.sls_ofc, 
  main.sls_ofc_desc, 
  main.channel, 
  main.sls_grp, 
  cus.sls_grp AS sls_grp_desc, 
  main.ctry_key, 
  main.ctry_nm, 
  main.store_type, 
  COALESCE(
    main.prod_hier_lvl2, 'Not Available' :: character varying
  ) AS prod_hier_lvl2, 
  COALESCE(
    main.prod_hier_lvl4, 'Not Available' :: character varying
  ) AS prod_hier_lvl4, 
  main.target_type, 
  main.from_crncy, 
  main.to_crncy, 
  main.ex_rt, 
  sum(main.amt_obj_crncy) AS amt_obj_crncy, 
  sum(main.target_amt) AS target_amt, 
  CASE WHEN (
    datediff(
      day, 
      (cd.start_date):: timestamp without time zone, 
      (cd.end_date):: timestamp without time zone
    ) = 27
  ) THEN 20 WHEN (
    datediff(
      day, 
      (cd.start_date):: timestamp without time zone, 
      (cd.end_date):: timestamp without time zone
    ) = 34
  ) THEN 25 ELSE 0 END AS total_working_days, 
  CASE WHEN (
    (
     current_timestamp() >= cd.start_date
    ) 
    AND (
     current_timestamp() <= cd.end_date
    )
  ) THEN (
    (
      datediff(
        day, 
        (cd.start_date):: timestamp without time zone, 
       current_timestamp()
      )
    ):: double precision - (
      floor(
        (
          (
            datediff(
              day, 
              (cd.start_date):: timestamp without time zone, 
             current_timestamp()
            ) / 7
          )
        ):: double precision
      ) * (2):: double precision
    )
  ) WHEN (
   current_timestamp() < cd.start_date
  ) THEN (0):: double precision WHEN (
    (
     current_timestamp() > cd.end_date
    ) 
    AND (
      datediff(
        day, 
        (cd.start_date):: timestamp without time zone, 
        (cd.end_date):: timestamp without time zone
      ) = 27
    )
  ) THEN (20):: double precision WHEN (
    (
     current_timestamp() > cd.end_date
    ) 
    AND (
      datediff(
        day, 
        (cd.start_date):: timestamp without time zone, 
        (cd.end_date):: timestamp without time zone
      ) = 34
    )
  ) THEN (25):: double precision ELSE (0):: double precision END AS working_days_elapsed 
FROM 
  (
    (
      (
        SELECT 
          edw_copa_trans_fact.fisc_yr_per, 
          edw_copa_trans_fact.sls_ofc, 
          edw_copa_trans_fact.sls_ofc_desc, 
          edw_copa_trans_fact.channel, 
          edw_copa_trans_fact.sls_grp, 
          edw_copa_trans_fact.ctry_key, 
          edw_copa_trans_fact.ctry_nm, 
          edw_copa_trans_fact.store_type, 
          edw_copa_trans_fact.prod_hier_lvl2, 
          edw_copa_trans_fact.prod_hier_lvl4, 
          '' :: character varying AS target_type, 
          edw_copa_trans_fact.from_crncy, 
          edw_copa_trans_fact.to_crncy, 
          edw_copa_trans_fact.ex_rt, 
          edw_copa_trans_fact.amt_obj_crncy, 
          0 AS target_amt 
        FROM 
          v_intrm_copa_trans edw_copa_trans_fact 
        WHERE 
          (
            (
              (
                edw_copa_trans_fact.acct_hier_shrt_desc
              ):: text = ('NTS' :: character varying):: text
            ) 
            AND (
              (
                (
                  edw_copa_trans_fact.fisc_yr_per
                ):: character varying
              ):: text >= (
                (
                  (
                    (
                      (
                        (
                          "date_part"(
                            year, 
                           current_timestamp()
                          ) -2
                        )
                      ):: character varying
                    ):: text || (
                      (0):: character varying
                    ):: text
                  ) || (
                    (0):: character varying
                  ):: text
                ) || (
                  (1):: character varying
                ):: text
              )
            )
          ) 
        UNION ALL 
        SELECT 
          edw_kr_sales_tgt.fisc_yr_per, 
          edw_kr_sales_tgt.sls_ofc_cd, 
          edw_kr_sales_tgt.sls_ofc_desc, 
          edw_kr_sales_tgt.channel, 
          edw_kr_sales_tgt.sls_grp_cd, 
          edw_kr_sales_tgt.ctry_cd, 
          'South Korea' :: character varying AS ctry_nm, 
          edw_kr_sales_tgt.store_type, 
          edw_kr_sales_tgt.prod_hier_l2, 
          edw_kr_sales_tgt.prod_hier_l4, 
          edw_kr_sales_tgt.target_type, 
          exch_rate.from_crncy, 
          exch_rate.to_crncy, 
          exch_rate.ex_rt, 
          0 AS amt_obj_crncy, 
          edw_kr_sales_tgt.target_amt 
        FROM 
          (
            edw_kr_sales_tgt edw_kr_sales_tgt 
            LEFT JOIN v_intrm_crncy_exch exch_rate ON (
              (
                (edw_kr_sales_tgt.crncy_cd):: text = (exch_rate.from_crncy):: text
              )
            )
          ) 
        WHERE 
          (
            (
              edw_kr_sales_tgt.crt_dttm IN (
                SELECT 
                  DISTINCT edw_kr_sales_tgt.crt_dttm 
                FROM 
                  edw_kr_sales_tgt 
                WHERE 
                  (
                    edw_kr_sales_tgt.fisc_yr = (
                      SELECT 
                        "max"(edw_kr_sales_tgt.fisc_yr) AS "max" 
                      FROM 
                        edw_kr_sales_tgt
                    )
                  )
              )
            ) 
            OR (
              edw_kr_sales_tgt.crt_dttm = (
                SELECT 
                  "max"(edw_kr_sales_tgt.crt_dttm) AS "max" 
                FROM 
                  edw_kr_sales_tgt 
                WHERE 
                  (
                    edw_kr_sales_tgt.fisc_yr = (
                      SELECT 
                        (
                          "max"(edw_kr_sales_tgt.fisc_yr) -1
                        ) 
                      FROM 
                        edw_kr_sales_tgt
                    )
                  )
              )
            )
          )
      ) main 
      LEFT JOIN (
        SELECT 
          DISTINCT edw_calendar_dim.fisc_per, 
          first_value(edw_calendar_dim.cal_day) OVER(
            PARTITION BY edw_calendar_dim.fisc_per 
            ORDER BY 
              edw_calendar_dim.cal_day ROWS BETWEEN UNBOUNDED PRECEDING 
              AND CURRENT ROW
          ) AS start_date, 
          first_value(edw_calendar_dim.cal_day) OVER(
            PARTITION BY edw_calendar_dim.fisc_per 
            ORDER BY 
              edw_calendar_dim.cal_day DESC ROWS BETWEEN UNBOUNDED PRECEDING 
              AND CURRENT ROW
          ) AS end_date 
        FROM 
          edw_calendar_dim
      ) cd ON (
        (cd.fisc_per = main.fisc_yr_per)
      )
    ) 
    LEFT JOIN (
      SELECT 
        DISTINCT edw_customer_attr_flat_dim.sls_grp_cd, 
        edw_customer_attr_flat_dim.sls_grp 
      FROM 
        edw_customer_attr_flat_dim
    ) cus ON (
      (
        (cus.sls_grp_cd):: text = (main.sls_grp):: text
      )
    )
  ) 
GROUP BY 
  main.fisc_yr_per, 
  main.sls_ofc, 
  main.sls_ofc_desc, 
  main.channel, 
  main.sls_grp, 
  cus.sls_grp, 
  main.ctry_key, 
  main.ctry_nm, 
  main.store_type, 
  main.prod_hier_lvl2, 
  main.prod_hier_lvl4, 
  main.from_crncy, 
  main.to_crncy, 
  main.ex_rt, 
  main.target_type, 
  cd.start_date, 
  cd.end_date
  )
  select * from final
  
