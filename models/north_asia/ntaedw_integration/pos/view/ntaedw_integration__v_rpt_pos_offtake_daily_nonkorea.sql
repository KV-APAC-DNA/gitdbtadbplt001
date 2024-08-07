with edw_pos_fact as (
select * from {{ ref('ntaedw_integration__edw_pos_fact') }}
),
edw_customer_attr_flat_dim as (
select * from {{ ref('aspedw_integration__edw_customer_attr_flat_dim') }}
),
v_interm_cust_hier_dim as (
select * from {{ ref('ntaedw_integration__v_interm_cust_hier_dim') }}
),
edw_product_attr_dim as (
select * from {{ ref('aspedw_integration__edw_product_attr_dim') }}
),
v_calendar_dtls as (
select * from {{ ref('aspedw_integration__v_calendar_dtls') }}
),
v_intrm_crncy_exch as (
select * from {{ ref('ntaedw_integration__v_intrm_crncy_exch') }}
),
final as (
SELECT 
  a.pos_dt, 
  b.fisc_per, 
  b.fisc_wk, 
  b.fisc_wk_strt_dt, 
  b.fisc_wk_end_dt, 
  b.promo_per, 
  b.promo_wk, 
  b.promo_wk_strt_dt, 
  b.promo_wk_end_dt, 
  b.univ_per, 
  b.univ_wk, 
  b.univ_wk_strt_dt, 
  b.univ_wk_end_dt, 
  sum(a.sls_qty) AS sls_qty, 
  sum(a.sls_amt) AS sls_amt_customer, 
  sum(a.invnt_qty) AS invnt_qty, 
  sum(a.invnt_amt) AS invnt_amt, 
  CASE WHEN (
    (a.ctry_cd):: text = ('TW' :: character varying):: text
  ) THEN sum(a.prom_sls_amt) ELSE (
    floor(sum(a.sls_amt))
  ):: numeric(18, 0) END AS sls_amt, 
  CASE WHEN (
    (a.str_cd IS NULL) 
    OR (
      (a.str_cd):: text = ('' :: character varying):: text
    )
  ) THEN 'Not Available' :: character varying ELSE a.str_cd END AS str_cd, 
  CASE WHEN (
    (a.str_nm IS NULL) 
    OR (
      (a.str_nm):: text = ('' :: character varying):: text
    )
  ) THEN 'Not Available' :: character varying ELSE a.str_nm END AS str_nm, 
  a.crncy_cd, 
  a.src_sys_cd, 
  CASE WHEN (
    (a.ctry_cd):: text = ('TW' :: character varying):: text
  ) THEN 'Taiwan' :: character varying WHEN (
    (a.ctry_cd):: text = ('HK' :: character varying):: text
  ) THEN 'Hong Kong' :: character varying ELSE NULL :: character varying END AS ctry_nm, 
  CASE WHEN (
    (a.sls_grp IS NULL) 
    OR (
      (a.sls_grp):: text = ('' :: character varying):: text
    )
  ) THEN 'Not Available' :: character varying ELSE a.sls_grp END AS sls_grp, 
  CASE WHEN (
    (d.sls_grp_cd IS NULL) 
    OR (
      (d.sls_grp_cd):: text = ('' :: character varying):: text
    )
  ) THEN 'Not Available' :: character varying ELSE d.sls_grp_cd END AS sls_grp_cd, 
  CASE WHEN (
    (a.sold_to_party IS NULL) 
    OR (
      (a.sold_to_party):: text = ('' :: character varying):: text
    )
  ) THEN 'Not Available' :: character varying ELSE a.sold_to_party END AS sold_to_party, 
  CASE WHEN (
    (a.mysls_brnd_nm IS NULL) 
    OR (
      (a.mysls_brnd_nm):: text = ('' :: character varying):: text
    )
  ) THEN 'Not Available' :: character varying ELSE a.mysls_brnd_nm END AS my_sls_brand_nm, 
  CASE WHEN (
    (a.mysls_catg IS NULL) 
    OR (
      (a.mysls_catg):: text = ('' :: character varying):: text
    )
  ) THEN 'Not Available' :: character varying ELSE a.mysls_catg END AS mysls_catg, 
  a.vend_prod_cd, 
  CASE WHEN (
    (a.matl_num IS NULL) 
    OR (
      (a.matl_num):: text = ('' :: character varying):: text
    )
  ) THEN 'Not Available' :: character varying ELSE a.matl_num END AS matl_num, 
  CASE WHEN (
    (a.matl_desc IS NULL) 
    OR (
      (a.matl_desc):: text = ('' :: character varying):: text
    )
  ) THEN 'Not Available' :: character varying ELSE a.matl_desc END AS matl_desc, 
  a.ean_num, 
  g.to_crncy, 
  g.ex_rt_typ, 
  g.ex_rt, 
  CASE WHEN (
    (d.channel IS NULL) 
    OR (
      (d.channel):: text = ('' :: character varying):: text
    )
  ) THEN 'Not Available' :: character varying ELSE d.channel END AS channel, 
  CASE WHEN (
    (d.store_typ IS NULL) 
    OR (
      (d.store_typ):: text = ('' :: character varying):: text
    )
  ) THEN 'Not Available' :: character varying ELSE d.store_typ END AS store_typ, 
  CASE WHEN (
    (e.cust_hier_l1 IS NULL) 
    OR (
      (e.cust_hier_l1):: text = ('' :: character varying):: text
    )
  ) THEN '#' :: character varying ELSE e.cust_hier_l1 END AS cust_hier_l1, 
  CASE WHEN (
    (e.cust_hier_l2 IS NULL) 
    OR (
      (e.cust_hier_l2):: text = ('' :: character varying):: text
    )
  ) THEN '#' :: character varying ELSE e.cust_hier_l2 END AS cust_hier_l2, 
  CASE WHEN (
    (e.cust_hier_l3 IS NULL) 
    OR (
      (e.cust_hier_l3):: text = ('' :: character varying):: text
    )
  ) THEN '#' :: character varying ELSE e.cust_hier_l3 END AS cust_hier_l3, 
  CASE WHEN (
    (e.cust_hier_l4 IS NULL) 
    OR (
      (e.cust_hier_l4):: text = ('' :: character varying):: text
    )
  ) THEN '#' :: character varying ELSE e.cust_hier_l4 END AS cust_hier_l4, 
  CASE WHEN (
    (e.cust_hier_l5 IS NULL) 
    OR (
      (e.cust_hier_l5):: text = ('' :: character varying):: text
    )
  ) THEN '#' :: character varying ELSE e.cust_hier_l5 END AS cust_hier_l5, 
  CASE WHEN (
    (
      (a.ctry_cd):: text = ('TW' :: character varying):: text
    ) 
    AND (
      (f.prod_hier_l1 IS NULL) 
      OR (
        (f.prod_hier_l1):: text = ('' :: character varying):: text
      )
    )
  ) THEN 'Taiwan' :: character varying WHEN (
    (
      (a.ctry_cd):: text = ('HK' :: character varying):: text
    ) 
    AND (
      (f.prod_hier_l1 IS NULL) 
      OR (
        (f.prod_hier_l1):: text = ('' :: character varying):: text
      )
    )
  ) THEN 'HK' :: character varying ELSE COALESCE(
    f.prod_hier_l1, '#' :: character varying
  ) END AS prod_hier_l1, 
  CASE WHEN (
    (f.prod_hier_l2 IS NULL) 
    OR (
      (f.prod_hier_l2):: text = ('' :: character varying):: text
    )
  ) THEN '#' :: character varying ELSE f.prod_hier_l2 END AS prod_hier_l2, 
  CASE WHEN (
    (f.prod_hier_l3 IS NULL) 
    OR (
      (f.prod_hier_l3):: text = ('' :: character varying):: text
    )
  ) THEN '#' :: character varying ELSE f.prod_hier_l3 END AS prod_hier_l3, 
  CASE WHEN (
    (f.prod_hier_l4 IS NULL) 
    OR (
      (f.prod_hier_l4):: text = ('' :: character varying):: text
    )
  ) THEN '#' :: character varying ELSE f.prod_hier_l4 END AS prod_hier_l4, 
  CASE WHEN (
    (f.prod_hier_l5 IS NULL) 
    OR (
      (f.prod_hier_l5):: text = ('' :: character varying):: text
    )
  ) THEN '#' :: character varying ELSE f.prod_hier_l5 END AS prod_hier_l5, 
  CASE WHEN (
    (f.prod_hier_l6 IS NULL) 
    OR (
      (f.prod_hier_l6):: text = ('' :: character varying):: text
    )
  ) THEN '#' :: character varying ELSE f.prod_hier_l6 END AS prod_hier_l6, 
  CASE WHEN (
    (f.prod_hier_l7 IS NULL) 
    OR (
      (f.prod_hier_l7):: text = ('' :: character varying):: text
    )
  ) THEN '#' :: character varying ELSE f.prod_hier_l7 END AS prod_hier_l7, 
  CASE WHEN (
    (f.prod_hier_l8 IS NULL) 
    OR (
      (f.prod_hier_l8):: text = ('' :: character varying):: text
    )
  ) THEN '#' :: character varying ELSE f.prod_hier_l8 END AS prod_hier_l8, 
  CASE WHEN (
    (f.prod_hier_l9 IS NULL) 
    OR (
      (f.prod_hier_l9):: text = ('' :: character varying):: text
    )
  ) THEN '#' :: character varying ELSE f.prod_hier_l9 END AS prod_hier_l9, 
  COALESCE(
    a.prom_prc_amt, 
    (
      (0):: numeric
    ):: numeric(18, 0)
  ) AS price, 
  CASE WHEN (
    (f.lcl_prod_nm IS NULL) 
    OR (
      (f.lcl_prod_nm):: text = ('' :: character varying):: text
    )
  ) THEN '#' :: character varying ELSE f.lcl_prod_nm END AS lcl_prod_nm 
FROM 
  (
    (
      (
        (
          (
            (
              SELECT 
                edw_pos_fact.pos_dt, 
                edw_pos_fact.vend_cd, 
                edw_pos_fact.vend_nm, 
                edw_pos_fact.prod_nm, 
                edw_pos_fact.vend_prod_cd, 
                edw_pos_fact.vend_prod_nm, 
                edw_pos_fact.brnd_nm, 
                edw_pos_fact.ean_num, 
                edw_pos_fact.str_cd, 
                edw_pos_fact.str_nm, 
                edw_pos_fact.sls_qty, 
                edw_pos_fact.sls_amt, 
                edw_pos_fact.prom_sls_amt, 
                edw_pos_fact.unit_prc_amt, 
                edw_pos_fact.invnt_qty, 
                edw_pos_fact.invnt_amt, 
                edw_pos_fact.invnt_dt, 
                edw_pos_fact.crncy_cd, 
                edw_pos_fact.src_sys_cd, 
                edw_pos_fact.ctry_cd, 
                edw_pos_fact.sold_to_party, 
                edw_pos_fact.sls_grp, 
                edw_pos_fact.mysls_brnd_nm, 
                edw_pos_fact.mysls_catg, 
                edw_pos_fact.matl_num, 
                edw_pos_fact.matl_desc, 
                edw_pos_fact.prom_prc_amt 
              FROM 
                edw_pos_fact 
              WHERE 
                (
                  (
                    "date_part"(
                      year, 
                      edw_pos_fact.pos_dt
                    ) >= (
                      "date_part"(
                        year, 
                        CONVERT_TIMEZONE('UTC',current_timestamp()):: timestamp without time zone
                      ) -3
                    )
                  ) 
                  AND (
                    (edw_pos_fact.ctry_cd):: text <> ('KR' :: character varying):: text
                  )
                )
            ) a 
            LEFT JOIN (
              SELECT 
                DISTINCT edw_customer_attr_flat_dim.store_typ, 
                edw_customer_attr_flat_dim.channel, 
                edw_customer_attr_flat_dim.sold_to_party, 
                edw_customer_attr_flat_dim.cust_store_ref, 
                edw_customer_attr_flat_dim.sls_grp_cd, 
                edw_customer_attr_flat_dim.cntry 
              FROM 
                edw_customer_attr_flat_dim
            ) d ON (
              (
                (
                  (
                    (
                      COALESCE(
                        d.sold_to_party, '~' :: character varying
                      )
                    ):: text = (
                      COALESCE(
                        a.sold_to_party, '~' :: character varying
                      )
                    ):: text
                  ) 
                  AND (
                    (
                      COALESCE(
                        d.cust_store_ref, '#' :: character varying
                      )
                    ):: text = (
                      COALESCE(a.str_cd, '~' :: character varying)
                    ):: text
                  )
                ) 
                AND (
                  upper(
                    trim(
                      (d.cntry):: text
                    )
                  ) = (
                    CASE WHEN (
                      (a.ctry_cd):: text = ('HK' :: character varying):: text
                    ) THEN 'HONGKONG' :: character varying WHEN (
                      (a.ctry_cd):: text = ('TW' :: character varying):: text
                    ) THEN 'TAIWAN' :: character varying ELSE NULL :: character varying END
                  ):: text
                )
              )
            )
          ) 
          LEFT JOIN (
            SELECT 
              DISTINCT v_interm_cust_hier_dim.sold_to_party, 
              v_interm_cust_hier_dim.cust_hier_l1, 
              v_interm_cust_hier_dim.cust_hier_l2, 
              v_interm_cust_hier_dim.cust_hier_l3, 
              v_interm_cust_hier_dim.cust_hier_l4, 
              v_interm_cust_hier_dim.cust_hier_l5 
            FROM 
              v_interm_cust_hier_dim
          ) e ON (
            (
              (
                COALESCE(
                  e.sold_to_party, '~' :: character varying
                )
              ):: text = (
                COALESCE(
                  a.sold_to_party, '~' :: character varying
                )
              ):: text
            )
          )
        ) 
        LEFT JOIN (
          SELECT 
            DISTINCT (edw_product_attr_dim.ean):: character varying(100) AS ean_num, 
            edw_product_attr_dim.prod_hier_l1, 
            edw_product_attr_dim.prod_hier_l2, 
            edw_product_attr_dim.prod_hier_l3, 
            edw_product_attr_dim.prod_hier_l4, 
            edw_product_attr_dim.prod_hier_l5, 
            edw_product_attr_dim.prod_hier_l6, 
            edw_product_attr_dim.prod_hier_l7, 
            edw_product_attr_dim.prod_hier_l8, 
            edw_product_attr_dim.prod_hier_l9, 
            edw_product_attr_dim.cntry, 
            edw_product_attr_dim.lcl_prod_nm 
          FROM 
            edw_product_attr_dim edw_product_attr_dim
        ) f ON (
          (
            (
              (
                (f.ean_num):: text = (a.ean_num):: text
              ) 
              AND (
                (
                  (0):: character varying
                ):: text = (
                  (0):: character varying
                ):: text
              )
            ) 
            AND (
              (f.cntry):: text = (a.ctry_cd):: text
            )
          )
        )
      ) 
      LEFT JOIN v_calendar_dtls b ON (
        (a.pos_dt = b.cal_day)
      )
    ) 
    LEFT JOIN v_intrm_crncy_exch g ON (
      (
        (a.crncy_cd):: text = (g.from_crncy):: text
      )
    )
  ) 
GROUP BY 
  a.pos_dt, 
  b.fisc_per, 
  a.crncy_cd, 
  a.src_sys_cd, 
  a.ctry_cd, 
  d.sls_grp_cd, 
  a.sls_grp, 
  a.sold_to_party, 
  a.mysls_brnd_nm, 
  g.to_crncy, 
  g.ex_rt_typ, 
  g.ex_rt, 
  b.promo_per, 
  b.univ_per, 
  a.str_cd, 
  a.str_nm, 
  b.fisc_wk, 
  b.univ_wk, 
  b.promo_wk, 
  a.mysls_catg, 
  a.matl_num, 
  a.matl_desc, 
  a.ean_num, 
  a.vend_prod_cd, 
  d.channel, 
  d.store_typ, 
  e.cust_hier_l1, 
  e.cust_hier_l2, 
  e.cust_hier_l3, 
  e.cust_hier_l4, 
  e.cust_hier_l5, 
  f.prod_hier_l1, 
  f.prod_hier_l2, 
  f.prod_hier_l3, 
  f.prod_hier_l4, 
  f.prod_hier_l5, 
  f.prod_hier_l6, 
  f.prod_hier_l7, 
  f.prod_hier_l8, 
  f.prod_hier_l9, 
  f.lcl_prod_nm, 
  b.fisc_wk_strt_dt, 
  b.fisc_wk_end_dt, 
  b.promo_wk_strt_dt, 
  b.promo_wk_end_dt, 
  b.univ_wk_strt_dt, 
  b.univ_wk_end_dt, 
  a.prom_prc_amt
)
select * from final 