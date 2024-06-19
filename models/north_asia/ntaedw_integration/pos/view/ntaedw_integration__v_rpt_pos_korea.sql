with edw_pos_fact as (
select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.EDW_POS_FACT
),
v_intrm_calendar as (
select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.V_INTRM_CALENDAR
),
edw_customer_attr_flat_dim as (
select * from DEV_DNA_CORE.SNAPASPEDW_INTEGRATION.EDW_CUSTOMER_ATTR_FLAT_DIM
),
v_interm_cust_hier_dim as (
select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.V_INTERM_CUST_HIER_DIM
),
edw_product_attr_dim as (
select * from DEV_DNA_CORE.SNAPASPEDW_INTEGRATION.EDW_PRODUCT_ATTR_DIM
),
v_intrm_crncy_exch as (
select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.V_INTRM_CRNCY_EXCH
),

a as (
              SELECT 
                edw_pos_fact.pos_dt, 
                edw_pos_fact.brnd_nm, 
                edw_pos_fact.ean_num, 
                edw_pos_fact.str_cd, 
                edw_pos_fact.src_sys_cd, 
                edw_pos_fact.str_nm, 
                edw_pos_fact.sls_qty, 
                edw_pos_fact.sls_amt, 
                edw_pos_fact.unit_prc_amt, 
                edw_pos_fact.invnt_qty, 
                CASE WHEN (
                  (edw_pos_fact.hist_flg):: text = ('Y' :: character varying):: text
                ) THEN edw_pos_fact.invnt_amt ELSE (
                  (
                    (edw_pos_fact.invnt_qty):: numeric
                  ):: numeric(18, 0) * edw_pos_fact.prom_prc_amt
                ) END AS invnt_amt, 
                edw_pos_fact.crncy_cd, 
                edw_pos_fact.ctry_cd, 
                edw_pos_fact.sold_to_party, 
                edw_pos_fact.sls_grp, 
                edw_pos_fact.sls_grp_cd, 
                edw_pos_fact.channel, 
                edw_pos_fact.store_type, 
                edw_pos_fact.mysls_brnd_nm, 
                edw_pos_fact.prom_sls_amt, 
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
                        convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)
                      ) -6
                    )
                  ) 
                  AND (
                    (edw_pos_fact.ctry_cd):: text = ('KR' :: character varying):: text
                  )
                )
            ),
i as  (
            SELECT 
              DISTINCT edw_customer_attr_flat_dim.store_typ, 
              edw_customer_attr_flat_dim.channel, 
              edw_customer_attr_flat_dim.sold_to_party, 
              edw_customer_attr_flat_dim.sls_grp_cd, 
              edw_customer_attr_flat_dim.cntry 
            FROM 
              edw_customer_attr_flat_dim 
            WHERE 
              (
                (
                  (
                    edw_customer_attr_flat_dim.sold_to_party IS NOT NULL
                  ) 
                  AND (
                    (
                      edw_customer_attr_flat_dim.sold_to_party
                    ):: text <> ('' :: character varying):: text
                  )
                ) 
                AND (
                  upper(
                    (
                      edw_customer_attr_flat_dim.cntry
                    ):: text
                  ) like ('%KOREA%' :: character varying):: text
                )
              )
          ),
e as (
          SELECT 
            DISTINCT edw_customer_attr_hier_dim.sold_to_party, 
            edw_customer_attr_hier_dim.cust_hier_l1, 
            edw_customer_attr_hier_dim.cust_hier_l2, 
            edw_customer_attr_hier_dim.cust_hier_l3, 
            edw_customer_attr_hier_dim.cust_hier_l4, 
            edw_customer_attr_hier_dim.cust_hier_l5 
          FROM 
            v_interm_cust_hier_dim edw_customer_attr_hier_dim
        ),
final as 
(SELECT 
  b.fisc_per, 
  b.cal_wk AS fisc_wk, 
  b.fisc_wk_num, 
  b.max_wk_flg, 
  CASE WHEN (
    (
      (
        "substring"(
          (
            (b.fisc_per):: character varying
          ):: text, 
          1, 
          4
        )
      ):: integer = "date_part"(
        year, 
        convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)
      )
    ) 
    AND (
      (
        "substring"(
          (
            (b.fisc_per):: character varying
          ):: text, 
          6, 
          2
        )
      ):: integer = "date_part"(
        month, 
        convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)
      )
    )
  ) THEN 'Y' :: character varying ELSE 'N' :: character varying END AS curr_month_ind, 
  sum(a.sls_qty) AS sls_qty, 
  CASE WHEN (
    (a.sls_grp):: text = ('Costco' :: character varying):: text
  ) THEN sum(a.prom_sls_amt) ELSE sum(a.sls_amt) END AS sls_amt_customer, 
  sum(a.invnt_qty) AS invnt_qty, 
  sum(a.invnt_amt) AS invnt_amt, 
  sum(a.prom_sls_amt) AS sls_amt, 
  a.crncy_cd, 
  a.src_sys_cd, 
  CASE WHEN (
    (a.ctry_cd):: text = ('KR' :: character varying):: text
  ) THEN 'South Korea' :: character varying ELSE NULL :: character varying END AS ctry_nm, 
  CASE WHEN (
    (a.sls_grp IS NULL) 
    OR (
      (a.sls_grp):: text = ('' :: character varying):: text
    )
  ) THEN 'Not Available' :: character varying ELSE a.sls_grp END AS sls_grp, 
  CASE WHEN (
    (a.sls_grp_cd IS NULL) 
    OR (
      (a.sls_grp_cd):: text = ('' :: character varying):: text
    )
  ) THEN i.sls_grp_cd ELSE a.sls_grp_cd END AS sls_grp_cd, 
  CASE WHEN (
    (a.sold_to_party IS NULL) 
    OR (
      (a.sold_to_party):: text = ('' :: character varying):: text
    )
  ) THEN 'Not Available' :: character varying ELSE a.sold_to_party END AS sold_to_party, 
  CASE WHEN (
    (
      (
        (i.store_typ IS NULL) 
        OR (
          (i.store_typ):: text = ('' :: character varying):: text
        )
      ) 
      OR (i.sls_grp_cd IS NULL)
    ) 
    OR (
      (i.sls_grp_cd):: text = ('' :: character varying):: text
    )
  ) THEN a.store_type ELSE i.store_typ END AS store_typ, 
  CASE WHEN (
    (a.mysls_brnd_nm IS NULL) 
    OR (
      (a.mysls_brnd_nm):: text = ('' :: character varying):: text
    )
  ) THEN 'Not Available' :: character varying ELSE a.mysls_brnd_nm END AS my_sls_brand_nm, 
  CASE WHEN (
    (
      (
        (i.channel IS NULL) 
        OR (
          (i.channel):: text = ('' :: character varying):: text
        )
      ) 
      OR (i.sls_grp_cd IS NULL)
    ) 
    OR (
      (i.sls_grp_cd):: text = ('' :: character varying):: text
    )
  ) THEN a.channel ELSE i.channel END AS channel, 
  g.to_crncy, 
  g.ex_rt_typ, 
  g.ex_rt, 
  a.ean_num, 
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
      (a.ctry_cd):: text = ('KR' :: character varying):: text
    ) 
    AND (
      (f.prod_hier_l1 IS NULL) 
      OR (
        (f.prod_hier_l1):: text = ('' :: character varying):: text
      )
    )
  ) THEN 'Korea' :: character varying ELSE COALESCE(
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
            (select * from a ) a
            LEFT JOIN v_intrm_calendar b ON (
              (b.cal_day = a.pos_dt)
            )
          ) 
          LEFT JOIN i ON (
            (
              (
                COALESCE(
                  i.sold_to_party, '~' :: character varying
                )
              ):: text = (
                COALESCE(
                  a.sold_to_party, '~' :: character varying
                )
              ):: text
            )
          )
        ) 
        LEFT JOIN  e ON (
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
          derived_table1.ean_num, 
          derived_table1.prod_hier_l1, 
          derived_table1.prod_hier_l2, 
          derived_table1.prod_hier_l3, 
          derived_table1.prod_hier_l4, 
          derived_table1.prod_hier_l5, 
          derived_table1.prod_hier_l6, 
          derived_table1.prod_hier_l7, 
          derived_table1.prod_hier_l8, 
          derived_table1.prod_hier_l9, 
          derived_table1.cntry, 
          derived_table1.lcl_prod_nm, 
          derived_table1.rnum 
        FROM 
          (
            SELECT 
              trim(
                ltrim(
                  (
                    (edw_product_attr_dim.ean):: character varying(100)
                  ):: text, 
                  (
                    (0):: character varying
                  ):: text
                )
              ) AS ean_num, 
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
              edw_product_attr_dim.lcl_prod_nm, 
              row_number() OVER(
                PARTITION BY trim(
                  ltrim(
                    (edw_product_attr_dim.ean):: text, 
                    (
                      (0):: character varying
                    ):: text
                  )
                ), 
                edw_product_attr_dim.cntry 
                ORDER BY 
                  edw_product_attr_dim.lcl_prod_nm, 
                  edw_product_attr_dim.prod_hier_l1, 
                  edw_product_attr_dim.prod_hier_l2, 
                  edw_product_attr_dim.prod_hier_l3, 
                  edw_product_attr_dim.prod_hier_l4, 
                  edw_product_attr_dim.prod_hier_l5, 
                  edw_product_attr_dim.prod_hier_l6, 
                  edw_product_attr_dim.prod_hier_l7, 
                  edw_product_attr_dim.prod_hier_l8, 
                  edw_product_attr_dim.prod_hier_l9
              ) AS rnum 
            FROM 
              edw_product_attr_dim edw_product_attr_dim
          ) derived_table1 
        WHERE 
          (derived_table1.rnum = 1)
      ) f ON (
        (
          (
            ltrim(
              (
                (f.ean_num):: character varying
              ):: text, 
              (
                (0):: character varying
              ):: text
            ) = ltrim(
              (a.ean_num):: text, 
              (
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
    LEFT JOIN v_intrm_crncy_exch g ON (
      (
        (a.crncy_cd):: text = (g.from_crncy):: text
      )
    )
  ) 
GROUP BY 
  b.fisc_per, 
  b.cal_wk, 
  b.fisc_wk_num, 
  a.crncy_cd, 
  a.src_sys_cd, 
  a.ctry_cd, 
  a.sls_grp, 
  i.sls_grp_cd, 
  a.sold_to_party, 
  a.mysls_brnd_nm, 
  a.ean_num, 
  a.str_cd, 
  a.str_nm, 
  g.to_crncy, 
  g.ex_rt_typ, 
  g.ex_rt, 
  i.store_typ, 
  b.max_wk_flg, 
  i.channel, 
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
  a.channel, 
  a.store_type, 
  a.sls_grp_cd
)
select * from final