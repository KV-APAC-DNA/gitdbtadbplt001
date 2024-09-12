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
a as (
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
                edw_pos_fact.unit_prc_amt,
                edw_pos_fact.invnt_qty,
                CASE WHEN (
                  (edw_pos_fact.hist_flg):: text = ('Y' :: character varying):: text
                ) THEN edw_pos_fact.invnt_amt ELSE (
                  (
                    (edw_pos_fact.invnt_qty):: numeric
                  ):: numeric(18, 0) * edw_pos_fact.prom_prc_amt
                ) END AS invnt_amt,
                edw_pos_fact.invnt_dt,
                edw_pos_fact.crncy_cd,
                edw_pos_fact.src_sys_cd,
                edw_pos_fact.ctry_cd,
                edw_pos_fact.sold_to_party,
                edw_pos_fact.sls_grp,
                edw_pos_fact.sls_grp_cd,
                edw_pos_fact.channel,
                edw_pos_fact.store_type,
                edw_pos_fact.mysls_brnd_nm,
                edw_pos_fact.mysls_catg,
                edw_pos_fact.matl_num,
                edw_pos_fact.matl_desc,
                edw_pos_fact.prom_prc_amt,
                edw_pos_fact.prom_sls_amt
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
                        current_timestamp():: timestamp without time zone
                      ) -5
                    )
                  )
                  AND (
                    (edw_pos_fact.ctry_cd):: text = ('KR' :: character varying):: text
                  )
                )
            ),
final as (
SELECT
  b.fisc_per,
  b.fisc_wk,
  b.fisc_wk_strt_dt,
  b.fisc_wk_end_dt,
  b.univ_per,
  b.univ_wk,
  b.univ_wk_strt_dt,
  b.univ_wk_end_dt,
  b.univ_week_month,
  b.univ_wk_month_strt_dt,
  b.univ_wk_month_end_dt,
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
  rtrim(a.ean_num) as ean_num,
  CASE WHEN (
    (rtrim(a.str_cd) IS NULL)
    OR (
      rtrim(a.str_cd):: text = ('' :: character varying):: text
    )
  ) THEN 'Not Available' :: character varying ELSE rtrim(a.str_cd) END AS str_cd,
  CASE WHEN (
    (a.str_nm IS NULL)
    OR (
      (a.str_nm):: text = ('' :: character varying):: text
    )
  ) THEN 'Not Available' :: character varying ELSE a.str_nm END AS str_nm,
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
  g.to_crncy,
  g.ex_rt_typ,
  g.ex_rt,
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
  COALESCE(
    a.prom_prc_amt,
    (
      (
        (0):: numeric
      ):: numeric(18, 0)
    ):: numeric(16, 5)
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
             (select * from a)  a
            LEFT JOIN (
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
            ) i ON (
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
              (
                rtrim(f.ean_num):: character varying
              ):: text = rtrim(a.ean_num):: text
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
  b.fisc_per,
  b.fisc_wk,
  b.fisc_wk_strt_dt,
  b.fisc_wk_end_dt,
  b.univ_per,
  b.univ_wk,
  b.univ_wk_strt_dt,
  b.univ_wk_end_dt,
  b.univ_week_month,
  b.univ_wk_month_strt_dt,
  b.univ_wk_month_end_dt,
  a.crncy_cd,
  a.src_sys_cd,
  a.ctry_cd,
  a.sls_grp,
  i.sls_grp_cd,
  a.sold_to_party,
  a.mysls_brnd_nm,
  g.to_crncy,
  g.ex_rt_typ,
  g.ex_rt,
  i.store_typ,
  a.mysls_catg,
  rtrim(a.ean_num),
  rtrim(a.str_cd),
  a.str_nm,
  a.matl_num,
  a.matl_desc,
  a.vend_prod_cd,
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
  a.prom_prc_amt,
  a.channel,
  a.store_type,
  a.sls_grp_cd
)
select * from final
