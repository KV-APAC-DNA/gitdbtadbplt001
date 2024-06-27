with edw_ecommerce_offtake as (
select * from {{ ref('ntaedw_integration__edw_ecommerce_offtake') }}
),
edw_product_attr_dim as (
select * from {{ ref('aspedw_integration__edw_product_attr_dim') }}
),
edw_intrm_calendar as (
select * from {{ ref('ntaedw_integration__edw_intrm_calendar') }}
),
v_intrm_reg_crncy_exch_fiscper as (
select * from {{ ref('aspedw_integration__v_intrm_reg_crncy_exch_fiscper') }}
),
edw_material_sales_dim as (
select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
edw_gch_producthierarchy as (
select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
),
edw_customer_attr_flat_dim as (
select * from {{ ref('aspedw_integration__edw_customer_attr_flat_dim') }}
),
edw_ims_fact as (
select * from {{ ref('ntaedw_integration__edw_ims_fact') }}
),
v_intrm_copa_trans as (
select * from {{ ref('ntaedw_integration__v_intrm_copa_trans') }}
),
final as (
SELECT 
  ecomm_offtake_krw.country, 
  edw_prod_attr_dim.prod_hier_l9 AS product_name, 
  ecomm_offtake_krw.retailer_code AS distributor_code, 
  ecomm_offtake_krw.retailer_name AS distributor_name, 
  ecomm_offtake_krw.sub_customer_name AS subcustomer_name, 
  ecomm_offtake_krw.ean, 
  exch_rate.ex_rt, 
  sum(ecomm_offtake_krw.quantity) AS sales_qty, 
  sum(ecomm_offtake_krw.sales_value) AS sales_value_lcy, 
  exch_rate.from_crncy, 
  exch_rate.to_crncy, 
  cal.fisc_yr AS fisc_year, 
  (
    "substring"(
      (
        (cal.fisc_per):: character varying
      ):: text, 
      6, 
      2
    )
  ):: character varying AS fisc_month, 
  to_date(
    (
      (
        "substring"(
          (
            (cal.fisc_per):: character varying
          ):: text, 
          6, 
          2
        ) || ('01' :: character varying):: text
      ) || "substring"(
        (
          (cal.fisc_per):: character varying
        ):: text, 
        1, 
        4
      )
    ), 
    ('MMDDYYYY' :: character varying):: text
  ) AS fisc_day, 
  cal.cal_yr AS cal_year, 
  cal.cal_mo_2 AS cal_month, 
  cal.cal_day, 
  cal.fisc_wk_num AS fisc_wk, 
  ecomm_offtake_krw.transaction_date AS ims_txn_dt, 
  gcph.gcph_franchise, 
  gcph.gcph_brand, 
  gcph.gcph_subbrand, 
  gcph.gcph_variant, 
  gcph.put_up_description, 
  gcph.gcph_needstate, 
  gcph.gcph_category, 
  gcph.gcph_subcategory, 
  gcph.gcph_segment, 
  gcph.gcph_subsegment, 
  gcph.matl_num AS jnj_sku_code, 
  NULL  AS sku_type, 
  edw_prod_attr_dim.prod_hier_l1, 
  edw_prod_attr_dim.prod_hier_l2, 
  edw_prod_attr_dim.prod_hier_l3, 
  edw_prod_attr_dim.prod_hier_l4, 
  edw_prod_attr_dim.prod_hier_l5, 
  edw_prod_attr_dim.prod_hier_l6, 
  edw_prod_attr_dim.prod_hier_l7, 
  edw_prod_attr_dim.prod_hier_l8, 
  edw_prod_attr_dim.prod_hier_l9, 
  ksales.store_typ AS store_type, 
  ksales.sls_grp, 
  ksales.sls_grp_cd 
FROM 
  (
    (
      (
        (
          (
            (
              SELECT 
                edw_ecommerce_offtake.country, 
                edw_ecommerce_offtake.product_title, 
                edw_ecommerce_offtake.retailer_sku_code, 
                CASE WHEN (
                  upper(
                    trim(
                      (
                        edw_ecommerce_offtake.retailer_code
                      ):: text
                    )
                  ) = ('COUPANG' :: character varying):: text
                ) THEN '135124' :: character varying WHEN (
                  upper(
                    trim(
                      (
                        edw_ecommerce_offtake.retailer_code
                      ):: text
                    )
                  ) = ('EBAY' :: character varying):: text
                ) THEN '133782' :: character varying WHEN (
                  upper(
                    trim(
                      (
                        edw_ecommerce_offtake.retailer_code
                      ):: text
                    )
                  ) = ('TREXI' :: character varying):: text
                ) THEN '135856' :: character varying ELSE edw_ecommerce_offtake.retailer_code END AS retailer_code, 
                edw_ecommerce_offtake.retailer_name, 
                edw_ecommerce_offtake.sub_customer_name, 
                edw_ecommerce_offtake.ean, 
                edw_ecommerce_offtake.transaction_currency, 
                to_date(
                  edw_ecommerce_offtake.transaction_date
                ) AS transaction_date, 
                sum(
                  edw_ecommerce_offtake.sales_value
                ) AS sales_value, 
                sum(edw_ecommerce_offtake.quantity) AS quantity 
              FROM 
                edw_ecommerce_offtake 
              WHERE 
                (
                  (
                    (
                      (
                        (
                          (
                            date_part(
                              year, 
                              edw_ecommerce_offtake.transaction_date
                            ) >= (
                              date_part(
                                year, 
                                '2020-06-12 08:31:36.359353' :: timestamp without time zone
                              ) - (3):: double precision
                            )
                          ) 
                          AND (
                            upper(
                              trim(
                                (
                                  edw_ecommerce_offtake.retailer_name
                                ):: text
                              )
                            ) <> ('AUCTION' :: character varying):: text
                          )
                        ) 
                        AND (
                          upper(
                            trim(
                              (
                                edw_ecommerce_offtake.retailer_name
                              ):: text
                            )
                          ) <> ('GMARKET' :: character varying):: text
                        )
                      ) 
                      AND (
                        upper(
                          trim(
                            (
                              edw_ecommerce_offtake.retailer_name
                            ):: text
                          )
                        ) <> ('SSG.COM' :: character varying):: text
                      )
                    ) 
                    AND (
                      upper(
                        trim(
                          (
                            edw_ecommerce_offtake.retailer_name
                          ):: text
                        )
                      ) <> ('ECVAN' :: character varying):: text
                    )
                  ) 
                  AND (
                    trim(
                      (edw_ecommerce_offtake.ean):: text
                    ) <> (
                      '8809602670046' :: character varying
                    ):: text
                  )
                ) 
              GROUP BY 
                edw_ecommerce_offtake.country, 
                edw_ecommerce_offtake.product_title, 
                edw_ecommerce_offtake.retailer_sku_code, 
                edw_ecommerce_offtake.retailer_code, 
                edw_ecommerce_offtake.retailer_name, 
                edw_ecommerce_offtake.sub_customer_name, 
                edw_ecommerce_offtake.ean, 
                edw_ecommerce_offtake.transaction_currency, 
                to_date(
                  edw_ecommerce_offtake.transaction_date
                )
            ) ecomm_offtake_krw 
            LEFT JOIN (
              SELECT 
                DISTINCT edw_product_attr_dim.aw_remote_key, 
                edw_product_attr_dim.awrefs_prod_remotekey, 
                edw_product_attr_dim.awrefs_buss_unit, 
                edw_product_attr_dim.sap_matl_num, 
                edw_product_attr_dim.cntry, 
                edw_product_attr_dim.ean, 
                edw_product_attr_dim.prod_hier_l1, 
                edw_product_attr_dim.prod_hier_l2, 
                edw_product_attr_dim.prod_hier_l3, 
                edw_product_attr_dim.prod_hier_l4, 
                edw_product_attr_dim.prod_hier_l5, 
                edw_product_attr_dim.prod_hier_l6, 
                edw_product_attr_dim.prod_hier_l7, 
                edw_product_attr_dim.prod_hier_l8, 
                edw_product_attr_dim.prod_hier_l9, 
                edw_product_attr_dim.crt_dttm, 
                edw_product_attr_dim.updt_dttm, 
                edw_product_attr_dim.lcl_prod_nm 
              FROM 
                edw_product_attr_dim 
              WHERE 
                (
                  (edw_product_attr_dim.cntry):: text = ('KR' :: character varying):: text
                )
            ) edw_prod_attr_dim ON (
              (
                (ecomm_offtake_krw.ean):: text = (edw_prod_attr_dim.ean):: text
              )
            )
          ) 
          LEFT JOIN edw_intrm_calendar cal ON (
            (
              ecomm_offtake_krw.transaction_date = cal.cal_day
            )
          )
        ) 
        LEFT JOIN (
          SELECT 
            v_intrm_crncy_exch.ex_rt_typ, 
            v_intrm_crncy_exch.from_crncy, 
            v_intrm_crncy_exch.vld_from, 
            v_intrm_crncy_exch.fisc_per, 
            v_intrm_crncy_exch.to_crncy, 
            v_intrm_crncy_exch.ex_rt 
          FROM 
            v_intrm_reg_crncy_exch_fiscper v_intrm_crncy_exch 
          WHERE 
            (
              (
                (v_intrm_crncy_exch.from_crncy):: text = ('KRW' :: character varying):: text
              ) 
              AND (
                (
                  (v_intrm_crncy_exch.to_crncy):: text = ('USD' :: character varying):: text
                ) 
                OR (
                  (v_intrm_crncy_exch.to_crncy):: text = ('KRW' :: character varying):: text
                )
              )
            )
        ) exch_rate ON (
          (
            (
              (
                ecomm_offtake_krw.transaction_currency
              ):: text = (exch_rate.from_crncy):: text
            ) 
            AND (
              cal.fisc_per = exch_rate.fisc_per
            )
          )
        )
      ) 
      LEFT JOIN (
        SELECT 
          derived_table1.ean_num, 
          derived_table1.matl_num, 
          derived_table1.med_desc, 
          derived_table1.gcph_franchise, 
          derived_table1.gcph_brand, 
          derived_table1.gcph_subbrand, 
          derived_table1.gcph_variant, 
          derived_table1.put_up_description, 
          derived_table1.gcph_needstate, 
          derived_table1.gcph_category, 
          derived_table1.gcph_subcategory, 
          derived_table1.gcph_segment, 
          derived_table1.gcph_subsegment, 
          derived_table1.row_num 
        FROM 
          (
            SELECT 
              mat.ean_num, 
              mat.matl_num, 
              mat.med_desc, 
              gph.gcph_franchise, 
              gph.gcph_brand, 
              gph.gcph_subbrand, 
              gph.gcph_variant, 
              gph.put_up_description, 
              gph.gcph_needstate, 
              gph.gcph_category, 
              gph.gcph_subcategory, 
              gph.gcph_segment, 
              gph.gcph_subsegment, 
              row_number() OVER(
                PARTITION BY mat.ean_num 
                ORDER BY 
                  gph.gcph_brand
              ) AS row_num 
            FROM 
              (
                (
                  SELECT 
                    "max"(
                      ltrim(
                        (edw_material_sales_dim.ean_num):: text, 
                        (
                          (0):: character varying
                        ):: text
                      )
                    ) AS ean_num, 
                    edw_material_sales_dim.matl_num, 
                    edw_material_sales_dim.med_desc 
                  FROM 
                    edw_material_sales_dim 
                  WHERE 
                    (
                      (
                        (
                          (
                            (edw_material_sales_dim.sls_org):: text = ('320S' :: character varying):: text
                          ) 
                          OR (
                            (edw_material_sales_dim.sls_org):: text = ('3200' :: character varying):: text
                          )
                        ) 
                        OR (
                          (edw_material_sales_dim.sls_org):: text = ('320A' :: character varying):: text
                        )
                      ) 
                      AND (
                        (edw_material_sales_dim.ean_num):: text <> ('' :: character varying):: text
                      )
                    ) 
                  GROUP BY 
                    edw_material_sales_dim.matl_num, 
                    edw_material_sales_dim.med_desc
                ) mat 
                LEFT JOIN edw_gch_producthierarchy gph ON (
                  (
                    (mat.matl_num):: text = (gph.materialnumber):: text
                  )
                )
              ) 
            WHERE 
              (gph.gcph_franchise IS NOT NULL)
          ) derived_table1 
        WHERE 
          (derived_table1.row_num = 1)
      ) gcph ON (
        (
          ltrim(
            "left"(
              (ecomm_offtake_krw.ean):: text, 
              13
            ), 
            (
              (0):: character varying
            ):: text
          ) = ltrim(
            gcph.ean_num, 
            (
              (0):: character varying
            ):: text
          )
        )
      )
    ) 
    LEFT JOIN (
      SELECT 
        DISTINCT edw_customer_attr_flat_dim.aw_remote_key, 
        edw_customer_attr_flat_dim.cntry, 
        edw_customer_attr_flat_dim.sold_to_party, 
        edw_customer_attr_flat_dim.cust_nm, 
        edw_customer_attr_flat_dim.store_typ, 
        edw_customer_attr_flat_dim.channel, 
        edw_customer_attr_flat_dim.sls_grp, 
        edw_customer_attr_flat_dim.sls_ofc, 
        edw_customer_attr_flat_dim.sls_ofc_desc, 
        edw_customer_attr_flat_dim.sls_grp_cd 
      FROM 
        edw_customer_attr_flat_dim 
      WHERE 
        (
          (
            edw_customer_attr_flat_dim.cntry
          ):: text = ('Korea' :: character varying):: text
        )
    ) ksales ON (
      (
        ltrim(
          (
            ecomm_offtake_krw.retailer_code
          ):: text, 
          ('0' :: character varying):: text
        ) = ltrim(
          (ksales.sold_to_party):: text
        )
      )
    )
  ) 
GROUP BY 
  ecomm_offtake_krw.country, 
  edw_prod_attr_dim.lcl_prod_nm, 
  ecomm_offtake_krw.retailer_sku_code, 
  ecomm_offtake_krw.retailer_code, 
  ecomm_offtake_krw.retailer_name, 
  ecomm_offtake_krw.sub_customer_name, 
  ecomm_offtake_krw.ean, 
  exch_rate.ex_rt, 
  exch_rate.from_crncy, 
  exch_rate.to_crncy, 
  cal.fisc_yr, 
  cal.fisc_per, 
  cal.cal_yr, 
  cal.cal_mo_2, 
  cal.cal_day, 
  cal.fisc_wk_num, 
  ecomm_offtake_krw.transaction_date, 
  gcph.gcph_franchise, 
  gcph.gcph_brand, 
  gcph.gcph_subbrand, 
  gcph.gcph_variant, 
  gcph.put_up_description, 
  gcph.gcph_needstate, 
  gcph.gcph_category, 
  gcph.gcph_subcategory, 
  gcph.gcph_segment, 
  gcph.gcph_subsegment, 
  gcph.matl_num, 
  edw_prod_attr_dim.prod_hier_l1, 
  edw_prod_attr_dim.prod_hier_l2, 
  edw_prod_attr_dim.prod_hier_l3, 
  edw_prod_attr_dim.prod_hier_l4, 
  edw_prod_attr_dim.prod_hier_l5, 
  edw_prod_attr_dim.prod_hier_l6, 
  edw_prod_attr_dim.prod_hier_l7, 
  edw_prod_attr_dim.prod_hier_l8, 
  edw_prod_attr_dim.prod_hier_l9, 
  ksales.store_typ, 
  ksales.sls_grp, 
  ksales.sls_grp_cd 
UNION ALL 
SELECT 
  'Korea' AS country, 
  edw_prod_attr_dim.prod_hier_l9 AS product_name, 
  ims_txn.dstr_cd AS distributor_code, 
  ims_txn.dstr_nm AS distributor_name, 
  ims_txn.cust_nm AS subcustomer_name, 
  ims_txn.ean_num AS ean, 
  exch_rate.ex_rt, 
  ims_txn.sls_qty AS sales_qty, 
  ims_txn.sls_amt AS sales_value_lcy, 
  exch_rate.from_crncy, 
  exch_rate.to_crncy, 
  cal.fisc_yr AS fisc_year, 
  (
    "substring"(
      (
        (cal.fisc_per):: character varying
      ):: text, 
      6, 
      2
    )
  ):: character varying AS fisc_month, 
  to_date(
    (
      (
        "substring"(
          (
            (cal.fisc_per):: character varying
          ):: text, 
          6, 
          2
        ) || ('01' :: character varying):: text
      ) || "substring"(
        (
          (cal.fisc_per):: character varying
        ):: text, 
        1, 
        4
      )
    ), 
    ('MMDDYYYY' :: character varying):: text
  ) AS fisc_day, 
  cal.cal_yr AS cal_year, 
  cal.cal_mo_2 AS cal_month, 
  cal.cal_day, 
  cal.fisc_wk_num AS fisc_wk, 
  ims_txn.ims_txn_dt, 
  gcph.gcph_franchise, 
  gcph.gcph_brand, 
  gcph.gcph_subbrand, 
  gcph.gcph_variant, 
  gcph.put_up_description, 
  gcph.gcph_needstate, 
  gcph.gcph_category, 
  gcph.gcph_subcategory, 
  gcph.gcph_segment, 
  gcph.gcph_subsegment, 
  ims_txn.sap_code AS jnj_sku_code, 
  ims_txn.sku_type, 
  edw_prod_attr_dim.prod_hier_l1, 
  edw_prod_attr_dim.prod_hier_l2, 
  edw_prod_attr_dim.prod_hier_l3, 
  edw_prod_attr_dim.prod_hier_l4, 
  edw_prod_attr_dim.prod_hier_l5, 
  edw_prod_attr_dim.prod_hier_l6, 
  edw_prod_attr_dim.prod_hier_l7, 
  edw_prod_attr_dim.prod_hier_l8, 
  edw_prod_attr_dim.prod_hier_l9, 
  cust_sales.store_type, 
  cust_sales.sls_grp_desc AS sls_grp, 
  cust_sales.sls_grp AS sls_grp_cd 
FROM 
  (
    (
      (
        (
          (
            (
              SELECT 
                edw_ims_fact.ims_txn_dt, 
                edw_ims_fact.dstr_cd, 
                edw_ims_fact.dstr_nm, 
                edw_ims_fact.cust_cd, 
                edw_ims_fact.cust_nm, 
                edw_ims_fact.prod_cd, 
                edw_ims_fact.prod_nm, 
                edw_ims_fact.rpt_per_strt_dt, 
                edw_ims_fact.rpt_per_end_dt, 
                edw_ims_fact.ean_num, 
                edw_ims_fact.uom, 
                edw_ims_fact.unit_prc, 
                edw_ims_fact.sls_amt, 
                edw_ims_fact.sls_qty, 
                edw_ims_fact.rtrn_qty, 
                edw_ims_fact.rtrn_amt, 
                edw_ims_fact.ship_cust_nm, 
                edw_ims_fact.cust_cls_grp, 
                edw_ims_fact.cust_sub_cls, 
                edw_ims_fact.prod_spec, 
                edw_ims_fact.itm_agn_nm, 
                edw_ims_fact.ordr_co, 
                edw_ims_fact.rtrn_rsn, 
                edw_ims_fact.sls_ofc_cd, 
                edw_ims_fact.sls_grp_cd, 
                edw_ims_fact.sls_ofc_nm, 
                edw_ims_fact.sls_grp_nm, 
                edw_ims_fact.acc_type, 
                edw_ims_fact.co_cd, 
                edw_ims_fact.sls_rep_cd, 
                edw_ims_fact.sls_rep_nm, 
                edw_ims_fact.doc_dt, 
                edw_ims_fact.doc_num, 
                edw_ims_fact.invc_num, 
                edw_ims_fact.remark_desc, 
                edw_ims_fact.gift_qty, 
                edw_ims_fact.sls_bfr_tax_amt, 
                edw_ims_fact.sku_per_box, 
                edw_ims_fact.ctry_cd, 
                edw_ims_fact.crncy_cd, 
                edw_ims_fact.crt_dttm, 
                edw_ims_fact.updt_dttm, 
                edw_ims_fact.updt_dttm, 
                edw_ims_fact.prom_sls_amt, 
                edw_ims_fact.prom_rtrn_amt, 
                edw_ims_fact.prom_prc_amt, 
                edw_ims_fact.sap_code, 
                edw_ims_fact.sku_type 
              FROM 
                edw_ims_fact 
              WHERE 
                (
                  (
                    (
                      (edw_ims_fact.ctry_cd):: text = ('KR' :: character varying):: text
                    ) 
                    AND (
                      (
                        (edw_ims_fact.dstr_cd):: text = ('129057' :: character varying):: text
                      ) 
                      OR (
                        (edw_ims_fact.dstr_cd):: text = ('135139' :: character varying):: text
                      )
                    )
                  ) 
                  AND (
                    date_part(
                      year, 
                      (edw_ims_fact.ims_txn_dt):: timestamp without time zone
                    ) >= (
                      date_part(
                        year, 
                        '2020-06-12 08:31:36.359353' :: timestamp without time zone
                      ) - (3):: double precision
                    )
                  )
                )
            ) ims_txn 
            LEFT JOIN (
              SELECT 
                DISTINCT edw_product_attr_dim.aw_remote_key, 
                edw_product_attr_dim.awrefs_prod_remotekey, 
                edw_product_attr_dim.awrefs_buss_unit, 
                edw_product_attr_dim.sap_matl_num, 
                edw_product_attr_dim.cntry, 
                edw_product_attr_dim.ean, 
                edw_product_attr_dim.prod_hier_l1, 
                edw_product_attr_dim.prod_hier_l2, 
                edw_product_attr_dim.prod_hier_l3, 
                edw_product_attr_dim.prod_hier_l4, 
                edw_product_attr_dim.prod_hier_l5, 
                edw_product_attr_dim.prod_hier_l6, 
                edw_product_attr_dim.prod_hier_l7, 
                edw_product_attr_dim.prod_hier_l8, 
                edw_product_attr_dim.prod_hier_l9, 
                edw_product_attr_dim.crt_dttm, 
                edw_product_attr_dim.updt_dttm, 
                edw_product_attr_dim.lcl_prod_nm 
              FROM 
                edw_product_attr_dim 
              WHERE 
                (
                  (edw_product_attr_dim.cntry):: text = ('KR' :: character varying):: text
                )
            ) edw_prod_attr_dim ON (
              (
                (ims_txn.ean_num):: text = (edw_prod_attr_dim.ean):: text
              )
            )
          ) 
          LEFT JOIN edw_intrm_calendar cal ON (
            (ims_txn.ims_txn_dt = cal.cal_day)
          )
        ) 
        LEFT JOIN (
          SELECT 
            v_intrm_crncy_exch.ex_rt_typ, 
            v_intrm_crncy_exch.from_crncy, 
            v_intrm_crncy_exch.vld_from, 
            v_intrm_crncy_exch.fisc_per, 
            v_intrm_crncy_exch.to_crncy, 
            v_intrm_crncy_exch.ex_rt 
          FROM 
            v_intrm_reg_crncy_exch_fiscper v_intrm_crncy_exch 
          WHERE 
            (
              (
                (v_intrm_crncy_exch.from_crncy):: text = ('KRW' :: character varying):: text
              ) 
              AND (
                (
                  (v_intrm_crncy_exch.to_crncy):: text = ('USD' :: character varying):: text
                ) 
                OR (
                  (v_intrm_crncy_exch.to_crncy):: text = ('KRW' :: character varying):: text
                )
              )
            )
        ) exch_rate ON (
          (
            cal.fisc_per = exch_rate.fisc_per
          )
        )
      ) 
      LEFT JOIN (
        SELECT 
          derived_table1.ean_num, 
          derived_table1.matl_num, 
          derived_table1.med_desc, 
          derived_table1.gcph_franchise, 
          derived_table1.gcph_brand, 
          derived_table1.gcph_subbrand, 
          derived_table1.gcph_variant, 
          derived_table1.put_up_description, 
          derived_table1.gcph_needstate, 
          derived_table1.gcph_category, 
          derived_table1.gcph_subcategory, 
          derived_table1.gcph_segment, 
          derived_table1.gcph_subsegment, 
          derived_table1.row_num 
        FROM 
          (
            SELECT 
              mat.ean_num, 
              mat.matl_num, 
              mat.med_desc, 
              gph.gcph_franchise, 
              gph.gcph_brand, 
              gph.gcph_subbrand, 
              gph.gcph_variant, 
              gph.put_up_description, 
              gph.gcph_needstate, 
              gph.gcph_category, 
              gph.gcph_subcategory, 
              gph.gcph_segment, 
              gph.gcph_subsegment, 
              row_number() OVER(
                PARTITION BY mat.ean_num 
                ORDER BY 
                  gph.gcph_brand
              ) AS row_num 
            FROM 
              (
                (
                  SELECT 
                    "max"(
                      ltrim(
                        (edw_material_sales_dim.ean_num):: text, 
                        (
                          (0):: character varying
                        ):: text
                      )
                    ) AS ean_num, 
                    edw_material_sales_dim.matl_num, 
                    edw_material_sales_dim.med_desc 
                  FROM 
                    edw_material_sales_dim 
                  WHERE 
                    (
                      (
                        (
                          (
                            (edw_material_sales_dim.sls_org):: text = ('320S' :: character varying):: text
                          ) 
                          OR (
                            (edw_material_sales_dim.sls_org):: text = ('3200' :: character varying):: text
                          )
                        ) 
                        OR (
                          (edw_material_sales_dim.sls_org):: text = ('320A' :: character varying):: text
                        )
                      ) 
                      AND (
                        (edw_material_sales_dim.ean_num):: text <> ('' :: character varying):: text
                      )
                    ) 
                  GROUP BY 
                    edw_material_sales_dim.matl_num, 
                    edw_material_sales_dim.med_desc
                ) mat 
                LEFT JOIN edw_gch_producthierarchy gph ON (
                  (
                    (mat.matl_num):: text = (gph.materialnumber):: text
                  )
                )
              ) 
            WHERE 
              (gph.gcph_franchise IS NOT NULL)
          ) derived_table1 
        WHERE 
          (derived_table1.row_num = 1)
      ) gcph ON (
        (
          ltrim(
            "left"(
              (ims_txn.ean_num):: text, 
              13
            ), 
            (
              (0):: character varying
            ):: text
          ) = ltrim(
            gcph.ean_num, 
            (
              (0):: character varying
            ):: text
          )
        )
      )
    ) 
    LEFT JOIN (
      SELECT 
        DISTINCT v_intrm_copa_trans.sls_grp, 
        v_intrm_copa_trans.sls_grp_desc, 
        v_intrm_copa_trans.store_type, 
        ltrim(
          (v_intrm_copa_trans.cust_num):: text, 
          (
            (0):: character varying
          ):: text
        ) AS cust_num 
      FROM 
        v_intrm_copa_trans 
      WHERE 
        (
          (v_intrm_copa_trans.sls_grp):: text <> ('' :: character varying):: text
        )
    ) cust_sales ON (
      (
        ltrim(
          (ims_txn.dstr_cd):: text, 
          (
            (0):: character varying
          ):: text
        ) = ltrim(
          cust_sales.cust_num, 
          (
            (0):: character varying
          ):: text
        )
      )
    )
  )
)
select * from final