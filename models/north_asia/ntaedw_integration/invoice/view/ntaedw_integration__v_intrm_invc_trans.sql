with edw_billing_fact as (
select * from {{ ref('aspedw_integration__edw_billing_fact') }}
),
itg_query_parameters as (
select * from {{ sourece('ntaitg_integration', 'itg_query_parameters') }}
),
edw_intrm_calendar as (
select * from {{ ref('ntaedw_integration__edw_intrm_calendar') }}
),
edw_company_dim as (
select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
edw_customer_base_dim as (
select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),
v_intrm_crncy_exch as (
select * from {{ ref('ntaedw_integration__v_intrm_crncy_exch') }}
),
edw_customer_attr_flat_dim as (
select * from aspedw_integration.edw_customer_attr_flat_dim
),
edw_material_sales_dim as (
select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
edw_product_attr_dim as (
select * from aspedw_integration.edw_product_attr_dim
),
edw_invoice_fact as (
select * from {{ ref('aspedw_integration__edw_invoice_fact') }}
),
edw_customer_sales_dim as (
select * from {{ ref('aspedw_integration__edw_customer_sales_dim') }}
),
edw_material_dim as (
select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
final as (
SELECT 
  a.bill_dt, 
  a.bill_type AS bill_typ, 
  a.bill_num AS bill_doc, 
  a.comp_cd AS co_cd, 
  e.company_nm, 
  a.sold_to AS cust_num, 
  a.distr_chnl AS dstr_chnl, 
  a.division AS div, 
  a.material AS matl_num, 
  a.doc_num AS sls_doc, 
  a.bill_type AS sls_doc_typ, 
  a.sls_org, 
  a.sold_to AS sold_to_prty, 
  a.bill_qty AS bill_qty_pc, 
  (
    (a.netval_inv):: numeric(18, 0)
  ):: numeric(24, 4) AS net_bill_val, 
  a.bill_dt AS cal_day, 
  a.doc_currcy AS curr_key, 
  a.doc_currcy AS doc_curr, 
  cal.fisc_per AS fisc_yr, 
  cal.fisc_wk_num, 
  current_timestamp() AS vld_from, 
  b.sls_grp, 
  h.store_typ, 
  (h.sls_grp):: character varying(40) AS sls_grp_desc, 
  b.sls_ofc, 
  b.sls_ofc_desc, 
  c.matl_desc, 
  c.mega_brnd_desc, 
  c.brnd_desc, 
  c.varnt_desc, 
  c.base_prod_desc, 
  c.put_up_desc, 
  COALESCE(
    h.channel, 'Others' :: character varying
  ) AS channel, 
  e.ctry_nm AS ctry_key, 
  e.ctry_key AS ctry_cd, 
  f.cust_nm AS edw_cust_nm, 
  g.from_crncy, 
  g.to_crncy, 
  g.ex_rt_typ, 
  g.ex_rt, 
  i.ean_num, 
  j.prod_hier_l1, 
  j.prod_hier_l2, 
  j.prod_hier_l3, 
  j.prod_hier_l4, 
  j.prod_hier_l5, 
  j.prod_hier_l6, 
  j.prod_hier_l7, 
  j.prod_hier_l8, 
  j.prod_hier_l9 
FROM 
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
                      SELECT 
                        edw_billing_fact.bill_num, 
                        edw_billing_fact.bill_item, 
                        edw_billing_fact.bill_dt, 
                        edw_billing_fact.bill_type, 
                        edw_billing_fact.sold_to, 
                        edw_billing_fact.rt_promo, 
                        edw_billing_fact.s_ord_item, 
                        edw_billing_fact.doc_num, 
                        edw_billing_fact.grs_wgt_dl, 
                        edw_billing_fact.inv_qty, 
                        edw_billing_fact.bill_qty, 
                        edw_billing_fact.base_uom, 
                        edw_billing_fact.exchg_rate, 
                        edw_billing_fact.req_qty, 
                        edw_billing_fact.sls_unit, 
                        edw_billing_fact.payer, 
                        edw_billing_fact.rebate_bas, 
                        edw_billing_fact.no_inv_it, 
                        edw_billing_fact.subtotal_1, 
                        edw_billing_fact.subtotal_3, 
                        edw_billing_fact.subtotal_4, 
                        edw_billing_fact.subtotal_2, 
                        edw_billing_fact.netval_inv, 
                        edw_billing_fact.exchg_stat, 
                        edw_billing_fact.zblqtycse, 
                        edw_billing_fact.exratexacc, 
                        edw_billing_fact.subtotal_6, 
                        edw_billing_fact.gross_val, 
                        edw_billing_fact.unit_of_wt, 
                        edw_billing_fact.subtotal_5, 
                        edw_billing_fact.numerator, 
                        edw_billing_fact.cost, 
                        edw_billing_fact.plant, 
                        edw_billing_fact.volume_dl, 
                        edw_billing_fact.loc_currcy, 
                        edw_billing_fact.denomintr, 
                        edw_billing_fact.volume_unit, 
                        edw_billing_fact.scale_qty, 
                        edw_billing_fact.cshdsc_bas, 
                        edw_billing_fact.net_wgt_dl, 
                        edw_billing_fact.tax_amt, 
                        edw_billing_fact.rate_type, 
                        edw_billing_fact.sls_org, 
                        edw_billing_fact.exrate_acc, 
                        edw_billing_fact.distr_chnl, 
                        edw_billing_fact.doc_currcy, 
                        edw_billing_fact.co_area, 
                        edw_billing_fact.doc_categ, 
                        edw_billing_fact.fisc_varnt, 
                        edw_billing_fact.cost_center, 
                        edw_billing_fact.matl_group, 
                        edw_billing_fact.division, 
                        edw_billing_fact.material, 
                        edw_billing_fact.sls_grp, 
                        edw_billing_fact.div_head, 
                        edw_billing_fact.ship_point, 
                        edw_billing_fact.wbs_elemt, 
                        edw_billing_fact.bill_rule, 
                        edw_billing_fact.bwapplnm, 
                        edw_billing_fact.process_key, 
                        edw_billing_fact.cust_grp, 
                        edw_billing_fact.sls_off, 
                        edw_billing_fact.refer_itm, 
                        edw_billing_fact.matl_grp_3, 
                        edw_billing_fact.price_dt, 
                        edw_billing_fact.sls_emply, 
                        edw_billing_fact.refer_doc, 
                        edw_billing_fact.st_up_dte, 
                        edw_billing_fact.stat_date, 
                        edw_billing_fact.item_categ, 
                        edw_billing_fact.prov_grp, 
                        edw_billing_fact.matl_grp_5, 
                        edw_billing_fact.prod_hier, 
                        edw_billing_fact.itm_type, 
                        edw_billing_fact.matl_grp_4, 
                        edw_billing_fact.ship_to, 
                        edw_billing_fact.bill_to_prty, 
                        edw_billing_fact.rebate_grp, 
                        edw_billing_fact.matl_grp_2, 
                        edw_billing_fact.matl_grp_1, 
                        edw_billing_fact.eanupc, 
                        edw_billing_fact.mat_entrd, 
                        edw_billing_fact.batch, 
                        edw_billing_fact.stor_loc, 
                        edw_billing_fact.created_on, 
                        edw_billing_fact.serv_date, 
                        edw_billing_fact.cust_grp5, 
                        edw_billing_fact.sls_deal, 
                        edw_billing_fact.bill_cat, 
                        edw_billing_fact.cust_grp1, 
                        edw_billing_fact.cust_grp3, 
                        edw_billing_fact.trans_dt, 
                        edw_billing_fact.cust_grp4, 
                        edw_billing_fact.cust_grp2, 
                        edw_billing_fact.stat_curr, 
                        edw_billing_fact.ch_on, 
                        edw_billing_fact.comp_cd, 
                        edw_billing_fact.sls_dist, 
                        edw_billing_fact.stor_no, 
                        edw_billing_fact.record_mode, 
                        edw_billing_fact.customer, 
                        edw_billing_fact.cust_sls, 
                        edw_billing_fact.oi_ebeln, 
                        edw_billing_fact.oi_ebelp, 
                        edw_billing_fact.zsd_pod, 
                        edw_billing_fact.cdl_dttm, 
                        edw_billing_fact.crtd_dttm, 
                        edw_billing_fact.updt_dttm 
                      FROM 
                        edw_billing_fact 
                      WHERE 
                        (
                          (
                            (
                              "left"(
                                (
                                  (edw_billing_fact.bill_dt):: character varying
                                ):: text, 
                                4
                              ) >= (
                                (
                                  (
                                    date_part(
                                      year, 
                                      current_timestamp()
                                    ) - (4):: double precision
                                  )
                                ):: character varying
                              ):: text
                            ) 
                            AND (
                              edw_billing_fact.sls_org IN (
                                SELECT 
                                  DISTINCT itg_query_parameters.parameter_value 
                                FROM 
                                  itg_query_parameters 
                                WHERE 
                                  (
                                    (
                                      (
                                        (
                                          itg_query_parameters.country_code
                                        ):: text = ('KR' :: character varying):: text
                                      ) 
                                      AND (
                                        (
                                          itg_query_parameters.parameter_name
                                        ):: text = (
                                          'net_invoice' :: character varying
                                        ):: text
                                      )
                                    ) 
                                    AND (
                                      (
                                        itg_query_parameters.parameter_type
                                      ):: text = ('sls_org' :: character varying):: text
                                    )
                                  )
                              )
                            )
                          ) 
                          AND (
                            edw_billing_fact.bill_type IN (
                              SELECT 
                                DISTINCT itg_query_parameters.parameter_value 
                              FROM 
                                itg_query_parameters 
                              WHERE 
                                (
                                  (
                                    (
                                      (
                                        itg_query_parameters.country_code
                                      ):: text = ('KR' :: character varying):: text
                                    ) 
                                    AND (
                                      (
                                        itg_query_parameters.parameter_name
                                      ):: text = (
                                        'net_invoice' :: character varying
                                      ):: text
                                    )
                                  ) 
                                  AND (
                                    (
                                      itg_query_parameters.parameter_type
                                    ):: text = ('bill_type' :: character varying):: text
                                  )
                                )
                            )
                          )
                        )
                    ) a 
                    LEFT JOIN edw_intrm_calendar cal ON (
                      (a.bill_dt = cal.cal_day)
                    )
                  ) 
                  LEFT JOIN edw_customer_sales_dim b ON (
                    (
                      (
                        (
                          (
                            ltrim(
                              (a.sold_to):: text, 
                              (
                                (0):: character varying
                              ):: text
                            ) = ltrim(
                              (b.cust_num):: text, 
                              (
                                (0):: character varying
                              ):: text
                            )
                          ) 
                          AND (
                            (a.distr_chnl):: text = (b.dstr_chnl):: text
                          )
                        ) 
                        AND (
                          (a.sls_org):: text = (b.sls_org):: text
                        )
                      ) 
                      AND (
                        (a.division):: text = (b.div):: text
                      )
                    )
                  )
                ) 
                LEFT JOIN edw_material_dim c ON (
                  (
                    ltrim(
                      (a.material):: text, 
                      (
                        (0):: character varying
                      ):: text
                    ) = ltrim(
                      (c.matl_num):: text, 
                      (
                        (0):: character varying
                      ):: text
                    )
                  )
                )
              ) 
              JOIN (
                SELECT 
                  edw_company_dim.co_cd, 
                  edw_company_dim.ctry_key, 
                  edw_company_dim.ctry_nm, 
                  edw_company_dim.company_nm 
                FROM 
                  edw_company_dim 
                WHERE 
                  (
                    (edw_company_dim.ctry_key):: text = ('KR' :: character varying):: text
                  )
              ) e ON (
                (
                  (a.comp_cd):: text = (e.co_cd):: text
                )
              )
            ) 
            LEFT JOIN edw_customer_base_dim f ON (
              (
                ltrim(
                  (a.customer):: text, 
                  (
                    (0):: character varying
                  ):: text
                ) = ltrim(
                  (f.cust_num):: text, 
                  (
                    (0):: character varying
                  ):: text
                )
              )
            )
          ) 
          LEFT JOIN v_intrm_crncy_exch g ON (
            (
              (a.doc_currcy):: text = (g.from_crncy):: text
            )
          )
        ) 
        LEFT JOIN (
          SELECT 
            DISTINCT edw_customer_attr_flat_dim.aw_remote_key AS sold_to_prty, 
            edw_customer_attr_flat_dim.channel, 
            edw_customer_attr_flat_dim.sls_grp, 
            edw_customer_attr_flat_dim.store_typ 
          FROM 
            edw_customer_attr_flat_dim 
          WHERE 
            (
              (
                edw_customer_attr_flat_dim.trgt_type
              ):: text = ('flat' :: character varying):: text
            )
        ) h ON (
          (
            ltrim(
              (a.sold_to):: text, 
              (
                (0):: character varying
              ):: text
            ) = ltrim(
              (h.sold_to_prty):: text, 
              (
                (0):: character varying
              ):: text
            )
          )
        )
      ) 
      LEFT JOIN edw_material_sales_dim i ON (
        (
          (
            (
              ltrim(
                (a.material):: text, 
                (
                  (0):: character varying
                ):: text
              ) = ltrim(
                (i.matl_num):: text, 
                (
                  (0):: character varying
                ):: text
              )
            ) 
            AND (
              (
                COALESCE(
                  a.sls_org, '#' :: character varying
                )
              ):: text = (
                COALESCE(
                  i.sls_org, '#' :: character varying
                )
              ):: text
            )
          ) 
          AND (
            (
              COALESCE(
                a.distr_chnl, '#' :: character varying
              )
            ):: text = (
              COALESCE(
                i.dstr_chnl, '#' :: character varying
              )
            ):: text
          )
        )
      )
    ) 
    LEFT JOIN edw_product_attr_dim j ON (
      (
        (
          (i.ean_num):: text = (j.aw_remote_key):: text
        ) 
        AND (
          (e.ctry_key):: text = (j.cntry):: text
        )
      )
    )
  ) 
WHERE 
  (
    (
      (
        (
          (
            (a.doc_currcy):: text = ('USD' :: character varying):: text
          ) 
          OR (
            (a.doc_currcy):: text = ('SGD' :: character varying):: text
          )
        ) 
        OR (
          (a.doc_currcy):: text = ('HKD' :: character varying):: text
        )
      ) 
      OR (
        (a.doc_currcy):: text = ('TWD' :: character varying):: text
      )
    ) 
    OR (
      (a.doc_currcy):: text = ('KRW' :: character varying):: text
    )
  ) 
UNION ALL 
SELECT 
  a.bill_dt, 
  a.bill_typ, 
  a.bill_doc, 
  a.co_cd, 
  e.company_nm, 
  a.cust_num, 
  a.dstr_chnl, 
  a.div, 
  a.matl_num, 
  a.sls_doc, 
  a.sls_doc_typ, 
  a.sls_org, 
  a.sold_to_prty, 
  a.bill_qty_pc, 
  a.net_bill_val, 
  a.cal_day, 
  a.curr_key, 
  a.doc_curr, 
  cal.fisc_per AS fisc_yr, 
  cal.fisc_wk_num, 
  current_timestamp() AS vld_from, 
  b.sls_grp, 
  h.store_typ, 
  (h.sls_grp):: character varying(40) AS sls_grp_desc, 
  b.sls_ofc, 
  b.sls_ofc_desc, 
  c.matl_desc, 
  c.mega_brnd_desc, 
  c.brnd_desc, 
  c.varnt_desc, 
  c.base_prod_desc, 
  c.put_up_desc, 
  COALESCE(
    h.channel, 'Others' :: character varying
  ) AS channel, 
  e.ctry_nm AS ctry_key, 
  e.ctry_key AS ctry_cd, 
  f.cust_nm AS edw_cust_nm, 
  g.from_crncy, 
  g.to_crncy, 
  g.ex_rt_typ, 
  g.ex_rt, 
  i.ean_num, 
  j.prod_hier_l1, 
  j.prod_hier_l2, 
  j.prod_hier_l3, 
  j.prod_hier_l4, 
  j.prod_hier_l5, 
  j.prod_hier_l6, 
  j.prod_hier_l7, 
  j.prod_hier_l8, 
  j.prod_hier_l9 
FROM 
  (
    (
      (
        (
          (
            (
              (
                (
                  (
                    edw_invoice_fact a 
                    LEFT JOIN edw_intrm_calendar cal ON (
                      (a.bill_dt = cal.cal_day)
                    )
                  ) 
                  LEFT JOIN edw_customer_sales_dim b ON (
                    (
                      (
                        (
                          (
                            ltrim(
                              (a.cust_num):: text, 
                              (
                                (0):: character varying
                              ):: text
                            ) = ltrim(
                              (b.cust_num):: text, 
                              (
                                (0):: character varying
                              ):: text
                            )
                          ) 
                          AND (
                            (a.dstr_chnl):: text = (b.dstr_chnl):: text
                          )
                        ) 
                        AND (
                          (a.sls_org):: text = (b.sls_org):: text
                        )
                      ) 
                      AND (
                        (a.div):: text = (b.div):: text
                      )
                    )
                  )
                ) 
                LEFT JOIN edw_material_dim c ON (
                  (
                    ltrim(
                      (a.matl_num):: text, 
                      (
                        (0):: character varying
                      ):: text
                    ) = ltrim(
                      (c.matl_num):: text, 
                      (
                        (0):: character varying
                      ):: text
                    )
                  )
                )
              ) 
              JOIN (
                SELECT 
                  edw_company_dim.co_cd, 
                  edw_company_dim.ctry_key, 
                  edw_company_dim.ctry_nm, 
                  edw_company_dim.company_nm 
                FROM 
                  edw_company_dim 
                WHERE 
                  (
                    (
                      (edw_company_dim.ctry_key):: text = ('HK' :: character varying):: text
                    ) 
                    OR (
                      (edw_company_dim.ctry_key):: text = ('TW' :: character varying):: text
                    )
                  )
              ) e ON (
                (
                  (a.co_cd):: text = (e.co_cd):: text
                )
              )
            ) 
            LEFT JOIN edw_customer_base_dim f ON (
              (
                ltrim(
                  (a.cust_num):: text, 
                  (
                    (0):: character varying
                  ):: text
                ) = ltrim(
                  (f.cust_num):: text, 
                  (
                    (0):: character varying
                  ):: text
                )
              )
            )
          ) 
          LEFT JOIN v_intrm_crncy_exch g ON (
            (
              (a.curr_key):: text = (g.from_crncy):: text
            )
          )
        ) 
        LEFT JOIN (
          SELECT 
            DISTINCT edw_customer_attr_flat_dim.aw_remote_key AS sold_to_prty, 
            edw_customer_attr_flat_dim.channel, 
            edw_customer_attr_flat_dim.sls_grp, 
            edw_customer_attr_flat_dim.store_typ 
          FROM 
            edw_customer_attr_flat_dim 
          WHERE 
            (
              (
                edw_customer_attr_flat_dim.trgt_type
              ):: text = ('flat' :: character varying):: text
            )
        ) h ON (
          (
            ltrim(
              (a.sold_to_prty):: text, 
              (
                (0):: character varying
              ):: text
            ) = ltrim(
              (h.sold_to_prty):: text, 
              (
                (0):: character varying
              ):: text
            )
          )
        )
      ) 
      LEFT JOIN edw_material_sales_dim i ON (
        (
          (
            (
              ltrim(
                (a.matl_num):: text, 
                (
                  (0):: character varying
                ):: text
              ) = ltrim(
                (i.matl_num):: text, 
                (
                  (0):: character varying
                ):: text
              )
            ) 
            AND (
              (
                COALESCE(
                  a.sls_org, '#' :: character varying
                )
              ):: text = (
                COALESCE(
                  i.sls_org, '#' :: character varying
                )
              ):: text
            )
          ) 
          AND (
            (
              COALESCE(
                a.dstr_chnl, '#' :: character varying
              )
            ):: text = (
              COALESCE(
                i.dstr_chnl, '#' :: character varying
              )
            ):: text
          )
        )
      )
    ) 
    LEFT JOIN edw_product_attr_dim j ON (
      (
        (
          (i.ean_num):: text = (j.aw_remote_key):: text
        ) 
        AND (
          (e.ctry_key):: text = (j.cntry):: text
        )
      )
    )
  ) 
WHERE 
  (
    (
      (
        (
          (
            (a.curr_key):: text = ('USD' :: character varying):: text
          ) 
          OR (
            (a.curr_key):: text = ('SGD' :: character varying):: text
          )
        ) 
        OR (
          (a.curr_key):: text = ('HKD' :: character varying):: text
        )
      ) 
      OR (
        (a.curr_key):: text = ('TWD' :: character varying):: text
      )
    ) 
    OR (
      (a.curr_key):: text = ('KRW' :: character varying):: text
    )
  )
  )
  select * from final

