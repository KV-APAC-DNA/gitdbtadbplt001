with edw_calendar_dim as (
select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
v_intrm_invc_trans as (
select * from {{ ref('ntaedw_integration__v_intrm_invc_trans') }}
),
final as (
SELECT 
  a.sls_doc_typ, 
  a.billing_type, 
  a.cust_num AS cust_no, 
  a.fisc_yr, 
  a.fisc_mo, 
  a.cal_day, 
  a.fisc_wk_num, 
  a.sls_ofc_cv_cd, 
  a.sls_ofc_cv, 
  a.sls_grp_cv_cd, 
  a.sls_grp_cv, 
  a.sls_ofc_cy, 
  a.mega_brnd_cd, 
  a.brnd_desc, 
  a.mother_sku, 
  a.prod_hier_l2, 
  a.prod_hier_l3, 
  a.category_1, 
  a.categroy_2, 
  a.platform_ca, 
  a.core_plat, 
  a.prod_hier_l9, 
  a.key_acct_nm, 
  a.channel, 
  a.store_cd, 
  a.store_nm, 
  a.store_typ, 
  a.matl_num AS mat, 
  a.matl_desc, 
  a.ean_num, 
  trunc(
    sum(a.net_bill_val)
  ,0) AS tot_invc_val, 
  sum(a.bill_qty_pc) AS tot_invc_qty, 
  a.country, 
  a.edw_cust_nm, 
  a.currency, 
  a.from_crncy, 
  a.to_crncy, 
  a.ex_rt_typ, 
  a.ex_rt, 
  a.country_cd, 
  a.company_nm, 
  (
    SELECT 
      edw_calendar_dim.fisc_per 
    FROM 
      edw_calendar_dim 
    WHERE 
      (
        edw_calendar_dim.cal_day = '2018-11-29' :: date
      )
  ) AS current_fisc_per 
FROM 
  (
    SELECT 
      edw_invc_trans_fact.ean_num, 
      edw_invc_trans_fact.sls_doc_typ, 
      edw_invc_trans_fact.bill_typ AS billing_type, 
      edw_invc_trans_fact.cust_num, 
      "substring"(
        (
          (edw_invc_trans_fact.fisc_yr):: character varying
        ):: text, 
        1, 
        4
      ) AS fisc_yr, 
      "substring"(
        (
          (edw_invc_trans_fact.fisc_yr):: character varying
        ):: text, 
        6, 
        8
      ) AS fisc_mo, 
      edw_invc_trans_fact.cal_day, 
      edw_invc_trans_fact.fisc_wk_num, 
      COALESCE(
        edw_invc_trans_fact.sls_ofc, 'Not Available' :: character varying
      ) AS sls_ofc_cv_cd, 
      COALESCE(
        edw_invc_trans_fact.sls_ofc_desc, 
        'Not Available' :: character varying
      ) AS sls_ofc_cv, 
      COALESCE(
        edw_invc_trans_fact.sls_grp, 'Not \012Available' :: character varying
      ) AS sls_grp_cv_cd, 
      COALESCE(
        edw_invc_trans_fact.sls_grp_desc, 
        'Not Available' :: character varying
      ) AS sls_grp_cv, 
      edw_invc_trans_fact.ctry_key AS sls_ofc_cy, 
      COALESCE(
        edw_invc_trans_fact.prod_hier_l1, 
        'Not Available' :: character varying
      ) AS mother_sku, 
      COALESCE(
        edw_invc_trans_fact.prod_hier_l2, 
        'Not Available' :: character varying
      ) AS prod_hier_l2, 
      COALESCE(
        edw_invc_trans_fact.prod_hier_l3, 
        'Not Available' :: character varying
      ) AS prod_hier_l3, 
      COALESCE(
        edw_invc_trans_fact.prod_hier_l4, 
        'Not Available' :: character varying
      ) AS mega_brnd_cd, 
      COALESCE(
        edw_invc_trans_fact.prod_hier_l4, 
        'Not Available' :: character varying
      ) AS brnd_desc, 
      COALESCE(
        edw_invc_trans_fact.prod_hier_l5, 
        'Not Available' :: character varying
      ) AS category_1, 
      COALESCE(
        edw_invc_trans_fact.prod_hier_l6, 
        'Not Available' :: character varying
      ) AS categroy_2, 
      COALESCE(
        edw_invc_trans_fact.prod_hier_l7, 
        'Not Available' :: character varying
      ) AS platform_ca, 
      COALESCE(
        edw_invc_trans_fact.prod_hier_l8, 
        'Not Available' :: character varying
      ) AS core_plat, 
      COALESCE(
        edw_invc_trans_fact.prod_hier_l9, 
        'Not Available' :: character varying
      ) AS prod_hier_l9, 
      COALESCE(
        (
          (
            (edw_invc_trans_fact.sls_grp):: text || ('-' :: character varying):: text
          ) || (
            edw_invc_trans_fact.sls_grp_desc
          ):: text
        ), 
        (
          'Not \012Available' :: character varying
        ):: text
      ) AS key_acct_nm, 
      edw_invc_trans_fact.channel, 
      COALESCE(
        edw_invc_trans_fact.cust_num, 'Not \012Available' :: character varying
      ) AS store_cd, 
      COALESCE(
        edw_invc_trans_fact.edw_cust_nm, 
        'Not Available' :: character varying
      ) AS store_nm, 
      COALESCE(
        edw_invc_trans_fact.store_typ, 'Not Available' :: character varying
      ) AS store_typ, 
      ltrim(
        (edw_invc_trans_fact.matl_num):: text, 
        (
          (0):: character varying
        ):: text
      ) AS matl_num, 
      COALESCE(
        edw_invc_trans_fact.matl_desc, 'Not Available' :: character varying
      ) AS matl_desc, 
      edw_invc_trans_fact.net_bill_val, 
      edw_invc_trans_fact.bill_qty_pc, 
      edw_invc_trans_fact.ctry_key AS country, 
      COALESCE(
        edw_invc_trans_fact.edw_cust_nm, 
        'Not \012Available' :: character varying
      ) AS edw_cust_nm, 
      edw_invc_trans_fact.curr_key AS currency, 
      edw_invc_trans_fact.from_crncy, 
      edw_invc_trans_fact.to_crncy, 
      edw_invc_trans_fact.ex_rt_typ, 
      edw_invc_trans_fact.ex_rt, 
      edw_invc_trans_fact.ctry_cd AS country_cd, 
      edw_invc_trans_fact.company_nm 
    FROM 
      v_intrm_invc_trans edw_invc_trans_fact 
    WHERE 
      (
        (
          (edw_invc_trans_fact.fisc_yr):: character varying
        ):: text >= (
          (
            (
              (
                (
                  (
                    "date_part"(
                      year, 
                      '2018-11-29 13:34:12.7334' :: timestamp without time zone
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
  ) a 
GROUP BY 
  a.fisc_yr, 
  a.fisc_mo, 
  a.cal_day, 
  a.fisc_wk_num, 
  a.country, 
  a.cust_num, 
  a.channel, 
  a.matl_num, 
  a.matl_desc, 
  a.ean_num, 
  a.mega_brnd_cd, 
  a.brnd_desc, 
  a.sls_grp_cv_cd, 
  a.sls_grp_cv, 
  a.sls_ofc_cv_cd, 
  a.sls_ofc_cv, 
  a.sls_doc_typ, 
  a.billing_type, 
  a.edw_cust_nm, 
  a.currency, 
  a.from_crncy, 
  a.to_crncy, 
  a.ex_rt_typ, 
  a.ex_rt, 
  a.country_cd, 
  a.company_nm, 
  a.sls_ofc_cy, 
  a.mother_sku, 
  a.prod_hier_l2, 
  a.prod_hier_l3, 
  a.category_1, 
  a.categroy_2, 
  a.platform_ca, 
  a.core_plat, 
  a.prod_hier_l9, 
  a.key_acct_nm, 
  a.store_cd, 
  a.store_nm, 
  a.store_typ
)
select * from final
