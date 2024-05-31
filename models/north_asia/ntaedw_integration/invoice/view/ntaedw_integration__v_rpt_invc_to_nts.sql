with v_intrm_copa_trans as (
select * from {{ ref('ntaedw_integration__v_intrm_copa_trans') }}
),
edw_acct_ciw_hier as (
select * from {{ ref('aspedw_integration__edw_acct_ciw_hier') }}
),
final as (
SELECT 
  a.fisc_yr_per, 
  to_date(
    (
      (
        "substring"(
          (
            (a.fisc_yr_per):: character varying
          ):: text, 
          6, 
          8
        ) || ('01' :: character varying):: text
      ) || "substring"(
        (
          (a.fisc_yr_per):: character varying
        ):: text, 
        1, 
        4
      )
    ), 
    ('MMDDYYYY' :: character varying):: text
  ) AS fisc_day, 
  a.sls_ofc, 
  a.sls_ofc_desc, 
  a.channel, 
  a.cust_sls_grp AS sls_grp, 
  a.sls_grp_desc, 
  a.mega_brnd_desc, 
  a.matl_num, 
  COALESCE(
    a.prod_hier_lvl4, 'Not Available' :: character varying
  ) AS brnd_desc, 
  COALESCE(
    a.prod_hier_lvl3, 'Not Available' :: character varying
  ) AS category, 
  COALESCE(
    a.prod_hier_lvl9, 'Not Available' :: character varying
  ) AS matl_desc, 
  a.cust_num, 
  a.ctry_key, 
  a.ctry_nm, 
  a.store_type, 
  COALESCE(
    a.prod_hier_lvl1, 'Not Available' :: character varying
  ) AS prod_hier_lvl1, 
  COALESCE(
    a.prod_hier_lvl2, 'Not Available' :: character varying
  ) AS prod_hier_lvl2, 
  COALESCE(
    a.prod_hier_lvl3, 'Not Available' :: character varying
  ) AS prod_hier_lvl3, 
  COALESCE(
    a.prod_hier_lvl4, 'Not Available' :: character varying
  ) AS prod_hier_lvl4, 
  COALESCE(
    a.prod_hier_lvl5, 'Not Available' :: character varying
  ) AS prod_hier_lvl5, 
  COALESCE(
    a.prod_hier_lvl6, 'Not Available' :: character varying
  ) AS prod_hier_lvl6, 
  COALESCE(
    a.prod_hier_lvl7, 'Not Available' :: character varying
  ) AS prod_hier_lvl7, 
  COALESCE(
    a.prod_hier_lvl8, 'Not Available' :: character varying
  ) AS prod_hier_lvl8, 
  COALESCE(
    a.prod_hier_lvl9, 'Not Available' :: character varying
  ) AS prod_hier_lvl9, 
  a.ean_num, 
  b.ciw_desc, 
  b.ciw_code, 
  b.ciw_bucket, 
  b.acct_num, 
  b.acct_nm, 
  a.from_crncy, 
  a.to_crncy, 
  sum(a.amt_obj_crncy) AS amt_obj_crncy, 
  a.ex_rt, 
  a.edw_cust_nm 
FROM 
  (
    (
      SELECT 
        v_intrm_copa_trans.co_cd, 
        v_intrm_copa_trans.cntl_area, 
        v_intrm_copa_trans.prft_ctr, 
        v_intrm_copa_trans.sls_org, 
        v_intrm_copa_trans.matl_num, 
        v_intrm_copa_trans.cust_num, 
        v_intrm_copa_trans.div, 
        v_intrm_copa_trans.plnt, 
        v_intrm_copa_trans.chrt_acct, 
        v_intrm_copa_trans.acct_num, 
        v_intrm_copa_trans.dstr_chnl, 
        v_intrm_copa_trans.fisc_yr_var, 
        v_intrm_copa_trans.vers, 
        v_intrm_copa_trans.bw_delta_upd_mode, 
        v_intrm_copa_trans.bill_typ, 
        v_intrm_copa_trans.sls_ofc, 
        v_intrm_copa_trans.ctry_nm, 
        v_intrm_copa_trans.ctry_key, 
        v_intrm_copa_trans.sls_deal, 
        v_intrm_copa_trans.sls_grp, 
        v_intrm_copa_trans.sls_emp_hist, 
        v_intrm_copa_trans.sls_dist, 
        v_intrm_copa_trans.cust_grp, 
        v_intrm_copa_trans.cust_sls, 
        v_intrm_copa_trans.buss_area, 
        v_intrm_copa_trans.val_type_rpt, 
        v_intrm_copa_trans.mercia_ref, 
        v_intrm_copa_trans.caln_day, 
        v_intrm_copa_trans.caln_yr_mo, 
        v_intrm_copa_trans.fisc_yr, 
        v_intrm_copa_trans.pstng_per, 
        v_intrm_copa_trans.fisc_yr_per, 
        v_intrm_copa_trans.b3_base_prod, 
        v_intrm_copa_trans.b4_var, 
        v_intrm_copa_trans.b5_put_up, 
        v_intrm_copa_trans.b1_mega_brnd, 
        v_intrm_copa_trans.b2_brnd, 
        v_intrm_copa_trans.reg, 
        v_intrm_copa_trans.prod_minor, 
        v_intrm_copa_trans.prod_maj, 
        v_intrm_copa_trans.prod_fran, 
        v_intrm_copa_trans.fran, 
        v_intrm_copa_trans.gran_grp, 
        v_intrm_copa_trans.oper_grp, 
        v_intrm_copa_trans.sls_prsn_resp, 
        v_intrm_copa_trans.matl_sls, 
        v_intrm_copa_trans.prod_hier, 
        v_intrm_copa_trans.mgmt_entity, 
        v_intrm_copa_trans.fx_amt_cntl_area_crncy, 
        v_intrm_copa_trans.amt_cntl_area_crncy, 
        v_intrm_copa_trans.crncy_key, 
        v_intrm_copa_trans.amt_obj_crncy, 
        v_intrm_copa_trans.obj_crncy_co_obj, 
        v_intrm_copa_trans.grs_amt_trans_crncy, 
        v_intrm_copa_trans.crncy_key_trans_crncy, 
        v_intrm_copa_trans.qty, 
        v_intrm_copa_trans.uom, 
        v_intrm_copa_trans.sls_vol, 
        v_intrm_copa_trans.un_sls_vol, 
        v_intrm_copa_trans.vld_from, 
        v_intrm_copa_trans.cust_sls_grp, 
        v_intrm_copa_trans.sls_grp_desc, 
        v_intrm_copa_trans.cust_sls_ofc, 
        v_intrm_copa_trans.sls_ofc_desc, 
        v_intrm_copa_trans.matl_desc, 
        v_intrm_copa_trans.mega_brnd_desc, 
        v_intrm_copa_trans.brnd_desc, 
        v_intrm_copa_trans.varnt_desc, 
        v_intrm_copa_trans.base_prod_desc, 
        v_intrm_copa_trans.put_up_desc, 
        v_intrm_copa_trans.channel, 
        v_intrm_copa_trans.med_desc, 
        v_intrm_copa_trans.edw_cust_nm, 
        v_intrm_copa_trans.from_crncy, 
        v_intrm_copa_trans.to_crncy, 
        v_intrm_copa_trans.ex_rt_typ, 
        v_intrm_copa_trans.ex_rt, 
        v_intrm_copa_trans.acct_hier_desc, 
        v_intrm_copa_trans.acct_hier_shrt_desc, 
        v_intrm_copa_trans.company_nm, 
        v_intrm_copa_trans.ean_num, 
        v_intrm_copa_trans.prod_hier_lvl1, 
        v_intrm_copa_trans.prod_hier_lvl2, 
        v_intrm_copa_trans.prod_hier_lvl3, 
        v_intrm_copa_trans.prod_hier_lvl4, 
        v_intrm_copa_trans.prod_hier_lvl5, 
        v_intrm_copa_trans.prod_hier_lvl6, 
        v_intrm_copa_trans.prod_hier_lvl7, 
        v_intrm_copa_trans.prod_hier_lvl8, 
        v_intrm_copa_trans.prod_hier_lvl9, 
        v_intrm_copa_trans.store_type 
      FROM 
        v_intrm_copa_trans 
      WHERE 
        (
          (
            (
              v_intrm_copa_trans.acct_hier_shrt_desc
            ):: text = ('NTS' :: character varying):: text
          ) 
          AND (
            (
              (v_intrm_copa_trans.fisc_yr_per):: character varying
            ):: text >= (
              (
                (
                  (
                    (
                      (
                        date_part(
                          year, 
                          current_timestamp():: timestamp without time zone
                        ) - (5):: double precision
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
    ) a 
    JOIN (
      SELECT 
        edw_acct_ciw_hier.chrt_acct, 
        edw_acct_ciw_hier.acct_num, 
        edw_acct_ciw_hier.acct_nm, 
        edw_acct_ciw_hier.ciw_desc, 
        edw_acct_ciw_hier.ciw_code, 
        edw_acct_ciw_hier.ciw_bucket, 
        edw_acct_ciw_hier.ciw_acct_col, 
        edw_acct_ciw_hier.ciw_acct_no, 
        edw_acct_ciw_hier.ciw_acct_nm, 
        edw_acct_ciw_hier.measure_code, 
        edw_acct_ciw_hier.measure_name, 
        edw_acct_ciw_hier.multiplication_factor 
      FROM 
        edw_acct_ciw_hier 
      WHERE 
        (
          (edw_acct_ciw_hier.measure_code):: text = ('NTS' :: character varying):: text
        )
    ) b ON (
      (
        ltrim(
          (a.acct_num):: text, 
          ('0' :: character varying):: text
        ) = ltrim(
          (b.acct_num):: text, 
          ('0' :: character varying):: text
        )
      )
    )
  ) 
GROUP BY 
  a.fisc_yr_per, 
  a.sls_ofc, 
  a.sls_ofc_desc, 
  a.channel, 
  a.cust_sls_grp, 
  a.sls_grp_desc, 
  a.mega_brnd_desc, 
  a.matl_num, 
  a.prod_hier_lvl4, 
  a.prod_hier_lvl3, 
  a.prod_hier_lvl9, 
  a.cust_num, 
  a.ctry_key, 
  a.ctry_nm, 
  a.store_type, 
  a.prod_hier_lvl1, 
  a.prod_hier_lvl2, 
  a.prod_hier_lvl5, 
  a.prod_hier_lvl6, 
  a.prod_hier_lvl7, 
  a.prod_hier_lvl8, 
  a.ean_num, 
  b.ciw_desc, 
  b.ciw_code, 
  b.ciw_bucket, 
  b.ciw_acct_nm, 
  b.acct_num, 
  b.acct_nm, 
  a.from_crncy, 
  a.to_crncy, 
  a.ex_rt, 
  a.edw_cust_nm
)
select * from final
