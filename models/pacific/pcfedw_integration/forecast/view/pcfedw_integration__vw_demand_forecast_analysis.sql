with edw_time_dim as (
    select * from {{ ref('pcfedw_integration__edw_time_dim') }}
),
vw_dmnd_frcst_customer_dim as (
    select * from {{ ref('pcfedw_integration__vw_dmnd_frcst_customer_dim') }}
),  
edw_apo_weekly_forecast_fact as (
    select * from {{ ref('pcfedw_integration__edw_apo_weekly_forecast_fact') }}
), 
edw_material_dim as (
    select * from {{ ref('pcfedw_integration__edw_material_dim') }}
),
vw_apo_parent_child_dim as (
    select * from {{ ref('pcfedw_integration__vw_apo_parent_child_dim') }}
),  
  
  
  SELECT 
    'SAPBW' AS pac_source_type, 
    'SAPBW_APO_FORECAST' AS pac_subsource_type, 
    awf.period AS jj_period, 
    awf.week_no AS jj_week_no, 
    vdt.jj_wk, 
    vdt.jj_mnth, 
    vdt.jj_mnth_shrt, 
    vdt.jj_mnth_long, 
    vdt.jj_qrtr, 
    vdt.jj_year, 
    vdt.jj_mnth_tot, 
    awf.week_date, 
    (
      ltrim(
        (awf.material):: text, 
        (
          (0):: character varying
        ):: text
      )
    ):: character varying AS matl_no, 
    vmd.matl_desc, 
    mstrcd.master_code, 
    (
      ltrim(
        (vapcd.parent_id):: text, 
        (
          (0):: character varying
        ):: text
      )
    ):: character varying AS parent_id, 
    mstrcd.parent_matl_desc, 
    vmd.mega_brnd_cd, 
    vmd.mega_brnd_desc, 
    vmd.brnd_cd, 
    vmd.brnd_desc, 
    vmd.base_prod_cd, 
    vmd.base_prod_desc, 
    vmd.variant_cd, 
    vmd.variant_desc, 
    vmd.fran_cd, 
    vmd.fran_desc, 
    vmd.grp_fran_cd, 
    vmd.grp_fran_desc, 
    vmd.matl_type_cd, 
    vmd.matl_type_desc, 
    vmd.prod_fran_cd, 
    vmd.prod_fran_desc, 
    vmd.prod_hier_cd, 
    vmd.prod_hier_desc, 
    vmd.prod_mjr_cd, 
    vmd.prod_mjr_desc, 
    vmd.prod_mnr_cd, 
    vmd.prod_mnr_desc, 
    vmd.mercia_plan, 
    vmd.putup_cd, 
    vmd.putup_desc, 
    vmd.bar_cd, 
    NULL :: "unknown" AS cust_no, 
    vdfcd.cmp_id, 
    NULL :: "unknown" AS ctry_key, 
    vdfcd.country, 
    NULL :: "unknown" AS state_cd, 
    NULL :: "unknown" AS post_cd, 
    NULL :: "unknown" AS cust_suburb, 
    NULL :: "unknown" AS cust_nm, 
    awf.channel AS fcst_chnl, 
    vdfcd.fcst_chnl_desc, 
    NULL :: "unknown" AS sales_office_cd, 
    NULL :: "unknown" AS sales_office_desc, 
    NULL :: "unknown" AS sales_grp_cd, 
    NULL :: "unknown" AS sales_grp_desc, 
    NULL :: "unknown" AS curr_cd, 
    0 AS actual_sales_qty, 
    awf.total_fcst AS apo_tot_frcst, 
    awf.tot_bas_fct AS apo_base_frcst, 
    awf.promo_fcst AS apo_promo_frcst, 
    0 AS px_tot_frcst, 
    0 AS px_base_frcst, 
    0 AS px_promo_frcst, 
    CASE WHEN (
      awf.period > (
        (projprd.prev_jj_period):: numeric
      ):: numeric(18, 0)
    ) THEN awf.total_fcst ELSE (
      (0):: numeric
    ):: numeric(18, 0) END AS projected_apo_tot_frcst, 
    0 AS projected_px_tot_frcst 
  FROM 
    au_edw.edw_time_dim vdt, 
    (
      SELECT 
        (
          to_char(
            add_months(
              (
                to_date(
                  (
                    (t1.jj_mnth_id):: character varying
                  ):: text, 
                  ('YYYYMM' :: character varying):: text
                )
              ):: timestamp without time zone, 
              (
                - (1):: bigint
              )
            ), 
            ('YYYYMM' :: character varying):: text
          )
        ):: integer AS prev_jj_period 
      FROM 
        au_edw.edw_time_dim t1 
      WHERE 
        (
          trunc(t1.cal_date) = trunc(
            (
              ('now' :: character varying):: timestamp without time zone + (1):: bigint
            )
          )
        )
    ) projprd, 
    (
      SELECT 
        DISTINCT vw_dmnd_frcst_customer_dim.cmp_id, 
        vw_dmnd_frcst_customer_dim.country, 
        vw_dmnd_frcst_customer_dim.sls_org, 
        vw_dmnd_frcst_customer_dim.fcst_chnl, 
        vw_dmnd_frcst_customer_dim.fcst_chnl_desc 
      FROM 
        au_edw.vw_dmnd_frcst_customer_dim
    ) vdfcd, 
    (
      (
        au_edw.edw_apo_weekly_forecast_fact awf 
        LEFT JOIN au_edw.edw_material_dim vmd ON (
          (
            (awf.material):: text = (vmd.matl_id):: text
          )
        )
      ) 
      LEFT JOIN (
        au_edw.vw_apo_parent_child_dim vapcd 
        LEFT JOIN (
          SELECT 
            DISTINCT vw_apo_parent_child_dim.master_code, 
            vw_apo_parent_child_dim.parent_matl_desc 
          FROM 
            au_edw.vw_apo_parent_child_dim 
          WHERE 
            (
              (vw_apo_parent_child_dim.cmp_id):: text = (
                (7470):: character varying
              ):: text
            ) 
          UNION ALL 
          SELECT 
            DISTINCT vw_apo_parent_child_dim.master_code, 
            vw_apo_parent_child_dim.parent_matl_desc 
          FROM 
            au_edw.vw_apo_parent_child_dim 
          WHERE 
            (
              NOT (
                vw_apo_parent_child_dim.master_code IN (
                  SELECT 
                    DISTINCT vw_apo_parent_child_dim.master_code 
                  FROM 
                    au_edw.vw_apo_parent_child_dim 
                  WHERE 
                    (
                      (vw_apo_parent_child_dim.cmp_id):: text = (
                        (7470):: character varying
                      ):: text
                    )
                )
              )
            )
        ) mstrcd ON (vapcd.master_code):: text = (mstrcd.master_code):: text
      ) ON 
          ((awf.sales_org):: text = (vapcd.sales_org):: text) 
          AND ((awf.material):: text = (vapcd.matl_id):: text) 
    ) 
  WHERE 
    (
        (awf.week_date):: numeric(18, 0) = vdt.time_id 
      AND 
        (awf.channel):: text = (vdfcd.fcst_chnl):: text
    )