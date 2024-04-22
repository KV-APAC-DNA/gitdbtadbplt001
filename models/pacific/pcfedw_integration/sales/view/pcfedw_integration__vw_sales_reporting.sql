with
edw_time_dim as (
select * from {{ source('pcfedw_integration', 'edw_time_dim') }}
),
edw_sapbw_plan_lkp as (
select * from {{ ref('pcfedw_integration__edw_sapbw_plan_lkp') }}
),
vw_jjbr_curr_exch_dim as (
select * from {{ ref('pcfedw_integration__vw_jjbr_curr_exch_dim') }}
),
vw_bwar_curr_exch_dim as (
select * from {{ ref('pcfedw_integration__vw_bwar_curr_exch_dim') }}
),
vw_sapbw_ciw_fact as (
select * from {{ ref('pcfedw_integration__vw_sapbw_ciw_fact') }}
),
vw_customer_dim as (
select * from {{ ref('pcfedw_integration__vw_customer_dim') }}
),
edw_material_dim as (
select * from {{ ref('pcfedw_integration__edw_material_dim') }}
),
vw_apo_parent_child_dim as (
select * from {{ ref('pcfedw_integration__vw_apo_parent_child_dim') }}
),
edw_sapbw_plan_lkp as (
select * from {{ ref('pcfedw_integration__edw_sapbw_plan_lkp') }}
),
px_combined_ciw_fact as (
select * from {{ ref('pcfedw_integration__px_combined_ciw_fact') }}
),
vw_customer_dim_adj as (
select * from {{ ref('pcfedw_integration__vw_customer_dim_adj') }}
),
edw_material_divest as (
select * from {{ source('pcfedw_integration', 'edw_material_divest') }}
),
edw_bme_transfers as (
select * from {{ source('pcfedw_integration', 'edw_bme_transfers') }}
),
vw_sapbw_futures_fact as (
select * from {{ ref('pcfedw_integration__vw_sapbw_futures_fact') }}
),
edw_vogue_data_ref as (
select * from {{ source('pcfedw_integration', 'edw_vogue_data_ref') }}
),
vw_fin_accrual_adj as (
select * from {{ ref('pcfedw_integration__vw_fin_accrual_adj') }}
),
edw_vw_mds_cogs_rate_dim as (
select * from {{ ref('pcfedw_integration__edw_vw_mds_cogs_rate_dim') }}
),
edw_gch_producthierarchy as (
select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
),
edw_copa_plan_fact as (
select * from {{ ref('aspedw_integration__edw_copa_plan_fact') }}
),
final as (
SELECT
  vsf.pac_source_type,
  vsf.pac_subsource_type,
  vsf.jj_period,
  vsf.jj_wk,
  vsf.jj_mnth,
  vsf.jj_mnth_shrt,
  vsf.jj_mnth_long,
  vsf.jj_qrtr,
  vsf.jj_year,
  vsf.jj_mnth_tot,
  vsf.prcnt_elpsd,
  vsf.elpsd_jj_period,
  vsf.cust_no,
  vsf.cmp_id,
  vsf.channel_cd,
  vsf.channel_desc,
  vsf.ctry_key,
  vsf.country,
  vsf.state_cd,
  vsf.post_cd,
  vsf.cust_suburb,
  vsf.cust_nm,
  vsf.sales_office_cd,
  vsf.sales_office_desc,
  vsf.sales_grp_cd,
  vsf.sales_grp_desc,
  vsf.mercia_ref,
  vsf.pln_cnnl_cd,
  vsf.pln_chnl_desc,
  vsf.cust_curr_cd,
  vsf.matl_id,
  vsf.matl_desc,
  vsf.master_code,
  vsf.parent_matl_id,
  vsf.parent_matl_desc,
  vsf.mega_brnd_cd,
  vsf.mega_brnd_desc,
  vsf.brnd_cd,
  vsf.brnd_desc,
  vsf.base_prod_cd,
  vsf.base_prod_desc,
  vsf.variant_cd,
  vsf.variant_desc,
  vsf.fran_cd,
  vsf.fran_desc,
  vsf.grp_fran_cd,
  vsf.grp_fran_desc,
  vsf.matl_type_cd,
  vsf.matl_type_desc,
  vsf.prod_fran_cd,
  vsf.prod_fran_desc,
  vsf.prod_hier_cd,
  vsf.prod_hier_desc,
  vsf.prod_mjr_cd,
  vsf.prod_mjr_desc,
  vsf.prod_mnr_cd,
  vsf.prod_mnr_desc,
  vsf.mercia_plan,
  vsf.putup_cd,
  vsf.putup_desc,
  vsf.bar_cd,
  vsf.prft_ctr,
  vsf.divest_flag,
  vsf.key_measure,
  vsf.ciw_ctgry,
  vsf.ciw_accnt_grp,
  vsf.px_gl_trans_desc,
  vsf.px_measure,
  vsf.px_bucket,
  vsf.sap_accnt,
  vsf.sap_accnt_desc,
  vsf.local_curr_cd,
  vsf.to_ccy,
  vsf.exch_rate,
  vsf.base_measure,
  vsf.sales_qty,
  vsf.gts,
  vsf.gts_less_rtrn,
  vsf.eff_val,
  vsf.jgf_si_val,
  vsf.pmt_terms_val,
  vsf.datains_val,
  vsf.exp_adj_val,
  vsf.jgf_sd_val,
  vsf.nts_val,
  vsf.tot_ciw_val,
  vsf.posted_cogs,
  vsf.posted_con_free_goods_val,
  vsf.posted_gp,
  vsf.cogs,
  vsf.gp,
  vsf.futr_sls_qty,
  vsf.futr_gts,
  vsf.futr_ts,
  vsf.futr_nts,
  vsf.px_qty,
  vsf.px_gts,
  vsf.px_gts_less_rtrn,
  vsf.px_eff_val,
  vsf.px_jgf_si_val,
  vsf.px_pmt_terms_val,
  vsf.px_datains_val,
  vsf.px_exp_adj_val,
  vsf.px_jgf_sd_val,
  vsf.px_nts,
  vsf.px_ciw_tot,
  vsf.px_cogs,
  vsf.px_gp,
  vsf.projected_qty,
  vsf.projected_gts_less_rtrn,
  vsf.projected_eff_val,
  vsf.projected_jgf_si_val,
  vsf.projected_pmt_terms_val,
  vsf.projected_datains_val,
  vsf.projected_exp_adj_val,
  vsf.projected_jgf_sd_val,
  vsf.projected_ciw_tot,
  vsf.projected_nts,
  vsf.projected_cogs_current,
  vsf.projected_gp_current,
  vsf.projected_cogs_actual,
  vsf.projected_gp_actual,
  vsf.goal_qty,
  vsf.goal_gts,
  vsf.goal_eff_val,
  vsf.goal_jgf_si_val,
  vsf.goal_pmt_terms_val,
  vsf.goal_datains_val,
  vsf.goal_exp_adj_val,
  vsf.goal_jgf_sd_val,
  vsf.goal_ciw_tot,
  vsf.goal_nts,
  vsf.goal_cogs,
  vsf.goal_gp,
  vsf.bp_qty,
  vsf.bp_gts,
  vsf.bp_eff_val,
  vsf.bp_jgf_si_val,
  vsf.bp_pmt_terms_val,
  vsf.bp_datains_val,
  vsf.bp_exp_adj_val,
  vsf.bp_jgf_sd_val,
  vsf.bp_ciw_tot,
  vsf.bp_nts,
  vsf.bp_cogs,
  vsf.bp_gp,
  vsf.ju_qty,
  vsf.ju_gts,
  vsf.ju_eff_val,
  vsf.ju_jgf_si_val,
  vsf.ju_pmt_terms_val,
  vsf.ju_datains_val,
  vsf.ju_exp_adj_val,
  vsf.ju_jgf_sd_val,
  vsf.ju_ciw_tot,
  vsf.ju_nts,
  vsf.ju_cogs,
  vsf.ju_gp,
  vsf.nu_qty,
  vsf.nu_gts,
  vsf.nu_eff_val,
  vsf.nu_jgf_si_val,
  vsf.nu_pmt_terms_val,
  vsf.nu_datains_val,
  vsf.nu_exp_adj_val,
  vsf.nu_jgf_sd_val,
  vsf.nu_ciw_tot,
  vsf.nu_nts,
  vsf.nu_cogs,
  vsf.nu_gp,
  vsf.bme_trans,
  vsf.incr_gts,
  vsf.px_case_qty,
  vsf.px_tot_planspend,
  vsf.px_tot_paid,
  vsf.px_committed_spend,
  (
    (
      cogs.cogs_per_unit * vsf.exch_rate
    )
  ):: numeric(38, 7) AS finance_cogs,
  vsf.gcph_franchise,
  vsf.gcph_brand,
  vsf.gcph_subbrand,
  vsf.gcph_variant,
  vsf.gcph_needstate,
  vsf.gcph_category,
  vsf.gcph_subcategory,
  vsf.gcph_segment,
  vsf.gcph_subsegment
FROM
  (
    (
      (
        (
          (
            (
              (
                (
                  SELECT
                    'SAPBW' :: character varying AS pac_source_type,
                    'SAPBW_ACTUAL' :: character varying AS pac_subsource_type,
                    etd.jj_mnth_id AS jj_period,
                    jj_tot_wks.total_wks AS jj_wk,
                    etd.jj_mnth,
                    etd.jj_mnth_shrt,
                    etd.jj_mnth_long,
                    etd.jj_qrtr,
                    etd.jj_year,
                    etd.jj_mnth_tot,
                    prelap.prcnt_elpsd,
                    prelap.elpsd_jj_period,
                    (
                      ltrim(
                        (vcd.cust_no):: text,
                        (
                          (0):: character varying
                        ):: text
                      )
                    ):: character varying AS cust_no,
                    vsf.cmp_id,
                    vcd.channel_cd,
                    vcd.channel_desc,
                    vcd.ctry_key,
                    COALESCE(
                      vcd.country,
                      CASE WHEN (
                        (
                          (vsf.cmp_id):: text = ('7470' :: character varying):: text
                        )
                        OR (
                          (vsf.cmp_id IS NULL)
                          AND ('7470' IS NULL)
                        )
                      ) THEN 'Australia' :: character varying WHEN (
                        (
                          (vsf.cmp_id):: text = ('8361' :: character varying):: text
                        )
                        OR (
                          (vsf.cmp_id IS NULL)
                          AND ('8361' IS NULL)
                        )
                      ) THEN 'New Zealand' :: character varying ELSE NULL :: character varying END
                    ) AS country,
                    vcd.state_cd,
                    vcd.post_cd,
                    vcd.cust_suburb,
                    vcd.cust_nm,
                    vcd.sales_office_cd,
                    vcd.sales_office_desc,
                    vcd.sales_grp_cd,
                    vcd.sales_grp_desc,
                    vcd.mercia_ref,
                    spl.pln_chnl_cd AS pln_cnnl_cd,
                    spl.pln_chnl_desc,
                    vcd.curr_cd AS cust_curr_cd,
                    (
                      ltrim(
                        (vmd.matl_id):: text,
                        (
                          (0):: character varying
                        ):: text
                      )
                    ):: character varying AS matl_id,
                    vmd.matl_desc,
                    mstrcd.master_code,
                    (
                      ltrim(
                        (vapcd.parent_id):: text,
                        (
                          (0):: character varying
                        ):: text
                      )
                    ):: character varying AS parent_matl_id,
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
                    vmd.prft_ctr,
                    nvl2(
                      emd.divested_sku, 'Y' :: character varying,
                      'N' :: character varying
                    ) AS divest_flag,
                    vsf.key_measure,
                    vsf.ciw_ctgry,
                    vsf.ciw_accnt_grp,
                    NULL :: character varying AS px_gl_trans_desc,
                    NULL :: character varying AS px_measure,
                    NULL :: character varying AS px_bucket,
                    vsf.sap_accnt,
                    vsf.sap_accnt_nm AS sap_accnt_desc,
                    vsf.local_ccy AS local_curr_cd,
                    currex.to_ccy,
                    currex.exch_rate,
                    (
                      (vsf.base_measure):: numeric(38, 2) * currex.exch_rate
                    ) AS base_measure,
                    vsf.sales_qty,
                    (
                      (vsf.gts_val):: numeric(38, 2) * currex.exch_rate
                    ) AS gts,
                    (
                      (vsf.gts_less_rtrn_val):: numeric(38, 2) * currex.exch_rate
                    ) AS gts_less_rtrn,
                    (
                      (vsf.eff_val):: numeric(38, 2) * currex.exch_rate
                    ) AS eff_val,
                    (
                      (vsf.jgf_si_val):: numeric(38, 2) * currex.exch_rate
                    ) AS jgf_si_val,
                    (
                      (vsf.pmt_terms_val):: numeric(38, 2) * currex.exch_rate
                    ) AS pmt_terms_val,
                    (
                      (vsf.datains_val):: numeric(38, 2) * currex.exch_rate
                    ) AS datains_val,
                    (
                      (vsf.exp_adj_val):: numeric(38, 2) * currex.exch_rate
                    ) AS exp_adj_val,
                    (
                      (vsf.jgf_sd_val):: numeric(38, 2) * currex.exch_rate
                    ) AS jgf_sd_val,
                    (
                      (vsf.nts_val):: numeric(38, 2) * currex.exch_rate
                    ) AS nts_val,
                    (
                      (vsf.tot_ciw_val):: numeric(38, 2) * currex.exch_rate
                    ) AS tot_ciw_val,
                    (
                      (vsf.cogs_val):: numeric(38, 2) * currex.exch_rate
                    ) AS posted_cogs,
                    (
                      (vsf.con_free_goods_val):: numeric(38, 2) * currex.exch_rate
                    ) AS posted_con_free_goods_val,
                    (
                      (vsf.gp_val):: numeric(38, 2) * currex.exch_rate
                    ) AS posted_gp,
                    (
                      (vsf.std_cost_lc):: numeric(38, 2) * currex.exch_rate
                    ) AS cogs,
                    (
                      (vsf.std_cost_gp_val):: numeric(38, 2) * currex.exch_rate
                    ) AS gp,
                    0 AS futr_sls_qty,
                    0 AS futr_gts,
                    0 AS futr_ts,
                    0 AS futr_nts,
                    0 AS px_qty,
                    0 AS px_gts,
                    0 AS px_gts_less_rtrn,
                    0 AS px_eff_val,
                    0 AS px_jgf_si_val,
                    0 AS px_pmt_terms_val,
                    0 AS px_datains_val,
                    0 AS px_exp_adj_val,
                    0 AS px_jgf_sd_val,
                    0 AS px_nts,
                    0 AS px_ciw_tot,
                    0 AS px_cogs,
                    0 AS px_gp,
                    CASE WHEN (
                      etd.jj_mnth_id <= (
                        (projprd.prev_jj_period):: numeric
                      ):: numeric(18, 0)
                    ) THEN vsf.sales_qty ELSE (
                      (0):: numeric
                    ):: numeric(18, 0) END AS projected_qty,
                    CASE WHEN (
                      etd.jj_mnth_id <= (
                        (projprd.prev_jj_period):: numeric
                      ):: numeric(18, 0)
                    ) THEN (
                      (vsf.gts_less_rtrn_val):: numeric(38, 2) * currex.exch_rate
                    ) ELSE (
                      (0):: numeric
                    ):: numeric(18, 0) END AS projected_gts_less_rtrn,
                    CASE WHEN (
                      etd.jj_mnth_id <= (
                        (projprd.prev_jj_period):: numeric
                      ):: numeric(18, 0)
                    ) THEN (
                      (vsf.eff_val):: numeric(38, 2) * currex.exch_rate
                    ) ELSE (
                      (0):: numeric
                    ):: numeric(18, 0) END AS projected_eff_val,
                    CASE WHEN (
                      etd.jj_mnth_id <= (
                        (projprd.prev_jj_period):: numeric
                      ):: numeric(18, 0)
                    ) THEN (
                      (vsf.jgf_si_val):: numeric(38, 2) * currex.exch_rate
                    ) ELSE (
                      (0):: numeric
                    ):: numeric(18, 0) END AS projected_jgf_si_val,
                    CASE WHEN (
                      etd.jj_mnth_id <= (
                        (projprd.prev_jj_period):: numeric
                      ):: numeric(18, 0)
                    ) THEN (
                      (vsf.pmt_terms_val):: numeric(38, 2) * currex.exch_rate
                    ) ELSE (
                      (0):: numeric
                    ):: numeric(18, 0) END AS projected_pmt_terms_val,
                    CASE WHEN (
                      etd.jj_mnth_id <= (
                        (projprd.prev_jj_period):: numeric
                      ):: numeric(18, 0)
                    ) THEN (
                      (vsf.datains_val):: numeric(38, 2) * currex.exch_rate
                    ) ELSE (
                      (0):: numeric
                    ):: numeric(18, 0) END AS projected_datains_val,
                    CASE WHEN (
                      etd.jj_mnth_id <= (
                        (projprd.prev_jj_period):: numeric
                      ):: numeric(18, 0)
                    ) THEN (
                      (vsf.exp_adj_val):: numeric(38, 2) * currex.exch_rate
                    ) ELSE (
                      (0):: numeric
                    ):: numeric(18, 0) END AS projected_exp_adj_val,
                    CASE WHEN (
                      etd.jj_mnth_id <= (
                        (projprd.prev_jj_period):: numeric
                      ):: numeric(18, 0)
                    ) THEN (
                      (vsf.jgf_sd_val):: numeric(38, 2) * currex.exch_rate
                    ) ELSE (
                      (0):: numeric
                    ):: numeric(18, 0) END AS projected_jgf_sd_val,
                    CASE WHEN (
                      etd.jj_mnth_id <= (
                        (projprd.prev_jj_period):: numeric
                      ):: numeric(18, 0)
                    ) THEN (
                      (vsf.tot_ciw_val):: numeric(38, 2) * currex.exch_rate
                    ) ELSE (
                      (0):: numeric
                    ):: numeric(18, 0) END AS projected_ciw_tot,
                    CASE WHEN (
                      etd.jj_mnth_id <= (
                        (projprd.prev_jj_period):: numeric
                      ):: numeric(18, 0)
                    ) THEN (
                      (vsf.nts_val):: numeric(38, 2) * currex.exch_rate
                    ) ELSE (
                      (0):: numeric
                    ):: numeric(18, 0) END AS projected_nts,
                    CASE WHEN (
                      etd.jj_mnth_id <= (
                        (projprd.prev_jj_period):: numeric
                      ):: numeric(18, 0)
                    ) THEN (
                      (vsf.std_cost_lc):: numeric(38, 2) * currex.exch_rate
                    ) ELSE (
                      (0):: numeric
                    ):: numeric(18, 0) END AS projected_cogs_current,
                    (
                      (
                        CASE WHEN (
                          etd.jj_mnth_id <= (
                            (projprd.prev_jj_period):: numeric
                          ):: numeric(18, 0)
                        ) THEN (
                          (vsf.gts_val):: numeric(38, 2) * currex.exch_rate
                        ) ELSE (
                          (0):: numeric
                        ):: numeric(18, 0) END - CASE WHEN (
                          etd.jj_mnth_id <= (
                            (projprd.prev_jj_period):: numeric
                          ):: numeric(18, 0)
                        ) THEN (
                          (vsf.tot_ciw_val):: numeric(38, 2) * currex.exch_rate
                        ) ELSE (
                          (0):: numeric
                        ):: numeric(18, 0) END
                      ) - CASE WHEN (
                        etd.jj_mnth_id <= (
                          (projprd.prev_jj_period):: numeric
                        ):: numeric(18, 0)
                      ) THEN (
                        (vsf.std_cost_lc):: numeric(38, 2) * currex.exch_rate
                      ) ELSE (
                        (0):: numeric
                      ):: numeric(18, 0) END
                    ) AS projected_gp_current,
                    (
                      CASE WHEN (
                        etd.jj_mnth_id <= (
                          (projprd.prev_jj_period):: numeric
                        ):: numeric(18, 0)
                      ) THEN (
                        (vsf.cogs_val):: numeric(38, 2) * currex.exch_rate
                      ) ELSE (
                        (0):: numeric
                      ):: numeric(18, 0) END + CASE WHEN (
                        etd.jj_mnth_id <= (
                          (projprd.prev_jj_period):: numeric
                        ):: numeric(18, 0)
                      ) THEN (
                        (vsf.con_free_goods_val):: numeric(38, 2) * currex.exch_rate
                      ) ELSE (
                        (0):: numeric
                      ):: numeric(18, 0) END
                    ) AS projected_cogs_actual,
                    (
                      (
                        (
                          CASE WHEN (
                            etd.jj_mnth_id <= (
                              (projprd.prev_jj_period):: numeric
                            ):: numeric(18, 0)
                          ) THEN (
                            (vsf.gts_val):: numeric(38, 2) * currex.exch_rate
                          ) ELSE (
                            (0):: numeric
                          ):: numeric(18, 0) END - CASE WHEN (
                            etd.jj_mnth_id <= (
                              (projprd.prev_jj_period):: numeric
                            ):: numeric(18, 0)
                          ) THEN (
                            (vsf.tot_ciw_val):: numeric(38, 2) * currex.exch_rate
                          ) ELSE (
                            (0):: numeric
                          ):: numeric(18, 0) END
                        ) - CASE WHEN (
                          etd.jj_mnth_id <= (
                            (projprd.prev_jj_period):: numeric
                          ):: numeric(18, 0)
                        ) THEN (
                          (vsf.cogs_val):: numeric(38, 2) * currex.exch_rate
                        ) ELSE (
                          (0):: numeric
                        ):: numeric(18, 0) END
                      ) - CASE WHEN (
                        etd.jj_mnth_id <= (
                          (projprd.prev_jj_period):: numeric
                        ):: numeric(18, 0)
                      ) THEN (
                        (vsf.con_free_goods_val):: numeric(38, 2) * currex.exch_rate
                      ) ELSE (
                        (0):: numeric
                      ):: numeric(18, 0) END
                    ) AS projected_gp_actual,
                    0 AS goal_qty,
                    0 AS goal_gts,
                    0 AS goal_eff_val,
                    0 AS goal_jgf_si_val,
                    0 AS goal_pmt_terms_val,
                    0 AS goal_datains_val,
                    0 AS goal_exp_adj_val,
                    0 AS goal_jgf_sd_val,
                    0 AS goal_ciw_tot,
                    0 AS goal_nts,
                    0 AS goal_cogs,
                    0 AS goal_gp,
                    0 AS bp_qty,
                    0 AS bp_gts,
                    0 AS bp_eff_val,
                    0 AS bp_jgf_si_val,
                    0 AS bp_pmt_terms_val,
                    0 AS bp_datains_val,
                    0 AS bp_exp_adj_val,
                    0 AS bp_jgf_sd_val,
                    0 AS bp_ciw_tot,
                    0 AS bp_nts,
                    0 AS bp_cogs,
                    0 AS bp_gp,
                    0 AS ju_qty,
                    0 AS ju_gts,
                    0 AS ju_eff_val,
                    0 AS ju_jgf_si_val,
                    0 AS ju_pmt_terms_val,
                    0 AS ju_datains_val,
                    0 AS ju_exp_adj_val,
                    0 AS ju_jgf_sd_val,
                    0 AS ju_ciw_tot,
                    0 AS ju_nts,
                    0 AS ju_cogs,
                    0 AS ju_gp,
                    0 AS nu_qty,
                    0 AS nu_gts,
                    0 AS nu_eff_val,
                    0 AS nu_jgf_si_val,
                    0 AS nu_pmt_terms_val,
                    0 AS nu_datains_val,
                    0 AS nu_exp_adj_val,
                    0 AS nu_jgf_sd_val,
                    0 AS nu_ciw_tot,
                    0 AS nu_nts,
                    0 AS nu_cogs,
                    0 AS nu_gp,
                    0 AS bme_trans,
                    0 AS incr_gts,
                    0 AS px_case_qty,
                    0 AS px_tot_planspend,
                    0 AS px_tot_paid,
                    0 AS px_committed_spend,
                    b.gcph_franchise,
                    b.gcph_brand,
                    b.gcph_subbrand,
                    b.gcph_variant,
                    b.gcph_needstate,
                    b.gcph_category,
                    b.gcph_subcategory,
                    b.gcph_segment,
                    b.gcph_subsegment
                  FROM
                    (
                      SELECT
                        DISTINCT edw_time_dim.jj_mnth,
                        edw_time_dim.jj_mnth_shrt,
                        edw_time_dim.jj_mnth_long,
                        edw_time_dim.jj_qrtr,
                        edw_time_dim.jj_year,
                        edw_time_dim.jj_mnth_id,
                        edw_time_dim.jj_mnth_tot
                      FROM
                        edw_time_dim
                    ) etd,
                    edw_sapbw_plan_lkp spl,
                    (
                      SELECT
                        vw_jjbr_curr_exch_dim.rate_type,
                        vw_jjbr_curr_exch_dim.from_ccy,
                        vw_jjbr_curr_exch_dim.to_ccy,
                        vw_jjbr_curr_exch_dim.jj_mnth_id,
                        vw_jjbr_curr_exch_dim.exch_rate
                      FROM
                        vw_jjbr_curr_exch_dim
                      WHERE
                        (
                          (
                            vw_jjbr_curr_exch_dim.exch_rate = (
                              (
                                (1):: numeric
                              ):: numeric(18, 0)
                            ):: numeric(15, 5)
                          )
                          AND (
                            (vw_jjbr_curr_exch_dim.from_ccy):: text = ('AUD' :: character varying):: text
                          )
                        )
                      UNION ALL
                      SELECT
                        vw_bwar_curr_exch_dim.rate_type,
                        vw_bwar_curr_exch_dim.from_ccy,
                        vw_bwar_curr_exch_dim.to_ccy,
                        vw_bwar_curr_exch_dim.jj_mnth_id,
                        vw_bwar_curr_exch_dim.exch_rate
                      FROM
                        vw_bwar_curr_exch_dim
                    ) currex,
                    (
                      SELECT
                        t1.jj_mnth_id AS elpsd_jj_period,
                        (
                          (t1.jj_mnth_day / t1.jj_mnth_tot) * (
                            (100):: numeric
                          ):: numeric(18, 0)
                        ) AS prcnt_elpsd
                      FROM
                        edw_time_dim t1
                      WHERE
                        (
                          t1.cal_date::date = dateadd(day,1,CURRENT_TIMESTAMP)::date
                        )
                    ) prelap,
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
                        ):: integer AS prev_jj_period,
                        t1.jj_mnth_id AS curr_jj_period
                      FROM
                        edw_time_dim t1
                      WHERE
                        (
                          t1.cal_date::date = dateadd(day,1,CURRENT_TIMESTAMP)::date
                        )
                    ) projprd,
                    (
                      SELECT
                        derived_table2.jj_mnth_id,
                        "max"(derived_table2.rownum) AS total_wks
                      FROM
                        (
                          SELECT
                            derived_table1.jj_mnth_id,
                            derived_table1.jj_wk,
                            row_number() OVER(
                              PARTITION BY derived_table1.jj_mnth_id
                              ORDER BY
                                derived_table1.jj_wk
                            ) AS rownum
                          FROM
                            (
                              SELECT
                                DISTINCT edw_time_dim.jj_mnth_id,
                                edw_time_dim.jj_wk
                              FROM
                                edw_time_dim
                            ) derived_table1
                        ) derived_table2
                      GROUP BY
                        derived_table2.jj_mnth_id
                    ) jj_tot_wks,
                    (
                      (
                        (
                          (
                            (
                              vw_sapbw_ciw_fact vsf
                              LEFT JOIN (
                                (
                                  (
                                    (
                                      (
                                        SELECT
                                          vw_customer_dim.cust_no,
                                          vw_customer_dim.cmp_id,
                                          vw_customer_dim.channel_cd,
                                          vw_customer_dim.channel_desc,
                                          (vw_customer_dim.ctry_key):: character varying AS ctry_key,
                                          vw_customer_dim.country,
                                          (vw_customer_dim.state_cd):: character varying AS state_cd,
                                          (vw_customer_dim.post_cd):: character varying AS post_cd,
                                          (vw_customer_dim.cust_suburb):: character varying AS cust_suburb,
                                          (vw_customer_dim.cust_nm):: character varying AS cust_nm,
                                          vw_customer_dim.sls_org,
                                          (vw_customer_dim.cust_del_flag):: character varying AS cust_del_flag,
                                          vw_customer_dim.sales_office_cd,
                                          vw_customer_dim.sales_office_desc,
                                          vw_customer_dim.sales_grp_cd,
                                          vw_customer_dim.sales_grp_desc,
                                          (vw_customer_dim.mercia_ref):: character varying AS mercia_ref,
                                          (vw_customer_dim.curr_cd):: character varying AS curr_cd
                                        FROM
                                          vw_customer_dim
                                        UNION
                                        SELECT
                                          vw_customer_dim_adj.cust_no,
                                          vw_customer_dim_adj.cmp_id,
                                          vw_customer_dim_adj.channel_cd,
                                          vw_customer_dim_adj.channel_desc,
                                          (vw_customer_dim_adj.ctry_key):: character varying AS ctry_key,
                                          vw_customer_dim_adj.country,
                                          (vw_customer_dim_adj.state_cd):: character varying AS state_cd,
                                          (vw_customer_dim_adj.post_cd):: character varying AS post_cd,
                                          (
                                            vw_customer_dim_adj.cust_suburb
                                          ):: character varying AS cust_suburb,
                                          (vw_customer_dim_adj.cust_nm):: character varying AS cust_nm,
                                          vw_customer_dim_adj.sls_org,
                                          (
                                            vw_customer_dim_adj.cust_del_flag
                                          ):: character varying AS cust_del_flag,
                                          vw_customer_dim_adj.sales_office_cd,
                                          vw_customer_dim_adj.sales_office_desc,
                                          vw_customer_dim_adj.sales_grp_cd,
                                          vw_customer_dim_adj.sales_grp_desc,
                                          (vw_customer_dim_adj.mercia_ref):: character varying AS mercia_ref,
                                          (vw_customer_dim_adj.curr_cd):: character varying AS curr_cd
                                        FROM
                                          vw_customer_dim_adj
                                      )
                                      UNION
                                      SELECT
                                        'Unknown' :: character varying AS cust_no,
                                        '7470' :: character varying AS cmp_id,
                                        vw_customer_dim.channel_cd,
                                        vw_customer_dim.channel_desc,
                                        (vw_customer_dim.ctry_key):: character varying AS ctry_key,
                                        vw_customer_dim.country,
                                        (vw_customer_dim.state_cd):: character varying AS state_cd,
                                        (vw_customer_dim.post_cd):: character varying AS post_cd,
                                        (vw_customer_dim.cust_suburb):: character varying AS cust_suburb,
                                        'Unknown' :: character varying AS cust_nm,
                                        vw_customer_dim.sls_org,
                                        (vw_customer_dim.cust_del_flag):: character varying AS cust_del_flag,
                                        vw_customer_dim.sales_office_cd,
                                        vw_customer_dim.sales_office_desc,
                                        vw_customer_dim.sales_grp_cd,
                                        vw_customer_dim.sales_grp_desc,
                                        (vw_customer_dim.mercia_ref):: character varying AS mercia_ref,
                                        (vw_customer_dim.curr_cd):: character varying AS curr_cd
                                      FROM
                                        vw_customer_dim
                                      WHERE
                                        (
                                          (
                                            (
                                              (vw_customer_dim.cmp_id):: text = ('7470' :: character varying):: text
                                            )
                                            AND (
                                              vw_customer_dim.post_cd = ('3025' :: character varying):: text
                                            )
                                          )
                                          AND (
                                            (vw_customer_dim.sales_grp_desc):: text = ('Unknown' :: character varying):: text
                                          )
                                        )
                                    )
                                    UNION
                                    SELECT
                                      'Unknown' :: character varying AS cust_no,
                                      '747A' :: character varying AS cmp_id,
                                      vw_customer_dim.channel_cd,
                                      vw_customer_dim.channel_desc,
                                      (vw_customer_dim.ctry_key):: character varying AS ctry_key,
                                      vw_customer_dim.country,
                                      (vw_customer_dim.state_cd):: character varying AS state_cd,
                                      (vw_customer_dim.post_cd):: character varying AS post_cd,
                                      (vw_customer_dim.cust_suburb):: character varying AS cust_suburb,
                                      'Unknown' :: character varying AS cust_nm,
                                      vw_customer_dim.sls_org,
                                      (vw_customer_dim.cust_del_flag):: character varying AS cust_del_flag,
                                      vw_customer_dim.sales_office_cd,
                                      vw_customer_dim.sales_office_desc,
                                      vw_customer_dim.sales_grp_cd,
                                      vw_customer_dim.sales_grp_desc,
                                      (vw_customer_dim.mercia_ref):: character varying AS mercia_ref,
                                      (vw_customer_dim.curr_cd):: character varying AS curr_cd
                                    FROM
                                      vw_customer_dim
                                    WHERE
                                      (
                                        (
                                          (vw_customer_dim.cmp_id):: text = ('747A' :: character varying):: text
                                        )
                                        AND (
                                          vw_customer_dim.post_cd = ('27106' :: character varying):: text
                                        )
                                      )
                                  )
                                  UNION
                                  SELECT
                                    'Unknown' :: character varying AS cust_no,
                                    '836A' :: character varying AS cmp_id,
                                    vw_customer_dim.channel_cd,
                                    vw_customer_dim.channel_desc,
                                    (vw_customer_dim.ctry_key):: character varying AS ctry_key,
                                    vw_customer_dim.country,
                                    (vw_customer_dim.state_cd):: character varying AS state_cd,
                                    (vw_customer_dim.post_cd):: character varying AS post_cd,
                                    (vw_customer_dim.cust_suburb):: character varying AS cust_suburb,
                                    'Unknown' :: character varying AS cust_nm,
                                    vw_customer_dim.sls_org,
                                    (vw_customer_dim.cust_del_flag):: character varying AS cust_del_flag,
                                    vw_customer_dim.sales_office_cd,
                                    vw_customer_dim.sales_office_desc,
                                    vw_customer_dim.sales_grp_cd,
                                    vw_customer_dim.sales_grp_desc,
                                    (vw_customer_dim.mercia_ref):: character varying AS mercia_ref,
                                    (vw_customer_dim.curr_cd):: character varying AS curr_cd
                                  FROM
                                    vw_customer_dim
                                  WHERE
                                    (
                                      (
                                        (vw_customer_dim.cmp_id):: text = ('836A' :: character varying):: text
                                      )
                                      AND (
                                        vw_customer_dim.post_cd = ('193' :: character varying):: text
                                      )
                                    )
                                )
                                UNION
                                SELECT
                                  'Unknown' :: character varying AS cust_no,
                                  '8361' :: character varying AS cmp_id,
                                  vw_customer_dim.channel_cd,
                                  vw_customer_dim.channel_desc,
                                  (vw_customer_dim.ctry_key):: character varying AS ctry_key,
                                  vw_customer_dim.country,
                                  (vw_customer_dim.state_cd):: character varying AS state_cd,
                                  (vw_customer_dim.post_cd):: character varying AS post_cd,
                                  (vw_customer_dim.cust_suburb):: character varying AS cust_suburb,
                                  'Unknown' :: character varying AS cust_nm,
                                  vw_customer_dim.sls_org,
                                  (vw_customer_dim.cust_del_flag):: character varying AS cust_del_flag,
                                  vw_customer_dim.sales_office_cd,
                                  vw_customer_dim.sales_office_desc,
                                  vw_customer_dim.sales_grp_cd,
                                  vw_customer_dim.sales_grp_desc,
                                  (vw_customer_dim.mercia_ref):: character varying AS mercia_ref,
                                  (vw_customer_dim.curr_cd):: character varying AS curr_cd
                                FROM
                                  vw_customer_dim
                                WHERE
                                  (
                                    (
                                      (
                                        (vw_customer_dim.cmp_id):: text = ('8361' :: character varying):: text
                                      )
                                      AND (
                                        vw_customer_dim.post_cd = ('1010' :: character varying):: text
                                      )
                                    )
                                    AND (
                                      (vw_customer_dim.sales_grp_desc):: text = ('Unknown' :: character varying):: text
                                    )
                                  )
                              ) vcd ON (
                                (
                                  (
                                    (
                                      COALESCE(
                                        CASE WHEN (
                                          (vsf.cust_no):: text = ('' :: character varying):: text
                                        ) THEN NULL :: character varying ELSE vsf.cust_no END,
                                        'Unknown' :: character varying
                                      )
                                    ):: text = (vcd.cust_no):: text
                                  )
                                  AND (
                                    (vsf.cmp_id):: text = (vcd.cmp_id):: text
                                  )
                                )
                              )
                            )
                            LEFT JOIN edw_material_dim vmd ON (
                              (
                                (vsf.matl_id):: text = (vmd.matl_id):: text
                              )
                            )
                          )
                          LEFT JOIN (
                            vw_apo_parent_child_dim vapcd
                            LEFT JOIN (
                              SELECT
                                DISTINCT vw_apo_parent_child_dim.master_code,
                                vw_apo_parent_child_dim.parent_matl_desc
                              FROM
                                vw_apo_parent_child_dim
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
                                vw_apo_parent_child_dim
                              WHERE
                                (
                                  NOT (
                                    vw_apo_parent_child_dim.master_code IN (
                                      SELECT
                                        DISTINCT vw_apo_parent_child_dim.master_code
                                      FROM
                                        vw_apo_parent_child_dim
                                      WHERE
                                        (
                                          (vw_apo_parent_child_dim.cmp_id):: text = (
                                            (7470):: character varying
                                          ):: text
                                        )
                                    )
                                  )
                                )
                            ) mstrcd ON (
                              (
                                (vapcd.master_code):: text = (mstrcd.master_code):: text
                              )
                            )
                          ) ON (
                            (
                              (
                                (vsf.cmp_id):: text = (vapcd.cmp_id):: text
                              )
                              AND (
                                (vsf.matl_id):: text = (vapcd.matl_id):: text
                              )
                            )
                          )
                        )
                        LEFT JOIN edw_material_divest emd ON (
                          (
                            (
                              (vsf.cmp_id):: text = (
                                (emd.cmp_id):: character varying
                              ):: text
                            )
                            AND (
                              ltrim(
                                (vsf.matl_id):: text,
                                ('0' :: character varying):: text
                              ) = (
                                (emd.divested_sku):: character varying
                              ):: text
                            )
                          )
                        )
                      )
                      LEFT JOIN (
                        SELECT
                          edw_gch_producthierarchy.materialnumber,
                          edw_gch_producthierarchy.gcph_franchise,
                          edw_gch_producthierarchy.gcph_brand,
                          edw_gch_producthierarchy.gcph_subbrand,
                          edw_gch_producthierarchy.gcph_variant,
                          edw_gch_producthierarchy.gcph_needstate,
                          edw_gch_producthierarchy.gcph_category,
                          edw_gch_producthierarchy.gcph_subcategory,
                          edw_gch_producthierarchy.gcph_segment,
                          edw_gch_producthierarchy.gcph_subsegment
                        FROM
                          edw_gch_producthierarchy
                        WHERE
                          (
                            (
                              ltrim(
                                (
                                  edw_gch_producthierarchy.materialnumber
                                ):: text,
                                (
                                  (0):: character varying
                                ):: text
                              ) <> ('' :: character varying):: text
                            )
                            AND (
                              (
                                edw_gch_producthierarchy."region"
                              ):: text = ('APAC' :: character varying):: text
                            )
                          )
                      ) b ON (
                        (
                          ltrim(
                            (vsf.matl_id):: text,
                            (
                              (0):: character varying
                            ):: text
                          ) = ltrim(
                            (b.materialnumber):: text,
                            (
                              (0):: character varying
                            ):: text
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
                              (
                                (vsf.jj_month_id):: numeric
                              ):: numeric(18, 0) = etd.jj_mnth_id
                            )
                            AND (
                              (vsf.local_ccy):: text = (currex.from_ccy):: text
                            )
                          )
                          AND (
                            etd.jj_mnth_id = currex.jj_mnth_id
                          )
                        )
                        AND (
                          jj_tot_wks.jj_mnth_id = etd.jj_mnth_id
                        )
                      )
                      AND (
                        (spl.sls_grp_cd):: text = (vcd.sales_grp_cd):: text
                      )
                    )
                  UNION ALL
                  SELECT
                    pcf.pac_source_type,
                    pcf.pac_subsource_type,
                    etd.jj_mnth_id AS jj_period,
                    jj_tot_wks.total_wks AS jj_wk,
                    etd.jj_mnth,
                    etd.jj_mnth_shrt,
                    etd.jj_mnth_long,
                    etd.jj_qrtr,
                    etd.jj_year,
                    etd.jj_mnth_tot,
                    prelap.prcnt_elpsd,
                    prelap.elpsd_jj_period,
                    (
                      ltrim(
                        (vcd.cust_no):: text,
                        (
                          (0):: character varying
                        ):: text
                      )
                    ):: character varying AS cust_no,
                    vcd.cmp_id,
                    vcd.channel_cd,
                    vcd.channel_desc,
                    (vcd.ctry_key):: character varying AS ctry_key,
                    vcd.country,
                    (vcd.state_cd):: character varying AS state_cd,
                    (vcd.post_cd):: character varying AS post_cd,
                    (vcd.cust_suburb):: character varying AS cust_suburb,
                    (vcd.cust_nm):: character varying AS cust_nm,
                    vcd.sales_office_cd,
                    vcd.sales_office_desc,
                    vcd.sales_grp_cd,
                    vcd.sales_grp_desc,
                    (vcd.mercia_ref):: character varying AS mercia_ref,
                    spl.pln_chnl_cd AS pln_cnnl_cd,
                    spl.pln_chnl_desc,
                    (vcd.curr_cd):: character varying AS cust_curr_cd,
                    (
                      ltrim(
                        (vmd.matl_id):: text,
                        (
                          (0):: character varying
                        ):: text
                      )
                    ):: character varying AS matl_id,
                    vmd.matl_desc,
                    mstrcd.master_code,
                    (
                      ltrim(
                        (vapcd.parent_id):: text,
                        (
                          (0):: character varying
                        ):: text
                      )
                    ):: character varying AS parent_matl_id,
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
                    vmd.prft_ctr,
                    nvl2(
                      emd.divested_sku, 'Y' :: character varying,
                      'N' :: character varying
                    ) AS divest_flag,
                    pcf.key_measure,
                    pcf.ciw_category AS ciw_ctgry,
                    pcf.ciw_account_group AS ciw_accnt_grp,
                    pcf.gl_trans_desc AS px_gl_trans_desc,
                    pcf.px_measure,
                    pcf.px_bucket,
                    pcf.sap_accnt,
                    pcf.sap_accnt_nm AS sap_accnt_desc,
                    pcf.local_ccy AS local_curr_cd,
                    currex.to_ccy,
                    currex.exch_rate,
                    (
                      (pcf.px_base_measure):: numeric(38, 2) * currex.exch_rate
                    ) AS base_measure,
                    0 AS sales_qty,
                    0 AS gts,
                    0 AS gts_less_rtrn,
                    0 AS eff_val,
                    0 AS jgf_si_val,
                    0 AS pmt_terms_val,
                    0 AS datains_val,
                    0 AS exp_adj_val,
                    0 AS jgf_sd_val,
                    0 AS nts_val,
                    0 AS tot_ciw_val,
                    0 AS posted_cogs,
                    0 AS posted_con_free_goods_val,
                    0 AS posted_gp,
                    0 AS cogs,
                    0 AS gp,
                    0 AS futr_sls_qty,
                    0 AS futr_gts,
                    0 AS futr_ts,
                    0 AS futr_nts,
                    pcf.px_qty,
                    (
                      (pcf.px_gts):: numeric(38, 2) * currex.exch_rate
                    ) AS px_gts,
                    (
                      (pcf.px_gts_less_rtrns):: numeric(38, 2) * currex.exch_rate
                    ) AS px_gts_less_rtrn,
                    (
                      (pcf.px_eff_val):: numeric(38, 2) * currex.exch_rate
                    ) AS px_eff_val,
                    (
                      (pcf.px_jgf_si_val):: numeric(38, 2) * currex.exch_rate
                    ) AS px_jgf_si_val,
                    (
                      (pcf.px_pmt_terms_val):: numeric(38, 2) * currex.exch_rate
                    ) AS px_pmt_terms_val,
                    (
                      (pcf.px_datains_val):: numeric(38, 2) * currex.exch_rate
                    ) AS px_datains_val,
                    (
                      (pcf.px_exp_adj_val):: numeric(38, 2) * currex.exch_rate
                    ) AS px_exp_adj_val,
                    (
                      (pcf.px_jgf_sd_val):: numeric(38, 2) * currex.exch_rate
                    ) AS px_jgf_sd_val,
                    (
                      (
                        (pcf.px_gts_less_rtrns):: numeric(38, 2) * currex.exch_rate
                      ) - (
                        (pcf.px_ciw_tot):: numeric(38, 2) * currex.exch_rate
                      )
                    ) AS px_nts,
                    (
                      (pcf.px_ciw_tot):: numeric(38, 2) * currex.exch_rate
                    ) AS px_ciw_tot,
                    (
                      (pcf.px_cogs):: numeric(38, 2) * currex.exch_rate
                    ) AS px_cogs,
                    (
                      (
                        (
                          (pcf.px_gts_less_rtrns):: numeric(38, 2) * currex.exch_rate
                        ) - (
                          (pcf.px_ciw_tot):: numeric(38, 2) * currex.exch_rate
                        )
                      ) - (
                        (pcf.px_cogs):: numeric(38, 2) * currex.exch_rate
                      )
                    ) AS px_gp,
                    CASE WHEN (
                      etd.jj_mnth_id > (
                        (projprd.prev_jj_period):: numeric
                      ):: numeric(18, 0)
                    ) THEN pcf.px_qty ELSE (0):: bigint END AS projected_qty,
                    CASE WHEN (
                      etd.jj_mnth_id > (
                        (projprd.prev_jj_period):: numeric
                      ):: numeric(18, 0)
                    ) THEN (
                      (pcf.px_gts_less_rtrns):: numeric(38, 2) * currex.exch_rate
                    ) ELSE (
                      (0):: numeric
                    ):: numeric(18, 0) END AS projected_gts_less_rtrn,
                    CASE WHEN (
                      etd.jj_mnth_id > (
                        (projprd.prev_jj_period):: numeric
                      ):: numeric(18, 0)
                    ) THEN (
                      (pcf.px_eff_val):: numeric(38, 2) * currex.exch_rate
                    ) ELSE (
                      (0):: numeric
                    ):: numeric(18, 0) END AS projected_eff_val,
                    CASE WHEN (
                      etd.jj_mnth_id > (
                        (projprd.prev_jj_period):: numeric
                      ):: numeric(18, 0)
                    ) THEN (
                      (pcf.px_jgf_si_val):: numeric(38, 2) * currex.exch_rate
                    ) ELSE (
                      (0):: numeric
                    ):: numeric(18, 0) END AS projected_jgf_si_val,
                    CASE WHEN (
                      etd.jj_mnth_id > (
                        (projprd.prev_jj_period):: numeric
                      ):: numeric(18, 0)
                    ) THEN (
                      (pcf.px_pmt_terms_val):: numeric(38, 2) * currex.exch_rate
                    ) ELSE (
                      (0):: numeric
                    ):: numeric(18, 0) END AS projected_pmt_terms_val,
                    CASE WHEN (
                      etd.jj_mnth_id > (
                        (projprd.prev_jj_period):: numeric
                      ):: numeric(18, 0)
                    ) THEN (
                      (pcf.px_datains_val):: numeric(38, 2) * currex.exch_rate
                    ) ELSE (
                      (0):: numeric
                    ):: numeric(18, 0) END AS projected_datains_val,
                    CASE WHEN (
                      etd.jj_mnth_id > (
                        (projprd.prev_jj_period):: numeric
                      ):: numeric(18, 0)
                    ) THEN (
                      (pcf.px_exp_adj_val):: numeric(38, 2) * currex.exch_rate
                    ) ELSE (
                      (0):: numeric
                    ):: numeric(18, 0) END AS projected_exp_adj_val,
                    CASE WHEN (
                      etd.jj_mnth_id > (
                        (projprd.prev_jj_period):: numeric
                      ):: numeric(18, 0)
                    ) THEN (
                      (pcf.px_jgf_sd_val):: numeric(38, 2) * currex.exch_rate
                    ) ELSE (
                      (0):: numeric
                    ):: numeric(18, 0) END AS projected_jgf_sd_val,
                    CASE WHEN (
                      etd.jj_mnth_id > (
                        (projprd.prev_jj_period):: numeric
                      ):: numeric(18, 0)
                    ) THEN (
                      (pcf.px_ciw_tot):: numeric(38, 2) * currex.exch_rate
                    ) ELSE (
                      (0):: numeric
                    ):: numeric(18, 0) END AS projected_ciw_tot,
                    (
                      CASE WHEN (
                        etd.jj_mnth_id > (
                          (projprd.prev_jj_period):: numeric
                        ):: numeric(18, 0)
                      ) THEN (
                        (pcf.px_gts_less_rtrns):: numeric(38, 2) * currex.exch_rate
                      ) ELSE (
                        (0):: numeric
                      ):: numeric(18, 0) END - CASE WHEN (
                        etd.jj_mnth_id > (
                          (projprd.prev_jj_period):: numeric
                        ):: numeric(18, 0)
                      ) THEN (
                        (pcf.px_ciw_tot):: numeric(38, 2) * currex.exch_rate
                      ) ELSE (
                        (0):: numeric
                      ):: numeric(18, 0) END
                    ) AS projected_nts,
                    CASE WHEN (
                      etd.jj_mnth_id > (
                        (projprd.prev_jj_period):: numeric
                      ):: numeric(18, 0)
                    ) THEN (
                      (pcf.px_cogs):: numeric(38, 2) * currex.exch_rate
                    ) ELSE (
                      (0):: numeric
                    ):: numeric(18, 0) END AS projected_cogs_current,
                    (
                      (
                        CASE WHEN (
                          etd.jj_mnth_id > (
                            (projprd.prev_jj_period):: numeric
                          ):: numeric(18, 0)
                        ) THEN (
                          (pcf.px_gts_less_rtrns):: numeric(38, 2) * currex.exch_rate
                        ) ELSE (
                          (0):: numeric
                        ):: numeric(18, 0) END - CASE WHEN (
                          etd.jj_mnth_id > (
                            (projprd.prev_jj_period):: numeric
                          ):: numeric(18, 0)
                        ) THEN (
                          (pcf.px_ciw_tot):: numeric(38, 2) * currex.exch_rate
                        ) ELSE (
                          (0):: numeric
                        ):: numeric(18, 0) END
                      ) - CASE WHEN (
                        etd.jj_mnth_id > (
                          (projprd.prev_jj_period):: numeric
                        ):: numeric(18, 0)
                      ) THEN (
                        (pcf.px_cogs):: numeric(38, 2) * currex.exch_rate
                      ) ELSE (
                        (0):: numeric
                      ):: numeric(18, 0) END
                    ) AS projected_gp_current,
                    CASE WHEN (
                      etd.jj_mnth_id > (
                        (projprd.prev_jj_period):: numeric
                      ):: numeric(18, 0)
                    ) THEN (
                      (pcf.px_cogs):: numeric(38, 2) * currex.exch_rate
                    ) ELSE (
                      (0):: numeric
                    ):: numeric(18, 0) END AS projected_cogs_actual,
                    (
                      (
                        CASE WHEN (
                          etd.jj_mnth_id > (
                            (projprd.prev_jj_period):: numeric
                          ):: numeric(18, 0)
                        ) THEN (
                          (pcf.px_gts_less_rtrns):: numeric(38, 2) * currex.exch_rate
                        ) ELSE (
                          (0):: numeric
                        ):: numeric(18, 0) END - CASE WHEN (
                          etd.jj_mnth_id > (
                            (projprd.prev_jj_period):: numeric
                          ):: numeric(18, 0)
                        ) THEN (
                          (pcf.px_ciw_tot):: numeric(38, 2) * currex.exch_rate
                        ) ELSE (
                          (0):: numeric
                        ):: numeric(18, 0) END
                      ) - CASE WHEN (
                        etd.jj_mnth_id > (
                          (projprd.prev_jj_period):: numeric
                        ):: numeric(18, 0)
                      ) THEN (
                        (pcf.px_cogs):: numeric(38, 2) * currex.exch_rate
                      ) ELSE (
                        (0):: numeric
                      ):: numeric(18, 0) END
                    ) AS projected_gp_actual,
                    0 AS goal_qty,
                    0 AS goal_gts,
                    0 AS goal_eff_val,
                    0 AS goal_jgf_si_val,
                    0 AS goal_pmt_terms_val,
                    0 AS goal_datains_val,
                    0 AS goal_exp_adj_val,
                    0 AS goal_jgf_sd_val,
                    0 AS goal_ciw_tot,
                    0 AS goal_nts,
                    0 AS goal_cogs,
                    0 AS goal_gp,
                    0 AS bp_qty,
                    0 AS bp_gts,
                    0 AS bp_eff_val,
                    0 AS bp_jgf_si_val,
                    0 AS bp_pmt_terms_val,
                    0 AS bp_datains_val,
                    0 AS bp_exp_adj_val,
                    0 AS bp_jgf_sd_val,
                    0 AS bp_ciw_tot,
                    0 AS bp_nts,
                    0 AS bp_cogs,
                    0 AS bp_gp,
                    0 AS ju_qty,
                    0 AS ju_gts,
                    0 AS ju_eff_val,
                    0 AS ju_jgf_si_val,
                    0 AS ju_pmt_terms_val,
                    0 AS ju_datains_val,
                    0 AS ju_exp_adj_val,
                    0 AS ju_jgf_sd_val,
                    0 AS ju_ciw_tot,
                    0 AS ju_nts,
                    0 AS ju_cogs,
                    0 AS ju_gp,
                    0 AS nu_qty,
                    0 AS nu_gts,
                    0 AS nu_eff_val,
                    0 AS nu_jgf_si_val,
                    0 AS nu_pmt_terms_val,
                    0 AS nu_datains_val,
                    0 AS nu_exp_adj_val,
                    0 AS nu_jgf_sd_val,
                    0 AS nu_ciw_tot,
                    0 AS nu_nts,
                    0 AS nu_cogs,
                    0 AS nu_gp,
                    0 AS bme_trans,
                    0 AS incr_gts,
                    pcf.case_qty AS px_case_qty,
                    (
                      (pcf.tot_planspend):: numeric(38, 2) * currex.exch_rate
                    ) AS px_tot_planspend,
                    (
                      (pcf.tot_paid):: numeric(38, 2) * currex.exch_rate
                    ) AS px_tot_paid,
                    (
                      (pcf.committed_spend):: numeric(38, 2) * currex.exch_rate
                    ) AS px_committed_spend,
                    b.gcph_franchise,
                    b.gcph_brand,
                    b.gcph_subbrand,
                    b.gcph_variant,
                    b.gcph_needstate,
                    b.gcph_category,
                    b.gcph_subcategory,
                    b.gcph_segment,
                    b.gcph_subsegment
                  FROM
                    (
                      SELECT
                        DISTINCT edw_time_dim.jj_mnth,
                        edw_time_dim.jj_mnth_shrt,
                        edw_time_dim.jj_mnth_long,
                        edw_time_dim.jj_qrtr,
                        edw_time_dim.jj_year,
                        edw_time_dim.jj_mnth_id,
                        edw_time_dim.jj_mnth_tot
                      FROM
                        edw_time_dim
                    ) etd,
                    edw_sapbw_plan_lkp spl,
                    (
                      SELECT
                        vw_jjbr_curr_exch_dim.rate_type,
                        vw_jjbr_curr_exch_dim.from_ccy,
                        vw_jjbr_curr_exch_dim.to_ccy,
                        vw_jjbr_curr_exch_dim.jj_mnth_id,
                        vw_jjbr_curr_exch_dim.exch_rate
                      FROM
                        vw_jjbr_curr_exch_dim
                      WHERE
                        (
                          (
                            vw_jjbr_curr_exch_dim.exch_rate = (
                              (
                                (1):: numeric
                              ):: numeric(18, 0)
                            ):: numeric(15, 5)
                          )
                          AND (
                            (vw_jjbr_curr_exch_dim.from_ccy):: text = ('AUD' :: character varying):: text
                          )
                        )
                      UNION ALL
                      SELECT
                        vw_bwar_curr_exch_dim.rate_type,
                        vw_bwar_curr_exch_dim.from_ccy,
                        vw_bwar_curr_exch_dim.to_ccy,
                        vw_bwar_curr_exch_dim.jj_mnth_id,
                        vw_bwar_curr_exch_dim.exch_rate
                      FROM
                        vw_bwar_curr_exch_dim
                    ) currex,
                    (
                      SELECT
                        t1.jj_mnth_id AS elpsd_jj_period,
                        (
                          (t1.jj_mnth_day / t1.jj_mnth_tot) * (
                            (100):: numeric
                          ):: numeric(18, 0)
                        ) AS prcnt_elpsd
                      FROM
                        edw_time_dim t1
                      WHERE
                        (
                          t1.cal_date::date =
                            (
                              current_timestamp()::date+1
                            )
                        )
                    ) prelap,
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
                        ):: integer AS prev_jj_period,
                        t1.jj_mnth_id AS curr_jj_period
                      FROM
                        edw_time_dim t1
                      WHERE
                        (
                          t1.cal_date::date =
                            (
                              current_timestamp()::date + (1):: bigint
                            )

                        )
                    ) projprd,
                    (
                      SELECT
                        derived_table4.jj_mnth_id,
                        "max"(derived_table4.rownum) AS total_wks
                      FROM
                        (
                          SELECT
                            derived_table3.jj_mnth_id,
                            derived_table3.jj_wk,
                            row_number() OVER(
                              PARTITION BY derived_table3.jj_mnth_id
                              ORDER BY
                                derived_table3.jj_wk
                            ) AS rownum
                          FROM
                            (
                              SELECT
                                DISTINCT edw_time_dim.jj_mnth_id,
                                edw_time_dim.jj_wk
                              FROM
                                edw_time_dim
                            ) derived_table3
                        ) derived_table4
                      GROUP BY
                        derived_table4.jj_mnth_id
                    ) jj_tot_wks,
                    (
                      (
                        (
                          (
                            (
                              px_combined_ciw_fact pcf
                              LEFT JOIN vw_customer_dim vcd ON (
                                (
                                  (pcf.cust_no):: text = ltrim(
                                    (vcd.cust_no):: text,
                                    ('0' :: character varying):: text
                                  )
                                )
                              )
                            )
                            LEFT JOIN edw_material_dim vmd ON (
                              (
                                (pcf.matl_id):: text = ltrim(
                                  (vmd.matl_id):: text,
                                  ('0' :: character varying):: text
                                )
                              )
                            )
                          )
                          LEFT JOIN (
                            vw_apo_parent_child_dim vapcd
                            LEFT JOIN (
                              SELECT
                                DISTINCT vw_apo_parent_child_dim.master_code,
                                vw_apo_parent_child_dim.parent_matl_desc
                              FROM
                                vw_apo_parent_child_dim
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
                                vw_apo_parent_child_dim
                              WHERE
                                (
                                  NOT (
                                    vw_apo_parent_child_dim.master_code IN (
                                      SELECT
                                        DISTINCT vw_apo_parent_child_dim.master_code
                                      FROM
                                        vw_apo_parent_child_dim
                                      WHERE
                                        (
                                          (vw_apo_parent_child_dim.cmp_id):: text = (
                                            (7470):: character varying
                                          ):: text
                                        )
                                    )
                                  )
                                )
                            ) mstrcd ON (
                              (
                                (vapcd.master_code):: text = (mstrcd.master_code):: text
                              )
                            )
                          ) ON (
                            (
                              (
                                (pcf.cmp_id):: text = (vapcd.cmp_id):: text
                              )
                              AND (
                                (pcf.matl_id):: text = ltrim(
                                  (vapcd.matl_id):: text,
                                  ('0' :: character varying):: text
                                )
                              )
                            )
                          )
                        )
                        LEFT JOIN edw_material_divest emd ON (
                          (
                            (
                              (pcf.cmp_id):: text = (
                                (emd.cmp_id):: character varying
                              ):: text
                            )
                            AND (
                              (pcf.matl_id):: text = (
                                (emd.divested_sku):: character varying
                              ):: text
                            )
                          )
                        )
                      )
                      LEFT JOIN (
                        SELECT
                          edw_gch_producthierarchy.materialnumber,
                          edw_gch_producthierarchy.gcph_franchise,
                          edw_gch_producthierarchy.gcph_brand,
                          edw_gch_producthierarchy.gcph_subbrand,
                          edw_gch_producthierarchy.gcph_variant,
                          edw_gch_producthierarchy.gcph_needstate,
                          edw_gch_producthierarchy.gcph_category,
                          edw_gch_producthierarchy.gcph_subcategory,
                          edw_gch_producthierarchy.gcph_segment,
                          edw_gch_producthierarchy.gcph_subsegment
                        FROM
                          edw_gch_producthierarchy
                        WHERE
                          (
                            (
                              ltrim(
                                (
                                  edw_gch_producthierarchy.materialnumber
                                ):: text,
                                (
                                  (0):: character varying
                                ):: text
                              ) <> ('' :: character varying):: text
                            )
                            AND (
                              (
                                edw_gch_producthierarchy."region"
                              ):: text = ('APAC' :: character varying):: text
                            )
                          )
                      ) b ON (
                        (
                          ltrim(
                            (pcf.matl_id):: text,
                            (
                              (0):: character varying
                            ):: text
                          ) = ltrim(
                            (b.materialnumber):: text,
                            (
                              (0):: character varying
                            ):: text
                          )
                        )
                      )
                    )
                  WHERE
                    (
                      (
                        (
                          (
                            (pcf.px_jj_mnth = etd.jj_mnth_id)
                            AND (
                              (pcf.local_ccy):: text = (currex.from_ccy):: text
                            )
                          )
                          AND (
                            etd.jj_mnth_id = currex.jj_mnth_id
                          )
                        )
                        AND (
                          jj_tot_wks.jj_mnth_id = etd.jj_mnth_id
                        )
                      )
                      AND (
                        (spl.sls_grp_cd):: text = (vcd.sales_grp_cd):: text
                      )
                    )
                )
                UNION ALL
                SELECT
                  'FILE' :: character varying AS pac_source_type,
                  'CUST_PLAN' :: character varying AS pac_subsource_type,
                  etd.jj_mnth_id AS jj_period,
                  jj_tot_wks.total_wks AS jj_wk,
                  etd.jj_mnth,
                  etd.jj_mnth_shrt,
                  etd.jj_mnth_long,
                  etd.jj_qrtr,
                  etd.jj_year,
                  etd.jj_mnth_tot,
                  prelap.prcnt_elpsd,
                  prelap.elpsd_jj_period,
                  NULL :: character varying AS cust_no,
                  vcd.cmp_id,
                  vcd.channel_cd,
                  vcd.channel_desc,
                  NULL :: character varying AS ctry_key,
                  vcd.country,
                  NULL :: character varying AS state_cd,
                  NULL :: character varying AS post_cd,
                  NULL :: character varying AS cust_suburb,
                  NULL :: character varying AS cust_nm,
                  vcd.sales_office_cd,
                  vcd.sales_office_desc,
                  vcd.sales_grp_cd,
                  vcd.sales_grp_desc,
                  NULL :: character varying AS mercia_ref,
                  spl.pln_chnl_cd AS pln_cnnl_cd,
                  spl.pln_chnl_desc,
                  NULL :: character varying AS cust_curr_cd,
                  NULL :: character varying AS matl_id,
                  NULL :: character varying AS matl_desc,
                  NULL :: character varying AS master_code,
                  NULL :: character varying AS parent_matl_id,
                  NULL :: character varying AS parent_matl_desc,
                  NULL :: character varying AS mega_brnd_cd,
                  NULL :: character varying AS mega_brnd_desc,
                  NULL :: character varying AS brnd_cd,
                  NULL :: character varying AS brnd_desc,
                  NULL :: character varying AS base_prod_cd,
                  NULL :: character varying AS base_prod_desc,
                  NULL :: character varying AS variant_cd,
                  NULL :: character varying AS variant_desc,
                  vmd.fran_cd,
                  vmd.fran_desc,
                  vmd.grp_fran_cd,
                  vmd.grp_fran_desc,
                  NULL :: character varying AS matl_type_cd,
                  NULL :: character varying AS matl_type_desc,
                  vmd.prod_fran_cd,
                  vmd.prod_fran_desc,
                  NULL :: character varying AS prod_hier_cd,
                  NULL :: character varying AS prod_hier_desc,
                  vmd.prod_mjr_cd,
                  vmd.prod_mjr_desc,
                  NULL :: character varying AS prod_mnr_cd,
                  NULL :: character varying AS prod_mnr_desc,
                  NULL :: character varying AS mercia_plan,
                  NULL :: character varying AS putup_cd,
                  NULL :: character varying AS putup_desc,
                  NULL :: character varying AS bar_cd,
                  NULL :: character varying AS prft_ctr,
                  NULL :: character varying AS divest_flag,
                  NULL :: character varying AS key_measure,
                  NULL :: character varying AS ciw_ctgry,
                  NULL :: character varying AS ciw_accnt_grp,
                  NULL :: character varying AS px_gl_trans_desc,
                  NULL :: character varying AS px_measure,
                  NULL :: character varying AS px_bucket,
                  NULL :: character varying AS sap_accnt,
                  NULL :: character varying AS sap_accnt_desc,
                  ecpf.local_ccy AS local_curr_cd,
                  currex.to_ccy,
                  currex.exch_rate,
                  0 AS base_measure,
                  0 AS sales_qty,
                  0 AS gts,
                  0 AS gts_less_rtrn,
                  0 AS eff_val,
                  0 AS jgf_si_val,
                  0 AS pmt_terms_val,
                  0 AS datains_val,
                  0 AS exp_adj_val,
                  0 AS jgf_sd_val,
                  0 AS nts_val,
                  0 AS tot_ciw_val,
                  0 AS posted_cogs,
                  0 AS posted_con_free_goods_val,
                  0 AS posted_gp,
                  0 AS cogs,
                  0 AS gp,
                  0 AS futr_sls_qty,
                  0 AS futr_gts,
                  0 AS futr_ts,
                  0 AS futr_nts,
                  0 AS px_qty,
                  0 AS px_gts,
                  0 AS px_gts_less_rtrn,
                  0 AS px_eff_val,
                  0 AS px_jgf_si_val,
                  0 AS px_pmt_terms_val,
                  0 AS px_datains_val,
                  0 AS px_exp_adj_val,
                  0 AS px_jgf_sd_val,
                  0 AS px_nts,
                  0 AS px_ciw_tot,
                  0 AS px_cogs,
                  0 AS px_gp,
                  0 AS projected_qty,
                  0 AS projected_gts_less_rtrn,
                  0 AS projected_eff_val,
                  0 AS projected_jgf_si_val,
                  0 AS projected_pmt_terms_val,
                  0 AS projected_datains_val,
                  0 AS projected_exp_adj_val,
                  0 AS projected_jgf_sd_val,
                  0 AS projected_ciw_tot,
                  0 AS projected_nts,
                  0 AS projected_cogs_current,
                  0 AS projected_gp_current,
                  0 AS projected_cogs_actual,
                  0 AS projected_gp_actual,
                  ecpf.goal_qty,
                  (
                    (ecpf.goal_gts):: numeric(38, 2) * currex.exch_rate
                  ) AS goal_gts,
                  (
                    (ecpf.goal_eff_val):: numeric(38, 2) * currex.exch_rate
                  ) AS goal_eff_val,
                  (
                    (ecpf.goal_jgf_si_val):: numeric(38, 2) * currex.exch_rate
                  ) AS goal_jgf_si_val,
                  (
                    (ecpf.goal_pmt_terms_val):: numeric(38, 2) * currex.exch_rate
                  ) AS goal_pmt_terms_val,
                  (
                    (ecpf.goal_datains_val):: numeric(38, 2) * currex.exch_rate
                  ) AS goal_datains_val,
                  (
                    (ecpf.goal_exp_adj_val):: numeric(38, 2) * currex.exch_rate
                  ) AS goal_exp_adj_val,
                  (
                    (ecpf.goal_jgf_sd_val):: numeric(38, 2) * currex.exch_rate
                  ) AS goal_jgf_sd_val,
                  (
                    (ecpf.goal_ciw_tot):: numeric(38, 2) * currex.exch_rate
                  ) AS goal_ciw_tot,
                  (
                    (
                      (ecpf.goal_gts):: numeric(38, 2) * currex.exch_rate
                    ) - (
                      (ecpf.goal_ciw_tot):: numeric(38, 2) * currex.exch_rate
                    )
                  ) AS goal_nts,
                  (
                    (ecpf.goal_cogs):: numeric(38, 2) * currex.exch_rate
                  ) AS goal_cogs,
                  (
                    (ecpf.goal_gp):: numeric(38, 2) * currex.exch_rate
                  ) AS goal_gp,
                  0 AS bp_qty,
                  0 AS bp_gts,
                  0 AS bp_eff_val,
                  0 AS bp_jgf_si_val,
                  0 AS bp_pmt_terms_val,
                  0 AS bp_datains_val,
                  0 AS bp_exp_adj_val,
                  0 AS bp_jgf_sd_val,
                  0 AS bp_ciw_tot,
                  0 AS bp_nts,
                  0 AS bp_cogs,
                  0 AS bp_gp,
                  0 AS ju_qty,
                  0 AS ju_gts,
                  0 AS ju_eff_val,
                  0 AS ju_jgf_si_val,
                  0 AS ju_pmt_terms_val,
                  0 AS ju_datains_val,
                  0 AS ju_exp_adj_val,
                  0 AS ju_jgf_sd_val,
                  0 AS ju_ciw_tot,
                  0 AS ju_nts,
                  0 AS ju_cogs,
                  0 AS ju_gp,
                  0 AS nu_qty,
                  0 AS nu_gts,
                  0 AS nu_eff_val,
                  0 AS nu_jgf_si_val,
                  0 AS nu_pmt_terms_val,
                  0 AS nu_datains_val,
                  0 AS nu_exp_adj_val,
                  0 AS nu_jgf_sd_val,
                  0 AS nu_ciw_tot,
                  0 AS nu_nts,
                  0 AS nu_cogs,
                  0 AS nu_gp,
                  0 AS bme_trans,
                  0 AS incr_gts,
                  0 AS px_case_qty,
                  0 AS px_tot_planspend,
                  0 AS px_tot_paid,
                  0 AS px_committed_spend,
                  NULL :: character varying AS gcph_franchise,
                  NULL :: character varying AS gcph_brand,
                  NULL :: character varying AS gcph_subbrand,
                  NULL :: character varying AS gcph_variant,
                  NULL :: character varying AS gcph_needstate,
                  NULL :: character varying AS gcph_category,
                  NULL :: character varying AS gcph_subcategory,
                  NULL :: character varying AS gcph_segment,
                  NULL :: character varying AS gcph_subsegment
                FROM
                  edw_sapbw_plan_lkp spl,
                  (
                    SELECT
                      vw_jjbr_curr_exch_dim.rate_type,
                      vw_jjbr_curr_exch_dim.from_ccy,
                      vw_jjbr_curr_exch_dim.to_ccy,
                      vw_jjbr_curr_exch_dim.jj_mnth_id,
                      vw_jjbr_curr_exch_dim.exch_rate
                    FROM
                      vw_jjbr_curr_exch_dim
                    WHERE
                      (
                        (
                          vw_jjbr_curr_exch_dim.exch_rate = (
                            (
                              (1):: numeric
                            ):: numeric(18, 0)
                          ):: numeric(15, 5)
                        )
                        AND (
                          (vw_jjbr_curr_exch_dim.from_ccy):: text = ('AUD' :: character varying):: text
                        )
                      )
                    UNION ALL
                    SELECT
                      vw_bwar_curr_exch_dim.rate_type,
                      vw_bwar_curr_exch_dim.from_ccy,
                      vw_bwar_curr_exch_dim.to_ccy,
                      vw_bwar_curr_exch_dim.jj_mnth_id,
                      vw_bwar_curr_exch_dim.exch_rate
                    FROM
                      vw_bwar_curr_exch_dim
                  ) currex,
                  (
                    SELECT
                      t1.jj_mnth_id AS elpsd_jj_period,
                      (
                        (t1.jj_mnth_day / t1.jj_mnth_tot) * (
                          (100):: numeric
                        ):: numeric(18, 0)
                      ) AS prcnt_elpsd
                    FROM
                      edw_time_dim t1
                    WHERE
                      (
                        t1.cal_date::date =
                          (
                            current_timestamp()::date + (1):: bigint
                          )

                      )
                  ) prelap,
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
                      ):: integer AS prev_jj_period,
                      t1.jj_mnth_id AS curr_jj_period
                    FROM
                      edw_time_dim t1
                    WHERE
                      (
                        t1.cal_date::date =
                          (
                            current_timestamp()::date + (1):: bigint
                          )

                      )
                  ) projprd,
                  (
                    SELECT
                      derived_table6.jj_mnth_id,
                      "max"(derived_table6.rownum) AS total_wks
                    FROM
                      (
                        SELECT
                          derived_table5.jj_mnth_id,
                          derived_table5.jj_wk,
                          row_number() OVER(
                            PARTITION BY derived_table5.jj_mnth_id
                            ORDER BY
                              derived_table5.jj_wk
                          ) AS rownum
                        FROM
                          (
                            SELECT
                              DISTINCT edw_time_dim.jj_mnth_id,
                              edw_time_dim.jj_wk
                            FROM
                              edw_time_dim
                          ) derived_table5
                      ) derived_table6
                    GROUP BY
                      derived_table6.jj_mnth_id
                  ) jj_tot_wks,
                  (
                    (
                      (
                        edw_cust_ciw_plan_fact ecpf
                        LEFT JOIN (
                          SELECT
                            DISTINCT edw_time_dim.jj_mnth,
                            edw_time_dim.jj_mnth_shrt,
                            edw_time_dim.jj_mnth_long,
                            edw_time_dim.jj_qrtr,
                            edw_time_dim.jj_year,
                            edw_time_dim.jj_mnth_id,
                            edw_time_dim.jj_mnth_tot
                          FROM
                            edw_time_dim
                        ) etd ON (
                          (
                            (ecpf.time_period):: numeric(18, 0) = etd.jj_mnth_id
                          )
                        )
                      )
                      LEFT JOIN (
                        SELECT
                          DISTINCT vw_customer_dim.cmp_id,
                          vw_customer_dim.channel_cd,
                          vw_customer_dim.channel_desc,
                          vw_customer_dim.sales_office_cd,
                          vw_customer_dim.sales_office_desc,
                          vw_customer_dim.sales_grp_cd,
                          vw_customer_dim.sales_grp_desc,
                          vw_customer_dim.country
                        FROM
                          vw_customer_dim
                      ) vcd ON (
                        (
                          (ecpf.sales_grp_cd):: text = (vcd.sales_grp_cd):: text
                        )
                      )
                    )
                    LEFT JOIN (
                      SELECT
                        DISTINCT edw_material_dim.fran_cd,
                        edw_material_dim.fran_desc,
                        edw_material_dim.grp_fran_cd,
                        edw_material_dim.grp_fran_desc,
                        edw_material_dim.prod_fran_cd,
                        edw_material_dim.prod_fran_desc,
                        edw_material_dim.prod_mjr_cd,
                        edw_material_dim.prod_mjr_desc
                      FROM
                        edw_material_dim
                    ) vmd ON (
                      (
                        (ecpf.prod_mjr_cd):: text = (vmd.prod_mjr_cd):: text
                      )
                    )
                  )
                WHERE
                  (
                    (
                      (
                        (
                          (ecpf.local_ccy):: text = (currex.from_ccy):: text
                        )
                        AND (
                          etd.jj_mnth_id = currex.jj_mnth_id
                        )
                      )
                      AND (
                        jj_tot_wks.jj_mnth_id = etd.jj_mnth_id
                      )
                    )
                    AND (
                      (spl.sls_grp_cd):: text = (vcd.sales_grp_cd):: text
                    )
                  )
              )
              UNION ALL
              SELECT
                'FILE' :: character varying AS pac_source_type,
                'BME_TRANSFER' :: character varying AS pac_subsource_type,
                etd.jj_mnth_id AS jj_period,
                jj_tot_wks.total_wks AS jj_wk,
                etd.jj_mnth,
                etd.jj_mnth_shrt,
                etd.jj_mnth_long,
                etd.jj_qrtr,
                etd.jj_year,
                etd.jj_mnth_tot,
                prelap.prcnt_elpsd,
                prelap.elpsd_jj_period,
                NULL :: character varying AS cust_no,
                vcd.cmp_id,
                vcd.channel_cd,
                vcd.channel_desc,
                NULL :: character varying AS ctry_key,
                vcd.country,
                NULL :: character varying AS state_cd,
                NULL :: character varying AS post_cd,
                NULL :: character varying AS cust_suburb,
                NULL :: character varying AS cust_nm,
                vcd.sales_office_cd,
                vcd.sales_office_desc,
                vcd.sales_grp_cd,
                vcd.sales_grp_desc,
                NULL :: character varying AS mercia_ref,
                spl.pln_chnl_cd AS pln_cnnl_cd,
                spl.pln_chnl_desc,
                NULL :: character varying AS cust_curr_cd,
                NULL :: character varying AS matl_id,
                NULL :: character varying AS matl_desc,
                NULL :: character varying AS master_code,
                NULL :: character varying AS parent_matl_id,
                NULL :: character varying AS parent_matl_desc,
                NULL :: character varying AS mega_brnd_cd,
                NULL :: character varying AS mega_brnd_desc,
                NULL :: character varying AS brnd_cd,
                NULL :: character varying AS brnd_desc,
                NULL :: character varying AS base_prod_cd,
                NULL :: character varying AS base_prod_desc,
                NULL :: character varying AS variant_cd,
                NULL :: character varying AS variant_desc,
                vmd.fran_cd,
                vmd.fran_desc,
                vmd.grp_fran_cd,
                vmd.grp_fran_desc,
                NULL :: character varying AS matl_type_cd,
                NULL :: character varying AS matl_type_desc,
                vmd.prod_fran_cd,
                vmd.prod_fran_desc,
                NULL :: character varying AS prod_hier_cd,
                NULL :: character varying AS prod_hier_desc,
                vmd.prod_mjr_cd,
                vmd.prod_mjr_desc,
                NULL :: character varying AS prod_mnr_cd,
                NULL :: character varying AS prod_mnr_desc,
                NULL :: character varying AS mercia_plan,
                NULL :: character varying AS putup_cd,
                NULL :: character varying AS putup_desc,
                NULL :: character varying AS bar_cd,
                NULL :: character varying AS prft_ctr,
                NULL :: character varying AS divest_flag,
                NULL :: character varying AS key_measure,
                NULL :: character varying AS ciw_ctgry,
                NULL :: character varying AS ciw_accnt_grp,
                NULL :: character varying AS px_gl_trans_desc,
                NULL :: character varying AS px_measure,
                NULL :: character varying AS px_bucket,
                NULL :: character varying AS sap_accnt,
                NULL :: character varying AS sap_accnt_desc,
                CASE WHEN (
                  (vcd.cmp_id):: text = (
                    (7470):: character varying
                  ):: text
                ) THEN 'AUD' :: character varying WHEN (
                  (vcd.cmp_id):: text = (
                    (8361):: character varying
                  ):: text
                ) THEN 'NZD' :: character varying ELSE 'AUD' :: character varying END AS local_curr_cd,
                currex.to_ccy,
                currex.exch_rate,
                0 AS base_measure,
                0 AS sales_qty,
                0 AS gts,
                0 AS gts_less_rtrn,
                0 AS eff_val,
                0 AS jgf_si_val,
                0 AS pmt_terms_val,
                0 AS datains_val,
                0 AS exp_adj_val,
                0 AS jgf_sd_val,
                0 AS nts_val,
                0 AS tot_ciw_val,
                0 AS posted_cogs,
                0 AS posted_con_free_goods_val,
                0 AS posted_gp,
                0 AS cogs,
                0 AS gp,
                0 AS futr_sls_qty,
                0 AS futr_gts,
                0 AS futr_ts,
                0 AS futr_nts,
                0 AS px_qty,
                0 AS px_gts,
                0 AS px_gts_less_rtrn,
                0 AS px_eff_val,
                0 AS px_jgf_si_val,
                0 AS px_pmt_terms_val,
                0 AS px_datains_val,
                0 AS px_exp_adj_val,
                0 AS px_jgf_sd_val,
                0 AS px_nts,
                0 AS px_ciw_tot,
                0 AS px_cogs,
                0 AS px_gp,
                0 AS projected_qty,
                0 AS projected_gts_less_rtrn,
                0 AS projected_eff_val,
                0 AS projected_jgf_si_val,
                0 AS projected_pmt_terms_val,
                0 AS projected_datains_val,
                0 AS projected_exp_adj_val,
                0 AS projected_jgf_sd_val,
                0 AS projected_ciw_tot,
                0 AS projected_nts,
                0 AS projected_cogs_current,
                0 AS projected_gp_current,
                0 AS projected_cogs_actual,
                0 AS projected_gp_actual,
                0 AS goal_qty,
                0 AS goal_gts,
                0 AS goal_eff_val,
                0 AS goal_jgf_si_val,
                0 AS goal_pmt_terms_val,
                0 AS goal_datains_val,
                0 AS goal_exp_adj_val,
                0 AS goal_jgf_sd_val,
                0 AS goal_ciw_tot,
                0 AS goal_nts,
                0 AS goal_cogs,
                0 AS goal_gp,
                0 AS bp_qty,
                0 AS bp_gts,
                0 AS bp_eff_val,
                0 AS bp_jgf_si_val,
                0 AS bp_pmt_terms_val,
                0 AS bp_datains_val,
                0 AS bp_exp_adj_val,
                0 AS bp_jgf_sd_val,
                0 AS bp_ciw_tot,
                0 AS bp_nts,
                0 AS bp_cogs,
                0 AS bp_gp,
                0 AS ju_qty,
                0 AS ju_gts,
                0 AS ju_eff_val,
                0 AS ju_jgf_si_val,
                0 AS ju_pmt_terms_val,
                0 AS ju_datains_val,
                0 AS ju_exp_adj_val,
                0 AS ju_jgf_sd_val,
                0 AS ju_ciw_tot,
                0 AS ju_nts,
                0 AS ju_cogs,
                0 AS ju_gp,
                0 AS nu_qty,
                0 AS nu_gts,
                0 AS nu_eff_val,
                0 AS nu_jgf_si_val,
                0 AS nu_pmt_terms_val,
                0 AS nu_datains_val,
                0 AS nu_exp_adj_val,
                0 AS nu_jgf_sd_val,
                0 AS nu_ciw_tot,
                0 AS nu_nts,
                0 AS nu_cogs,
                0 AS nu_gp,
                (
                  (ebt.bme_transfer):: numeric(38, 2) * currex.exch_rate
                ) AS bme_trans,
                (
                  (ebt.incremental_gts):: numeric(38, 2) * currex.exch_rate
                ) AS incr_gts,
                0 AS px_case_qty,
                0 AS px_tot_planspend,
                0 AS px_tot_paid,
                0 AS px_committed_spend,
                NULL :: character varying AS gcph_franchise,
                NULL :: character varying AS gcph_brand,
                NULL :: character varying AS gcph_subbrand,
                NULL :: character varying AS gcph_variant,
                NULL :: character varying AS gcph_needstate,
                NULL :: character varying AS gcph_category,
                NULL :: character varying AS gcph_subcategory,
                NULL :: character varying AS gcph_segment,
                NULL :: character varying AS gcph_subsegment
              FROM
                edw_sapbw_plan_lkp spl,
                (
                  SELECT
                    vw_jjbr_curr_exch_dim.rate_type,
                    vw_jjbr_curr_exch_dim.from_ccy,
                    vw_jjbr_curr_exch_dim.to_ccy,
                    vw_jjbr_curr_exch_dim.jj_mnth_id,
                    vw_jjbr_curr_exch_dim.exch_rate
                  FROM
                    vw_jjbr_curr_exch_dim
                  WHERE
                    (
                      (
                        vw_jjbr_curr_exch_dim.exch_rate = (
                          (
                            (1):: numeric
                          ):: numeric(18, 0)
                        ):: numeric(15, 5)
                      )
                      AND (
                        (vw_jjbr_curr_exch_dim.from_ccy):: text = ('AUD' :: character varying):: text
                      )
                    )
                  UNION ALL
                  SELECT
                    vw_bwar_curr_exch_dim.rate_type,
                    vw_bwar_curr_exch_dim.from_ccy,
                    vw_bwar_curr_exch_dim.to_ccy,
                    vw_bwar_curr_exch_dim.jj_mnth_id,
                    vw_bwar_curr_exch_dim.exch_rate
                  FROM
                    vw_bwar_curr_exch_dim
                ) currex,
                (
                  SELECT
                    t1.jj_mnth_id AS elpsd_jj_period,
                    (
                      (t1.jj_mnth_day / t1.jj_mnth_tot) * (
                        (100):: numeric
                      ):: numeric(18, 0)
                    ) AS prcnt_elpsd
                  FROM
                    edw_time_dim t1
                  WHERE
                    (
                      t1.cal_date::date =
                        (
                          current_timestamp()::date + (1):: bigint
                        )

                    )
                ) prelap,
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
                    ):: integer AS prev_jj_period,
                    t1.jj_mnth_id AS curr_jj_period
                  FROM
                    edw_time_dim t1
                  WHERE
                    (
                      t1.cal_date::date =
                        (
                          current_timestamp()::date + (1):: bigint
                        )

                    )
                ) projprd,
                (
                  SELECT
                    derived_table8.jj_mnth_id,
                    "max"(derived_table8.rownum) AS total_wks
                  FROM
                    (
                      SELECT
                        derived_table7.jj_mnth_id,
                        derived_table7.jj_wk,
                        row_number() OVER(
                          PARTITION BY derived_table7.jj_mnth_id
                          ORDER BY
                            derived_table7.jj_wk
                        ) AS rownum
                      FROM
                        (
                          SELECT
                            DISTINCT edw_time_dim.jj_mnth_id,
                            edw_time_dim.jj_wk
                          FROM
                            edw_time_dim
                        ) derived_table7
                    ) derived_table8
                  GROUP BY
                    derived_table8.jj_mnth_id
                ) jj_tot_wks,
                (
                  (
                    (
                      edw_bme_transfers ebt
                      LEFT JOIN (
                        SELECT
                          DISTINCT edw_time_dim.jj_mnth,
                          edw_time_dim.jj_mnth_shrt,
                          edw_time_dim.jj_mnth_long,
                          edw_time_dim.jj_qrtr,
                          edw_time_dim.jj_year,
                          edw_time_dim.jj_mnth_id,
                          edw_time_dim.jj_mnth_tot
                        FROM
                          edw_time_dim
                      ) etd ON (
                        (ebt.year = etd.jj_mnth_id)
                      )
                    )
                    LEFT JOIN (
                      SELECT
                        DISTINCT vw_customer_dim.cmp_id,
                        vw_customer_dim.channel_cd,
                        vw_customer_dim.channel_desc,
                        vw_customer_dim.sales_office_cd,
                        vw_customer_dim.sales_office_desc,
                        vw_customer_dim.sales_grp_cd,
                        vw_customer_dim.sales_grp_desc,
                        vw_customer_dim.country
                      FROM
                        vw_customer_dim
                    ) vcd ON (
                      (
                        (ebt.sales_group_cd):: text = (vcd.sales_grp_cd):: text
                      )
                    )
                  )
                  LEFT JOIN (
                    SELECT
                      DISTINCT edw_material_dim.fran_cd,
                      edw_material_dim.fran_desc,
                      edw_material_dim.grp_fran_cd,
                      edw_material_dim.grp_fran_desc,
                      edw_material_dim.prod_fran_cd,
                      edw_material_dim.prod_fran_desc,
                      edw_material_dim.prod_mjr_cd,
                      edw_material_dim.prod_mjr_desc
                    FROM
                      edw_material_dim
                  ) vmd ON (
                    (
                      (ebt.prod_mjr_cd):: text = (vmd.prod_mjr_cd):: text
                    )
                  )
                )
              WHERE
                (
                  (
                    (
                      (
                        (
                          CASE WHEN (
                            (vcd.cmp_id):: text = (
                              (7470):: character varying
                            ):: text
                          ) THEN 'AUD' :: character varying WHEN (
                            (vcd.cmp_id):: text = (
                              (8361):: character varying
                            ):: text
                          ) THEN 'NZD' :: character varying ELSE 'AUD' :: character varying END
                        ):: text = (currex.from_ccy):: text
                      )
                      AND (
                        etd.jj_mnth_id = currex.jj_mnth_id
                      )
                    )
                    AND (
                      jj_tot_wks.jj_mnth_id = etd.jj_mnth_id
                    )
                  )
                  AND (
                    (spl.sls_grp_cd):: text = (vcd.sales_grp_cd):: text
                  )
                )
            )
            UNION ALL
            SELECT
              'SAPBW' :: character varying AS pac_source_type,
              'SAPBW_FUTURES' :: character varying AS pac_subsource_type,
              etd.jj_mnth_id AS jj_period,
              jj_tot_wks.total_wks AS jj_wk,
              etd.jj_mnth,
              etd.jj_mnth_shrt,
              etd.jj_mnth_long,
              etd.jj_qrtr,
              etd.jj_year,
              etd.jj_mnth_tot,
              prelap.prcnt_elpsd,
              prelap.elpsd_jj_period,
              (
                ltrim(
                  (vcd.cust_no):: text,
                  (
                    (0):: character varying
                  ):: text
                )
              ):: character varying AS cust_no,
              vcd.cmp_id,
              vcd.channel_cd,
              vcd.channel_desc,
              (vcd.ctry_key):: character varying AS ctry_key,
              vcd.country,
              (vcd.state_cd):: character varying AS state_cd,
              (vcd.post_cd):: character varying AS post_cd,
              (vcd.cust_suburb):: character varying AS cust_suburb,
              (vcd.cust_nm):: character varying AS cust_nm,
              vcd.sales_office_cd,
              vcd.sales_office_desc,
              vcd.sales_grp_cd,
              vcd.sales_grp_desc,
              (vcd.mercia_ref):: character varying AS mercia_ref,
              NULL :: character varying AS pln_cnnl_cd,
              NULL :: character varying AS pln_chnl_desc,
              (vcd.curr_cd):: character varying AS cust_curr_cd,
              (
                ltrim(
                  (vmd.matl_id):: text,
                  (
                    (0):: character varying
                  ):: text
                )
              ):: character varying AS matl_id,
              vmd.matl_desc,
              mstrcd.master_code,
              (
                ltrim(
                  (vapcd.parent_id):: text,
                  (
                    (0):: character varying
                  ):: text
                )
              ):: character varying AS parent_matl_id,
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
              vmd.prft_ctr,
              nvl2(
                emd.divested_sku, 'Y' :: character varying,
                'N' :: character varying
              ) AS divest_flag,
              NULL :: character varying AS key_measure,
              NULL :: character varying AS ciw_ctgry,
              NULL :: character varying AS ciw_accnt_grp,
              NULL :: character varying AS px_gl_trans_desc,
              NULL :: character varying AS px_measure,
              NULL :: character varying AS px_bucket,
              NULL :: character varying AS sap_accnt,
              NULL :: character varying AS sap_accnt_desc,
              vsff.local_ccy AS local_curr_cd,
              currex.to_ccy,
              currex.exch_rate,
              0 AS base_measure,
              0 AS sales_qty,
              0 AS gts,
              0 AS gts_less_rtrn,
              0 AS eff_val,
              0 AS jgf_si_val,
              0 AS pmt_terms_val,
              0 AS datains_val,
              0 AS exp_adj_val,
              0 AS jgf_sd_val,
              0 AS nts_val,
              0 AS tot_ciw_val,
              0 AS posted_cogs,
              0 AS posted_con_free_goods_val,
              0 AS posted_gp,
              0 AS cogs,
              0 AS gp,
              vsff.future_sales_qty AS futr_sls_qty,
              (
                vsff.future_gts_val * currex.exch_rate
              ) AS futr_gts,
              (
                (
                  (0):: numeric
                ):: numeric(18, 0) * currex.exch_rate
              ) AS futr_ts,
              (
                vsff.future_nts_val * currex.exch_rate
              ) AS futr_nts,
              0 AS px_qty,
              0 AS px_gts,
              0 AS px_gts_less_rtrn,
              0 AS px_eff_val,
              0 AS px_jgf_si_val,
              0 AS px_pmt_terms_val,
              0 AS px_datains_val,
              0 AS px_exp_adj_val,
              0 AS px_jgf_sd_val,
              0 AS px_nts,
              0 AS px_ciw_tot,
              0 AS px_cogs,
              0 AS px_gp,
              0 AS projected_qty,
              0 AS projected_gts_less_rtrn,
              0 AS projected_eff_val,
              0 AS projected_jgf_si_val,
              0 AS projected_pmt_terms_val,
              0 AS projected_datains_val,
              0 AS projected_exp_adj_val,
              0 AS projected_jgf_sd_val,
              0 AS projected_ciw_tot,
              0 AS projected_nts,
              0 AS projected_cogs_current,
              0 AS projected_gp_current,
              0 AS projected_cogs_actual,
              0 AS projected_gp_actual,
              0 AS goal_qty,
              0 AS goal_gts,
              0 AS goal_eff_val,
              0 AS goal_jgf_si_val,
              0 AS goal_pmt_terms_val,
              0 AS goal_datains_val,
              0 AS goal_exp_adj_val,
              0 AS goal_jgf_sd_val,
              0 AS goal_ciw_tot,
              0 AS goal_nts,
              0 AS goal_cogs,
              0 AS goal_gp,
              0 AS bp_qty,
              0 AS bp_gts,
              0 AS bp_eff_val,
              0 AS bp_jgf_si_val,
              0 AS bp_pmt_terms_val,
              0 AS bp_datains_val,
              0 AS bp_exp_adj_val,
              0 AS bp_jgf_sd_val,
              0 AS bp_ciw_tot,
              0 AS bp_nts,
              0 AS bp_cogs,
              0 AS bp_gp,
              0 AS ju_qty,
              0 AS ju_gts,
              0 AS ju_eff_val,
              0 AS ju_jgf_si_val,
              0 AS ju_pmt_terms_val,
              0 AS ju_datains_val,
              0 AS ju_exp_adj_val,
              0 AS ju_jgf_sd_val,
              0 AS ju_ciw_tot,
              0 AS ju_nts,
              0 AS ju_cogs,
              0 AS ju_gp,
              0 AS nu_qty,
              0 AS nu_gts,
              0 AS nu_eff_val,
              0 AS nu_jgf_si_val,
              0 AS nu_pmt_terms_val,
              0 AS nu_datains_val,
              0 AS nu_exp_adj_val,
              0 AS nu_jgf_sd_val,
              0 AS nu_ciw_tot,
              0 AS nu_nts,
              0 AS nu_cogs,
              0 AS nu_gp,
              0 AS bme_trans,
              0 AS incr_gts,
              0 AS px_case_qty,
              0 AS px_tot_planspend,
              0 AS px_tot_paid,
              0 AS px_committed_spend,
              b.gcph_franchise,
              b.gcph_brand,
              b.gcph_subbrand,
              b.gcph_variant,
              b.gcph_needstate,
              b.gcph_category,
              b.gcph_subcategory,
              b.gcph_segment,
              b.gcph_subsegment
            FROM
              (
                SELECT
                  DISTINCT edw_time_dim.jj_mnth,
                  edw_time_dim.jj_mnth_shrt,
                  edw_time_dim.jj_mnth_long,
                  edw_time_dim.jj_qrtr,
                  edw_time_dim.jj_year,
                  edw_time_dim.jj_mnth_id,
                  edw_time_dim.jj_mnth_tot
                FROM
                  edw_time_dim
              ) etd,
              (
                SELECT
                  vw_jjbr_curr_exch_dim.rate_type,
                  vw_jjbr_curr_exch_dim.from_ccy,
                  vw_jjbr_curr_exch_dim.to_ccy,
                  vw_jjbr_curr_exch_dim.jj_mnth_id,
                  vw_jjbr_curr_exch_dim.exch_rate
                FROM
                  vw_jjbr_curr_exch_dim
                WHERE
                  (
                    (
                      vw_jjbr_curr_exch_dim.exch_rate = (
                        (
                          (1):: numeric
                        ):: numeric(18, 0)
                      ):: numeric(15, 5)
                    )
                    AND (
                      (vw_jjbr_curr_exch_dim.from_ccy):: text = ('AUD' :: character varying):: text
                    )
                  )
                UNION ALL
                SELECT
                  vw_bwar_curr_exch_dim.rate_type,
                  vw_bwar_curr_exch_dim.from_ccy,
                  vw_bwar_curr_exch_dim.to_ccy,
                  vw_bwar_curr_exch_dim.jj_mnth_id,
                  vw_bwar_curr_exch_dim.exch_rate
                FROM
                  vw_bwar_curr_exch_dim
              ) currex,
              (
                SELECT
                  t1.jj_mnth_id AS elpsd_jj_period,
                  (
                    (t1.jj_mnth_day / t1.jj_mnth_tot) * (
                      (100):: numeric
                    ):: numeric(18, 0)
                  ) AS prcnt_elpsd
                FROM
                  edw_time_dim t1
                WHERE
                  (
                    t1.cal_date::date =
                      (
                        current_timestamp()::date + (1):: bigint
                      )

                  )
              ) prelap,
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
                  ):: integer AS prev_jj_period,
                  t1.jj_mnth_id AS curr_jj_period
                FROM
                  edw_time_dim t1
                WHERE
                  (
                    t1.cal_date::date =
                      (
                        current_timestamp()::date + (1):: bigint
                      )

                  )
              ) projprd,
              (
                SELECT
                  derived_table10.jj_mnth_id,
                  "max"(derived_table10.rownum) AS total_wks
                FROM
                  (
                    SELECT
                      derived_table9.jj_mnth_id,
                      derived_table9.jj_wk,
                      row_number() OVER(
                        PARTITION BY derived_table9.jj_mnth_id
                        ORDER BY
                          derived_table9.jj_wk
                      ) AS rownum
                    FROM
                      (
                        SELECT
                          DISTINCT edw_time_dim.jj_mnth_id,
                          edw_time_dim.jj_wk
                        FROM
                          edw_time_dim
                      ) derived_table9
                  ) derived_table10
                GROUP BY
                  derived_table10.jj_mnth_id
              ) jj_tot_wks,
              (
                (
                  (
                    (
                      (
                        VW_SAPBW_FUTURES_FACT vsff
                        LEFT JOIN vw_customer_dim vcd ON (
                          (
                            (vsff.cust_no):: text = (vcd.cust_no):: text
                          )
                        )
                      )
                      LEFT JOIN edw_material_dim vmd ON (
                        (
                          (vsff.matl_id):: text = (vmd.matl_id):: text
                        )
                      )
                    )
                    LEFT JOIN (
                      vw_apo_parent_child_dim vapcd
                      LEFT JOIN (
                        SELECT
                          DISTINCT vw_apo_parent_child_dim.master_code,
                          vw_apo_parent_child_dim.parent_matl_desc
                        FROM
                          vw_apo_parent_child_dim
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
                          vw_apo_parent_child_dim
                        WHERE
                          (
                            NOT (
                              vw_apo_parent_child_dim.master_code IN (
                                SELECT
                                  DISTINCT vw_apo_parent_child_dim.master_code
                                FROM
                                  vw_apo_parent_child_dim
                                WHERE
                                  (
                                    (vw_apo_parent_child_dim.cmp_id):: text = (
                                      (7470):: character varying
                                    ):: text
                                  )
                              )
                            )
                          )
                      ) mstrcd ON (
                        (
                          (vapcd.master_code):: text = (mstrcd.master_code):: text
                        )
                      )
                    ) ON (
                      (
                        (
                          (vsff.cmp_id):: text = (vapcd.cmp_id):: text
                        )
                        AND (
                          (vsff.matl_id):: text = (vapcd.matl_id):: text
                        )
                      )
                    )
                  )
                  LEFT JOIN edw_material_divest emd ON (
                    (
                      (
                        (vsff.cmp_id):: text = (
                          (emd.cmp_id):: character varying
                        ):: text
                      )
                      AND (
                        ltrim(
                          (vsff.matl_id):: text,
                          ('0' :: character varying):: text
                        ) = (
                          (emd.divested_sku):: character varying
                        ):: text
                      )
                    )
                  )
                )
                LEFT JOIN (
                  SELECT
                    edw_gch_producthierarchy.materialnumber,
                    edw_gch_producthierarchy.gcph_franchise,
                    edw_gch_producthierarchy.gcph_brand,
                    edw_gch_producthierarchy.gcph_subbrand,
                    edw_gch_producthierarchy.gcph_variant,
                    edw_gch_producthierarchy.gcph_needstate,
                    edw_gch_producthierarchy.gcph_category,
                    edw_gch_producthierarchy.gcph_subcategory,
                    edw_gch_producthierarchy.gcph_segment,
                    edw_gch_producthierarchy.gcph_subsegment
                  FROM
                    edw_gch_producthierarchy
                  WHERE
                    (
                      (
                        ltrim(
                          (
                            edw_gch_producthierarchy.materialnumber
                          ):: text,
                          (
                            (0):: character varying
                          ):: text
                        ) <> ('' :: character varying):: text
                      )
                      AND (
                        (
                          edw_gch_producthierarchy."region"
                        ):: text = ('APAC' :: character varying):: text
                      )
                    )
                ) b ON (
                  (
                    ltrim(
                      (vsff.matl_id):: text,
                      (
                        (0):: character varying
                      ):: text
                    ) = ltrim(
                      (b.materialnumber):: text,
                      (
                        (0):: character varying
                      ):: text
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
                        (vsff.time_period):: numeric
                      ):: numeric(18, 0) = etd.jj_mnth_id
                    )
                    AND (
                      (vsff.local_ccy):: text = (currex.from_ccy):: text
                    )
                  )
                  AND (
                    etd.jj_mnth_id = currex.jj_mnth_id
                  )
                )
                AND (
                  jj_tot_wks.jj_mnth_id = etd.jj_mnth_id
                )
              )
          )
          UNION ALL
          SELECT
            'FILE' :: character varying AS pac_source_type,
            'OGX_HIST' :: character varying AS pac_subsource_type,
            etd.jj_mnth_id AS jj_period,
            jj_tot_wks.total_wks AS jj_wk,
            etd.jj_mnth,
            etd.jj_mnth_shrt,
            etd.jj_mnth_long,
            etd.jj_qrtr,
            etd.jj_year,
            etd.jj_mnth_tot,
            prelap.prcnt_elpsd,
            prelap.elpsd_jj_period,
            NULL :: character varying AS cust_no,
            vcd.cmp_id,
            vcd.channel_cd,
            vcd.channel_desc,
            NULL :: character varying AS ctry_key,
            vcd.country,
            NULL :: character varying AS state_cd,
            NULL :: character varying AS post_cd,
            NULL :: character varying AS cust_suburb,
            NULL :: character varying AS cust_nm,
            vcd.sales_office_cd,
            vcd.sales_office_desc,
            vcd.sales_grp_cd,
            vcd.sales_grp_desc,
            NULL :: character varying AS mercia_ref,
            spl.pln_chnl_cd AS pln_cnnl_cd,
            spl.pln_chnl_desc,
            NULL :: character varying AS cust_curr_cd,
            NULL :: character varying AS matl_id,
            NULL :: character varying AS matl_desc,
            NULL :: character varying AS master_code,
            NULL :: character varying AS parent_matl_id,
            NULL :: character varying AS parent_matl_desc,
            NULL :: character varying AS mega_brnd_cd,
            NULL :: character varying AS mega_brnd_desc,
            NULL :: character varying AS brnd_cd,
            NULL :: character varying AS brnd_desc,
            NULL :: character varying AS base_prod_cd,
            NULL :: character varying AS base_prod_desc,
            NULL :: character varying AS variant_cd,
            NULL :: character varying AS variant_desc,
            vmd.fran_cd,
            vmd.fran_desc,
            vmd.grp_fran_cd,
            vmd.grp_fran_desc,
            NULL :: character varying AS matl_type_cd,
            NULL :: character varying AS matl_type_desc,
            vmd.prod_fran_cd,
            vmd.prod_fran_desc,
            NULL :: character varying AS prod_hier_cd,
            NULL :: character varying AS prod_hier_desc,
            vmd.prod_mjr_cd,
            vmd.prod_mjr_desc,
            NULL :: character varying AS prod_mnr_cd,
            NULL :: character varying AS prod_mnr_desc,
            NULL :: character varying AS mercia_plan,
            NULL :: character varying AS putup_cd,
            NULL :: character varying AS putup_desc,
            NULL :: character varying AS bar_cd,
            NULL :: character varying AS prft_ctr,
            NULL :: character varying AS divest_flag,
            NULL :: character varying AS key_measure,
            NULL :: character varying AS ciw_ctgry,
            NULL :: character varying AS ciw_accnt_grp,
            NULL :: character varying AS px_gl_trans_desc,
            NULL :: character varying AS px_measure,
            NULL :: character varying AS px_bucket,
            evdr.sap_accnt,
            evdr.sap_accnt_desc,
            evdr.local_ccy AS local_curr_cd,
            currex.to_ccy,
            currex.exch_rate,
            (
              (evdr.base_measure):: numeric(38, 2) * currex.exch_rate
            ) AS base_measure,
            evdr.sales_qty,
            (
              (evdr.sales_gts_val):: numeric(38, 2) * currex.exch_rate
            ) AS gts,
            (
              (evdr.sales_gts_val):: numeric(38, 2) * currex.exch_rate
            ) AS gts_less_rtrn,
            (
              (evdr.sales_eff_val):: numeric(38, 2) * currex.exch_rate
            ) AS eff_val,
            (
              (evdr.sales_jgf_si_val):: numeric(38, 2) * currex.exch_rate
            ) AS jgf_si_val,
            (
              (evdr.sales_pmt_terms_val):: numeric(38, 2) * currex.exch_rate
            ) AS pmt_terms_val,
            (
              (evdr.sales_datains_val):: numeric(38, 2) * currex.exch_rate
            ) AS datains_val,
            (
              (evdr.sales_exp_adj_val):: numeric(38, 2) * currex.exch_rate
            ) AS exp_adj_val,
            (
              (evdr.sales_jgf_sd_val):: numeric(38, 2) * currex.exch_rate
            ) AS jgf_sd_val,
            (
              (evdr.sales_nts_val):: numeric(38, 2) * currex.exch_rate
            ) AS nts_val,
            (
              (evdr.sales_ciw_tot_val):: numeric(38, 2) * currex.exch_rate
            ) AS tot_ciw_val,
            (
              (evdr.sales_cogs_val):: numeric(38, 2) * currex.exch_rate
            ) AS posted_cogs,
            0 AS posted_con_free_goods_val,
            (
              (evdr.sales_gp_val):: numeric(38, 2) * currex.exch_rate
            ) AS posted_gp,
            (
              (evdr.sales_cogs_val):: numeric(38, 2) * currex.exch_rate
            ) AS cogs,
            (
              (evdr.sales_gp_val):: numeric(38, 2) * currex.exch_rate
            ) AS gp,
            0 AS futr_sls_qty,
            0 AS futr_gts,
            0 AS futr_ts,
            0 AS futr_nts,
            0 AS px_qty,
            0 AS px_gts,
            0 AS px_gts_less_rtrn,
            0 AS px_eff_val,
            0 AS px_jgf_si_val,
            0 AS px_pmt_terms_val,
            0 AS px_datains_val,
            0 AS px_exp_adj_val,
            0 AS px_jgf_sd_val,
            0 AS px_nts,
            0 AS px_ciw_tot,
            0 AS px_cogs,
            0 AS px_gp,
            CASE WHEN (
              etd.jj_mnth_id <= (
                (projprd.prev_jj_period):: numeric
              ):: numeric(18, 0)
            ) THEN evdr.sales_qty ELSE (
              (0):: numeric
            ):: numeric(18, 0) END AS projected_qty,
            CASE WHEN (
              etd.jj_mnth_id <= (
                (projprd.prev_jj_period):: numeric
              ):: numeric(18, 0)
            ) THEN (
              (evdr.sales_gts_val):: numeric(38, 2) * currex.exch_rate
            ) ELSE (
              (0):: numeric
            ):: numeric(18, 0) END AS projected_gts_less_rtrn,
            CASE WHEN (
              etd.jj_mnth_id <= (
                (projprd.prev_jj_period):: numeric
              ):: numeric(18, 0)
            ) THEN (
              (evdr.sales_eff_val):: numeric(38, 2) * currex.exch_rate
            ) ELSE (
              (0):: numeric
            ):: numeric(18, 0) END AS projected_eff_val,
            CASE WHEN (
              etd.jj_mnth_id <= (
                (projprd.prev_jj_period):: numeric
              ):: numeric(18, 0)
            ) THEN (
              (evdr.sales_jgf_si_val):: numeric(38, 2) * currex.exch_rate
            ) ELSE (
              (0):: numeric
            ):: numeric(18, 0) END AS projected_jgf_si_val,
            CASE WHEN (
              etd.jj_mnth_id <= (
                (projprd.prev_jj_period):: numeric
              ):: numeric(18, 0)
            ) THEN (
              (evdr.sales_pmt_terms_val):: numeric(38, 2) * currex.exch_rate
            ) ELSE (
              (0):: numeric
            ):: numeric(18, 0) END AS projected_pmt_terms_val,
            CASE WHEN (
              etd.jj_mnth_id <= (
                (projprd.prev_jj_period):: numeric
              ):: numeric(18, 0)
            ) THEN (
              (evdr.sales_datains_val):: numeric(38, 2) * currex.exch_rate
            ) ELSE (
              (0):: numeric
            ):: numeric(18, 0) END AS projected_datains_val,
            CASE WHEN (
              etd.jj_mnth_id <= (
                (projprd.prev_jj_period):: numeric
              ):: numeric(18, 0)
            ) THEN (
              (evdr.sales_exp_adj_val):: numeric(38, 2) * currex.exch_rate
            ) ELSE (
              (0):: numeric
            ):: numeric(18, 0) END AS projected_exp_adj_val,
            CASE WHEN (
              etd.jj_mnth_id <= (
                (projprd.prev_jj_period):: numeric
              ):: numeric(18, 0)
            ) THEN (
              (evdr.sales_jgf_sd_val):: numeric(38, 2) * currex.exch_rate
            ) ELSE (
              (0):: numeric
            ):: numeric(18, 0) END AS projected_jgf_sd_val,
            CASE WHEN (
              etd.jj_mnth_id <= (
                (projprd.prev_jj_period):: numeric
              ):: numeric(18, 0)
            ) THEN (
              (evdr.sales_ciw_tot_val):: numeric(38, 2) * currex.exch_rate
            ) ELSE (
              (0):: numeric
            ):: numeric(18, 0) END AS projected_ciw_tot,
            CASE WHEN (
              etd.jj_mnth_id <= (
                (projprd.prev_jj_period):: numeric
              ):: numeric(18, 0)
            ) THEN (
              (evdr.sales_nts_val):: numeric(38, 2) * currex.exch_rate
            ) ELSE (
              (0):: numeric
            ):: numeric(18, 0) END AS projected_nts,
            CASE WHEN (
              etd.jj_mnth_id <= (
                (projprd.prev_jj_period):: numeric
              ):: numeric(18, 0)
            ) THEN (
              (evdr.sales_cogs_val):: numeric(38, 2) * currex.exch_rate
            ) ELSE (
              (0):: numeric
            ):: numeric(18, 0) END AS projected_cogs_current,
            (
              CASE WHEN (
                etd.jj_mnth_id <= (
                  (projprd.prev_jj_period):: numeric
                ):: numeric(18, 0)
              ) THEN (
                (evdr.sales_nts_val):: numeric(38, 2) * currex.exch_rate
              ) ELSE (
                (0):: numeric
              ):: numeric(18, 0) END - CASE WHEN (
                etd.jj_mnth_id <= (
                  (projprd.prev_jj_period):: numeric
                ):: numeric(18, 0)
              ) THEN (
                (evdr.sales_cogs_val):: numeric(38, 2) * currex.exch_rate
              ) ELSE (
                (0):: numeric
              ):: numeric(18, 0) END
            ) AS projected_gp_current,
            CASE WHEN (
              etd.jj_mnth_id <= (
                (projprd.prev_jj_period):: numeric
              ):: numeric(18, 0)
            ) THEN (
              (evdr.sales_cogs_val):: numeric(38, 2) * currex.exch_rate
            ) ELSE (
              (0):: numeric
            ):: numeric(18, 0) END AS projected_cogs_actual,
            (
              CASE WHEN (
                etd.jj_mnth_id <= (
                  (projprd.prev_jj_period):: numeric
                ):: numeric(18, 0)
              ) THEN (
                (evdr.sales_nts_val):: numeric(38, 2) * currex.exch_rate
              ) ELSE (
                (0):: numeric
              ):: numeric(18, 0) END - CASE WHEN (
                etd.jj_mnth_id <= (
                  (projprd.prev_jj_period):: numeric
                ):: numeric(18, 0)
              ) THEN (
                (evdr.sales_cogs_val):: numeric(38, 2) * currex.exch_rate
              ) ELSE (
                (0):: numeric
              ):: numeric(18, 0) END
            ) AS projected_gp_actual,
            0 AS goal_qty,
            0 AS goal_gts,
            0 AS goal_eff_val,
            0 AS goal_jgf_si_val,
            0 AS goal_pmt_terms_val,
            0 AS goal_datains_val,
            0 AS goal_exp_adj_val,
            0 AS goal_jgf_sd_val,
            0 AS goal_ciw_tot,
            0 AS goal_nts,
            0 AS goal_cogs,
            0 AS goal_gp,
            0 AS bp_qty,
            0 AS bp_gts,
            0 AS bp_eff_val,
            0 AS bp_jgf_si_val,
            0 AS bp_pmt_terms_val,
            0 AS bp_datains_val,
            0 AS bp_exp_adj_val,
            0 AS bp_jgf_sd_val,
            0 AS bp_ciw_tot,
            0 AS bp_nts,
            0 AS bp_cogs,
            0 AS bp_gp,
            0 AS ju_qty,
            0 AS ju_gts,
            0 AS ju_eff_val,
            0 AS ju_jgf_si_val,
            0 AS ju_pmt_terms_val,
            0 AS ju_datains_val,
            0 AS ju_exp_adj_val,
            0 AS ju_jgf_sd_val,
            0 AS ju_ciw_tot,
            0 AS ju_nts,
            0 AS ju_cogs,
            0 AS ju_gp,
            0 AS nu_qty,
            0 AS nu_gts,
            0 AS nu_eff_val,
            0 AS nu_jgf_si_val,
            0 AS nu_pmt_terms_val,
            0 AS nu_datains_val,
            0 AS nu_exp_adj_val,
            0 AS nu_jgf_sd_val,
            0 AS nu_ciw_tot,
            0 AS nu_nts,
            0 AS nu_cogs,
            0 AS nu_gp,
            0 AS bme_trans,
            0 AS incr_gts,
            0 AS px_case_qty,
            0 AS px_tot_planspend,
            0 AS px_tot_paid,
            0 AS px_committed_spend,
            NULL :: character varying AS gcph_franchise,
            NULL :: character varying AS gcph_brand,
            NULL :: character varying AS gcph_subbrand,
            NULL :: character varying AS gcph_variant,
            NULL :: character varying AS gcph_needstate,
            NULL :: character varying AS gcph_category,
            NULL :: character varying AS gcph_subcategory,
            NULL :: character varying AS gcph_segment,
            NULL :: character varying AS gcph_subsegment
          FROM
            edw_sapbw_plan_lkp spl,
            (
              SELECT
                vw_jjbr_curr_exch_dim.rate_type,
                vw_jjbr_curr_exch_dim.from_ccy,
                vw_jjbr_curr_exch_dim.to_ccy,
                vw_jjbr_curr_exch_dim.jj_mnth_id,
                vw_jjbr_curr_exch_dim.exch_rate
              FROM
                vw_jjbr_curr_exch_dim
              WHERE
                (
                  (
                    vw_jjbr_curr_exch_dim.exch_rate = (
                      (
                        (1):: numeric
                      ):: numeric(18, 0)
                    ):: numeric(15, 5)
                  )
                  AND (
                    (vw_jjbr_curr_exch_dim.from_ccy):: text = ('AUD' :: character varying):: text
                  )
                )
              UNION ALL
              SELECT
                vw_bwar_curr_exch_dim.rate_type,
                vw_bwar_curr_exch_dim.from_ccy,
                vw_bwar_curr_exch_dim.to_ccy,
                vw_bwar_curr_exch_dim.jj_mnth_id,
                vw_bwar_curr_exch_dim.exch_rate
              FROM
                vw_bwar_curr_exch_dim
            ) currex,
            (
              SELECT
                t1.jj_mnth_id AS elpsd_jj_period,
                (
                  (t1.jj_mnth_day / t1.jj_mnth_tot) * (
                    (100):: numeric
                  ):: numeric(18, 0)
                ) AS prcnt_elpsd
              FROM
                edw_time_dim t1
              WHERE
                (
                  t1.cal_date::date =
                    (
                      current_timestamp()::date + (1):: bigint
                    )

                )
            ) prelap,
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
                ):: integer AS prev_jj_period,
                t1.jj_mnth_id AS curr_jj_period
              FROM
                edw_time_dim t1
              WHERE
                (
                  t1.cal_date::date =
                    (
                      current_timestamp()::date + (1):: bigint
                    )
                )
            ) projprd,
            (
              SELECT
                derived_table12.jj_mnth_id,
                "max"(derived_table12.rownum) AS total_wks
              FROM
                (
                  SELECT
                    derived_table11.jj_mnth_id,
                    derived_table11.jj_wk,
                    row_number() OVER(
                      PARTITION BY derived_table11.jj_mnth_id
                      ORDER BY
                        derived_table11.jj_wk
                    ) AS rownum
                  FROM
                    (
                      SELECT
                        DISTINCT edw_time_dim.jj_mnth_id,
                        edw_time_dim.jj_wk
                      FROM
                        edw_time_dim
                    ) derived_table11
                ) derived_table12
              GROUP BY
                derived_table12.jj_mnth_id
            ) jj_tot_wks,
            (
              (
                (
                  EDW_VOGUE_DATA_REF evdr
                  LEFT JOIN (
                    SELECT
                      DISTINCT edw_time_dim.jj_mnth,
                      edw_time_dim.jj_mnth_shrt,
                      edw_time_dim.jj_mnth_long,
                      edw_time_dim.jj_qrtr,
                      edw_time_dim.jj_year,
                      edw_time_dim.jj_mnth_id,
                      edw_time_dim.jj_mnth_tot
                    FROM
                      edw_time_dim
                  ) etd ON (
                    (
                      (evdr.time_period):: numeric(18, 0) = etd.jj_mnth_id
                    )
                  )
                )
                LEFT JOIN (
                  SELECT
                    DISTINCT vw_customer_dim.cmp_id,
                    vw_customer_dim.channel_cd,
                    vw_customer_dim.channel_desc,
                    vw_customer_dim.sales_office_cd,
                    vw_customer_dim.sales_office_desc,
                    vw_customer_dim.sales_grp_cd,
                    vw_customer_dim.sales_grp_desc,
                    vw_customer_dim.country
                  FROM
                    vw_customer_dim
                ) vcd ON (
                  (
                    (evdr.sls_grp_cd):: text = (vcd.sales_grp_cd):: text
                  )
                )
              )
              LEFT JOIN (
                SELECT
                  DISTINCT edw_material_dim.fran_cd,
                  edw_material_dim.fran_desc,
                  edw_material_dim.grp_fran_cd,
                  edw_material_dim.grp_fran_desc,
                  edw_material_dim.prod_fran_cd,
                  edw_material_dim.prod_fran_desc,
                  edw_material_dim.prod_mjr_cd,
                  edw_material_dim.prod_mjr_desc
                FROM
                  edw_material_dim
              ) vmd ON (
                (
                  (evdr.prod_mjr_cd):: text = (vmd.prod_mjr_cd):: text
                )
              )
            )
          WHERE
            (
              (
                (
                  (
                    (evdr.local_ccy):: text = (currex.from_ccy):: text
                  )
                  AND (
                    etd.jj_mnth_id = currex.jj_mnth_id
                  )
                )
                AND (
                  jj_tot_wks.jj_mnth_id = etd.jj_mnth_id
                )
              )
              AND (
                (spl.sls_grp_cd):: text = (vcd.sales_grp_cd):: text
              )
            )
        )
        UNION ALL
        SELECT
          'FILE' :: character varying AS pac_source_type,
          'CIW_ADJUST' :: character varying AS pac_subsource_type,
          etd.jj_mnth_id AS jj_period,
          jj_tot_wks.total_wks AS jj_wk,
          etd.jj_mnth,
          etd.jj_mnth_shrt,
          etd.jj_mnth_long,
          etd.jj_qrtr,
          etd.jj_year,
          etd.jj_mnth_tot,
          prelap.prcnt_elpsd,
          prelap.elpsd_jj_period,
          (
            ltrim(
              vcd.cust_no,
              (
                (0):: character varying
              ):: text
            )
          ):: character varying AS cust_no,
          vcd.cmp_id,
          vcd.channel_cd,
          vcd.channel_desc,
          NULL :: character varying AS ctry_key,
          vcd.country,
          NULL :: character varying AS state_cd,
          NULL :: character varying AS post_cd,
          NULL :: character varying AS cust_suburb,
          NULL :: character varying AS cust_nm,
          vcd.sales_office_cd,
          vcd.sales_office_desc,
          vcd.sales_grp_cd,
          vcd.sales_grp_desc,
          NULL :: character varying AS mercia_ref,
          spl.pln_chnl_cd AS pln_cnnl_cd,
          spl.pln_chnl_desc,
          NULL :: character varying AS cust_curr_cd,
          NULL :: character varying AS matl_id,
          NULL :: character varying AS matl_desc,
          NULL :: character varying AS master_code,
          NULL :: character varying AS parent_matl_id,
          NULL :: character varying AS parent_matl_desc,
          NULL :: character varying AS mega_brnd_cd,
          NULL :: character varying AS mega_brnd_desc,
          NULL :: character varying AS brnd_cd,
          NULL :: character varying AS brnd_desc,
          NULL :: character varying AS base_prod_cd,
          NULL :: character varying AS base_prod_desc,
          NULL :: character varying AS variant_cd,
          NULL :: character varying AS variant_desc,
          vmd.fran_cd,
          vmd.fran_desc,
          vmd.grp_fran_cd,
          vmd.grp_fran_desc,
          NULL :: character varying AS matl_type_cd,
          NULL :: character varying AS matl_type_desc,
          vmd.prod_fran_cd,
          vmd.prod_fran_desc,
          NULL :: character varying AS prod_hier_cd,
          NULL :: character varying AS prod_hier_desc,
          vmd.prod_mjr_cd,
          vmd.prod_mjr_desc,
          NULL :: character varying AS prod_mnr_cd,
          NULL :: character varying AS prod_mnr_desc,
          NULL :: character varying AS mercia_plan,
          NULL :: character varying AS putup_cd,
          NULL :: character varying AS putup_desc,
          NULL :: character varying AS bar_cd,
          NULL :: character varying AS prft_ctr,
          NULL :: character varying AS divest_flag,
          NULL :: character varying AS key_measure,
          NULL :: character varying AS ciw_ctgry,
          NULL :: character varying AS ciw_accnt_grp,
          NULL :: character varying AS px_gl_trans_desc,
          NULL :: character varying AS px_measure,
          NULL :: character varying AS px_bucket,
          faar.sap_accnt,
          faar.sap_accnt_desc,
          faar.local_ccy AS local_curr_cd,
          currex.to_ccy,
          currex.exch_rate,
          (
            (faar.base_measure):: numeric(38, 2) * currex.exch_rate
          ) AS base_measure,
          0 AS sales_qty,
          0 AS gts,
          0 AS gts_less_rtrn,
          0 AS eff_val,
          0 AS jgf_si_val,
          0 AS pmt_terms_val,
          0 AS datains_val,
          0 AS exp_adj_val,
          0 AS jgf_sd_val,
          0 AS nts_val,
          0 AS tot_ciw_val,
          0 AS posted_cogs,
          0 AS posted_con_free_goods_val,
          0 AS posted_gp,
          0 AS cogs,
          0 AS gp,
          0 AS futr_sls_qty,
          0 AS futr_gts,
          0 AS futr_ts,
          0 AS futr_nts,
          0 AS px_qty,
          0 AS px_gts,
          0 AS px_gts_less_rtrn,
          0 AS px_eff_val,
          0 AS px_jgf_si_val,
          0 AS px_pmt_terms_val,
          0 AS px_datains_val,
          0 AS px_exp_adj_val,
          0 AS px_jgf_sd_val,
          0 AS px_nts,
          0 AS px_ciw_tot,
          0 AS px_cogs,
          0 AS px_gp,
          0 AS projected_qty,
          0 AS projected_gts_less_rtrn,
          0 AS projected_eff_val,
          0 AS projected_jgf_si_val,
          0 AS projected_pmt_terms_val,
          0 AS projected_datains_val,
          0 AS projected_exp_adj_val,
          0 AS projected_jgf_sd_val,
          0 AS projected_ciw_tot,
          0 AS projected_nts,
          0 AS projected_cogs_current,
          0 AS projected_gp_current,
          0 AS projected_cogs_actual,
          0 AS projected_gp_actual,
          0 AS goal_qty,
          0 AS goal_gts,
          0 AS goal_eff_val,
          0 AS goal_jgf_si_val,
          0 AS goal_pmt_terms_val,
          0 AS goal_datains_val,
          0 AS goal_exp_adj_val,
          0 AS goal_jgf_sd_val,
          0 AS goal_ciw_tot,
          0 AS goal_nts,
          0 AS goal_cogs,
          0 AS goal_gp,
          0 AS bp_qty,
          0 AS bp_gts,
          0 AS bp_eff_val,
          0 AS bp_jgf_si_val,
          0 AS bp_pmt_terms_val,
          0 AS bp_datains_val,
          0 AS bp_exp_adj_val,
          0 AS bp_jgf_sd_val,
          0 AS bp_ciw_tot,
          0 AS bp_nts,
          0 AS bp_cogs,
          0 AS bp_gp,
          0 AS ju_qty,
          0 AS ju_gts,
          0 AS ju_eff_val,
          0 AS ju_jgf_si_val,
          0 AS ju_pmt_terms_val,
          0 AS ju_datains_val,
          0 AS ju_exp_adj_val,
          0 AS ju_jgf_sd_val,
          0 AS ju_ciw_tot,
          0 AS ju_nts,
          0 AS ju_cogs,
          0 AS ju_gp,
          0 AS nu_qty,
          0 AS nu_gts,
          0 AS nu_eff_val,
          0 AS nu_jgf_si_val,
          0 AS nu_pmt_terms_val,
          0 AS nu_datains_val,
          0 AS nu_exp_adj_val,
          0 AS nu_jgf_sd_val,
          0 AS nu_ciw_tot,
          0 AS nu_nts,
          0 AS nu_cogs,
          0 AS nu_gp,
          0 AS bme_trans,
          0 AS incr_gts,
          0 AS px_case_qty,
          0 AS px_tot_planspend,
          0 AS px_tot_paid,
          0 AS px_committed_spend,
          NULL :: character varying AS gcph_franchise,
          NULL :: character varying AS gcph_brand,
          NULL :: character varying AS gcph_subbrand,
          NULL :: character varying AS gcph_variant,
          NULL :: character varying AS gcph_needstate,
          NULL :: character varying AS gcph_category,
          NULL :: character varying AS gcph_subcategory,
          NULL :: character varying AS gcph_segment,
          NULL :: character varying AS gcph_subsegment
        FROM
          edw_sapbw_plan_lkp spl,
          (
            SELECT
              vw_jjbr_curr_exch_dim.rate_type,
              vw_jjbr_curr_exch_dim.from_ccy,
              vw_jjbr_curr_exch_dim.to_ccy,
              vw_jjbr_curr_exch_dim.jj_mnth_id,
              vw_jjbr_curr_exch_dim.exch_rate
            FROM
              vw_jjbr_curr_exch_dim
            WHERE
              (
                (
                  vw_jjbr_curr_exch_dim.exch_rate = (
                    (
                      (1):: numeric
                    ):: numeric(18, 0)
                  ):: numeric(15, 5)
                )
                AND (
                  (vw_jjbr_curr_exch_dim.from_ccy):: text = ('AUD' :: character varying):: text
                )
              )
            UNION ALL
            SELECT
              vw_bwar_curr_exch_dim.rate_type,
              vw_bwar_curr_exch_dim.from_ccy,
              vw_bwar_curr_exch_dim.to_ccy,
              vw_bwar_curr_exch_dim.jj_mnth_id,
              vw_bwar_curr_exch_dim.exch_rate
            FROM
              vw_bwar_curr_exch_dim
          ) currex,
          (
            SELECT
              t1.jj_mnth_id AS elpsd_jj_period,
              (
                (t1.jj_mnth_day / t1.jj_mnth_tot) * (
                  (100):: numeric
                ):: numeric(18, 0)
              ) AS prcnt_elpsd
            FROM
              edw_time_dim t1
            WHERE
              (
                t1.cal_date::date =
                  (
                    current_timestamp()::date + (1):: bigint
                  )

              )
          ) prelap,
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
              ):: integer AS prev_jj_period,
              t1.jj_mnth_id AS curr_jj_period
            FROM
              edw_time_dim t1
            WHERE
              (
                t1.cal_date::date =
                  (
                    current_timestamp()::date + (1):: bigint
                  )

              )
          ) projprd,
          (
            SELECT
              derived_table14.jj_mnth_id,
              "max"(derived_table14.rownum) AS total_wks
            FROM
              (
                SELECT
                  derived_table13.jj_mnth_id,
                  derived_table13.jj_wk,
                  row_number() OVER(
                    PARTITION BY derived_table13.jj_mnth_id
                    ORDER BY
                      derived_table13.jj_wk
                  ) AS rownum
                FROM
                  (
                    SELECT
                      DISTINCT edw_time_dim.jj_mnth_id,
                      edw_time_dim.jj_wk
                    FROM
                      edw_time_dim
                  ) derived_table13
              ) derived_table14
            GROUP BY
              derived_table14.jj_mnth_id
          ) jj_tot_wks,
          (
            (
              (
                VW_FIN_ACCRUAL_ADJ faar
                LEFT JOIN (
                  SELECT
                    DISTINCT edw_time_dim.jj_mnth,
                    edw_time_dim.jj_mnth_shrt,
                    edw_time_dim.jj_mnth_long,
                    edw_time_dim.jj_qrtr,
                    edw_time_dim.jj_year,
                    edw_time_dim.jj_mnth_id,
                    edw_time_dim.jj_mnth_tot
                  FROM
                    edw_time_dim
                ) etd ON (
                  (
                    (faar.time_period):: numeric(18, 0) = etd.jj_mnth_id
                  )
                )
              )
              LEFT JOIN (
                SELECT
                  DISTINCT vw_customer_dim.cmp_id,
                  ltrim(
                    (vw_customer_dim.cust_no):: text,
                    (
                      (0):: character varying
                    ):: text
                  ) AS cust_no,
                  vw_customer_dim.channel_cd,
                  vw_customer_dim.channel_desc,
                  vw_customer_dim.sales_office_cd,
                  vw_customer_dim.sales_office_desc,
                  vw_customer_dim.sales_grp_cd,
                  vw_customer_dim.sales_grp_desc,
                  vw_customer_dim.country
                FROM
                  vw_customer_dim
              ) vcd ON (
                (
                  (faar.cust_id):: text = vcd.cust_no
                )
              )
            )
            LEFT JOIN (
              SELECT
                DISTINCT edw_material_dim.fran_cd,
                edw_material_dim.fran_desc,
                edw_material_dim.grp_fran_cd,
                edw_material_dim.grp_fran_desc,
                edw_material_dim.prod_fran_cd,
                edw_material_dim.prod_fran_desc,
                edw_material_dim.prod_mjr_cd,
                edw_material_dim.prod_mjr_desc
              FROM
                edw_material_dim
            ) vmd ON (
              (
                (faar.prod_mjr_cd):: text = (vmd.prod_mjr_cd):: text
              )
            )
          )
        WHERE
          (
            (
              (
                (
                  (faar.local_ccy):: text = (currex.from_ccy):: text
                )
                AND (
                  etd.jj_mnth_id = currex.jj_mnth_id
                )
              )
              AND (
                jj_tot_wks.jj_mnth_id = etd.jj_mnth_id
              )
            )
            AND (
              (spl.sls_grp_cd):: text = (vcd.sales_grp_cd):: text
            )
          )
      )
      UNION ALL
      SELECT
        'SAPBW' :: character varying AS pac_source_type,
        'SAPBW_PLAN' :: character varying AS pac_subsource_type,
        etd.jj_mnth_id AS jj_period,
        jj_tot_wks.total_wks AS jj_wk,
        etd.jj_mnth,
        etd.jj_mnth_shrt,
        etd.jj_mnth_long,
        etd.jj_qrtr,
        etd.jj_year,
        etd.jj_mnth_tot,
        prelap.prcnt_elpsd,
        prelap.elpsd_jj_period,
        NULL :: character varying AS cust_no,
        ecpf.co_cd AS cmp_id,
        NULL :: character varying AS channel_cd,
        NULL :: character varying AS channel_desc,
        ecpf.ctry_key,
        CASE WHEN (
          (
            (ecpf.co_cd):: text = ('7470' :: character varying):: text
          )
          OR (
            (ecpf.co_cd IS NULL)
            AND ('7470' IS NULL)
          )
        ) THEN 'Australia' :: character varying WHEN (
          (
            (ecpf.co_cd):: text = ('8361' :: character varying):: text
          )
          OR (
            (ecpf.co_cd IS NULL)
            AND ('8361' IS NULL)
          )
        ) THEN 'New Zealand' :: character varying ELSE NULL :: character varying END AS country,
        NULL :: character varying AS state_cd,
        NULL :: character varying AS post_cd,
        NULL :: character varying AS cust_suburb,
        NULL :: character varying AS cust_nm,
        ecpf.sls_ofc AS sales_office_cd,
        NULL :: character varying AS sales_office_desc,
        NULL :: character varying AS sales_grp_cd,
        NULL :: character varying AS sales_grp_desc,
        CASE WHEN (
          (
            (ecpf.mercia_ref):: text = ('AU5' :: character varying):: text
          )
          OR (
            (ecpf.mercia_ref IS NULL)
            AND ('AU5' IS NULL)
          )
        ) THEN 'AUEX' :: character varying ELSE ecpf.mercia_ref END AS mercia_ref,
        spl.pln_chnl_cd AS pln_cnnl_cd,
        spl.pln_chnl_desc,
        NULL :: character varying AS cust_curr_cd,
        (
          CASE WHEN (
            (
              (
                ecpf.fisc_yr = (
                  (2020):: numeric
                ):: numeric(18, 0)
              )
              AND (
                (ecpf.category):: text = ('JU' :: character varying):: text
              )
            )
            AND (
              ltrim(
                (ecpf.matl_num):: text,
                (
                  (0):: character varying
                ):: text
              ) = ('46101731' :: character varying):: text
            )
          ) THEN ('79619630' :: character varying):: text ELSE ltrim(
            (ecpf.matl_num):: text,
            (
              (0):: character varying
            ):: text
          ) END
        ):: character varying AS matl_id,
        vmd.matl_desc,
        mstrcd.master_code,
        (
          ltrim(
            (vapcd.parent_id):: text,
            (
              (0):: character varying
            ):: text
          )
        ):: character varying AS parent_matl_id,
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
        vmd.prft_ctr,
        NULL :: character varying AS divest_flag,
        NULL :: character varying AS key_measure,
        NULL :: character varying AS ciw_ctgry,
        NULL :: character varying AS ciw_accnt_grp,
        NULL :: character varying AS px_gl_trans_desc,
        NULL :: character varying AS px_measure,
        NULL :: character varying AS px_bucket,
        NULL :: character varying AS sap_accnt,
        NULL :: character varying AS sap_accnt_desc,
        ecpf.obj_crncy AS local_curr_cd,
        currex.to_ccy,
        currex.exch_rate,
        0 AS base_measure,
        0 AS sales_qty,
        0 AS gts,
        0 AS gts_less_rtrn,
        0 AS eff_val,
        0 AS jgf_si_val,
        0 AS pmt_terms_val,
        0 AS datains_val,
        0 AS exp_adj_val,
        0 AS jgf_sd_val,
        0 AS nts_val,
        0 AS tot_ciw_val,
        0 AS posted_cogs,
        0 AS posted_con_free_goods_val,
        0 AS posted_gp,
        0 AS cogs,
        0 AS gp,
        0 AS futr_sls_qty,
        0 AS futr_gts,
        0 AS futr_ts,
        0 AS futr_nts,
        0 AS px_qty,
        0 AS px_gts,
        0 AS px_gts_less_rtrn,
        0 AS px_eff_val,
        0 AS px_jgf_si_val,
        0 AS px_pmt_terms_val,
        0 AS px_datains_val,
        0 AS px_exp_adj_val,
        0 AS px_jgf_sd_val,
        0 AS px_nts,
        0 AS px_ciw_tot,
        0 AS px_cogs,
        0 AS px_gp,
        0 AS projected_qty,
        0 AS projected_gts_less_rtrn,
        0 AS projected_eff_val,
        0 AS projected_jgf_si_val,
        0 AS projected_pmt_terms_val,
        0 AS projected_datains_val,
        0 AS projected_exp_adj_val,
        0 AS projected_jgf_sd_val,
        0 AS projected_ciw_tot,
        0 AS projected_nts,
        0 AS projected_cogs_current,
        0 AS projected_gp_current,
        0 AS projected_cogs_actual,
        0 AS projected_gp_actual,
        0 AS goal_qty,
        0 AS goal_gts,
        0 AS goal_eff_val,
        0 AS goal_jgf_si_val,
        0 AS goal_pmt_terms_val,
        0 AS goal_datains_val,
        0 AS goal_exp_adj_val,
        0 AS goal_jgf_sd_val,
        0 AS goal_ciw_tot,
        0 AS goal_nts,
        0 AS goal_cogs,
        0 AS goal_gp,
        CASE WHEN (
          (
            (ecpf.category):: text = ('BP' :: character varying):: text
          )
          AND (
            (ecpf.acct_hier_shrt_desc):: text = ('GTS' :: character varying):: text
          )
        ) THEN (ecpf.qty):: numeric(38, 2) ELSE (
          (0):: numeric
        ):: numeric(18, 0) END AS bp_qty,
        CASE WHEN (
          (
            (ecpf.category):: text = ('BP' :: character varying):: text
          )
          AND (
            (ecpf.acct_hier_shrt_desc):: text = ('GTS' :: character varying):: text
          )
        ) THEN (
          (ecpf.amt_obj_crcy):: numeric(38, 2) * currex.exch_rate
        ) ELSE (
          (0):: numeric
        ):: numeric(18, 0) END AS bp_gts,
        0 AS bp_eff_val,
        0 AS bp_jgf_si_val,
        0 AS bp_pmt_terms_val,
        0 AS bp_datains_val,
        0 AS bp_exp_adj_val,
        0 AS bp_jgf_sd_val,
        (
          CASE WHEN (
            (
              (ecpf.category):: text = ('BP' :: character varying):: text
            )
            AND (
              (ecpf.acct_hier_shrt_desc):: text = ('GTS' :: character varying):: text
            )
          ) THEN (
            (ecpf.amt_obj_crcy):: numeric(38, 2) * currex.exch_rate
          ) ELSE (
            (0):: numeric
          ):: numeric(18, 0) END - CASE WHEN (
            (
              (ecpf.category):: text = ('BP' :: character varying):: text
            )
            AND (
              (ecpf.acct_hier_shrt_desc):: text = ('NTS' :: character varying):: text
            )
          ) THEN (
            (ecpf.amt_obj_crcy):: numeric(38, 2) * currex.exch_rate
          ) ELSE (
            (0):: numeric
          ):: numeric(18, 0) END
        ) AS bp_ciw_tot,
        CASE WHEN (
          (
            (ecpf.category):: text = ('BP' :: character varying):: text
          )
          AND (
            (ecpf.acct_hier_shrt_desc):: text = ('NTS' :: character varying):: text
          )
        ) THEN (
          (ecpf.amt_obj_crcy):: numeric(38, 2) * currex.exch_rate
        ) ELSE (
          (0):: numeric
        ):: numeric(18, 0) END AS bp_nts,
        0 AS bp_cogs,
        0 AS bp_gp,
        CASE WHEN (
          (
            (ecpf.category):: text = ('JU' :: character varying):: text
          )
          AND (
            (ecpf.acct_hier_shrt_desc):: text = ('GTS' :: character varying):: text
          )
        ) THEN (ecpf.qty):: numeric(38, 2) ELSE (
          (0):: numeric
        ):: numeric(18, 0) END AS ju_qty,
        CASE WHEN (
          (
            (ecpf.category):: text = ('JU' :: character varying):: text
          )
          AND (
            (ecpf.acct_hier_shrt_desc):: text = ('GTS' :: character varying):: text
          )
        ) THEN (
          (ecpf.amt_obj_crcy):: numeric(38, 2) * currex.exch_rate
        ) ELSE (
          (0):: numeric
        ):: numeric(18, 0) END AS ju_gts,
        0 AS ju_eff_val,
        0 AS ju_jgf_si_val,
        0 AS ju_pmt_terms_val,
        0 AS ju_datains_val,
        0 AS ju_exp_adj_val,
        0 AS ju_jgf_sd_val,
        (
          CASE WHEN (
            (
              (ecpf.category):: text = ('JU' :: character varying):: text
            )
            AND (
              (ecpf.acct_hier_shrt_desc):: text = ('GTS' :: character varying):: text
            )
          ) THEN (
            (ecpf.amt_obj_crcy):: numeric(38, 2) * currex.exch_rate
          ) ELSE (
            (0):: numeric
          ):: numeric(18, 0) END - CASE WHEN (
            (
              (ecpf.category):: text = ('JU' :: character varying):: text
            )
            AND (
              (ecpf.acct_hier_shrt_desc):: text = ('NTS' :: character varying):: text
            )
          ) THEN (
            (ecpf.amt_obj_crcy):: numeric(38, 2) * currex.exch_rate
          ) ELSE (
            (0):: numeric
          ):: numeric(18, 0) END
        ) AS ju_ciw_tot,
        CASE WHEN (
          (
            (ecpf.category):: text = ('JU' :: character varying):: text
          )
          AND (
            (ecpf.acct_hier_shrt_desc):: text = ('NTS' :: character varying):: text
          )
        ) THEN (
          (ecpf.amt_obj_crcy):: numeric(38, 2) * currex.exch_rate
        ) ELSE (
          (0):: numeric
        ):: numeric(18, 0) END AS ju_nts,
        0 AS ju_cogs,
        0 AS ju_gp,
        CASE WHEN (
          (
            (ecpf.category):: text = ('NU' :: character varying):: text
          )
          AND (
            (ecpf.acct_hier_shrt_desc):: text = ('GTS' :: character varying):: text
          )
        ) THEN (ecpf.qty):: numeric(38, 2) ELSE (
          (0):: numeric
        ):: numeric(18, 0) END AS nu_qty,
        CASE WHEN (
          (
            (ecpf.category):: text = ('NU' :: character varying):: text
          )
          AND (
            (ecpf.acct_hier_shrt_desc):: text = ('GTS' :: character varying):: text
          )
        ) THEN (
          (ecpf.amt_obj_crcy):: numeric(38, 2) * currex.exch_rate
        ) ELSE (
          (0):: numeric
        ):: numeric(18, 0) END AS nu_gts,
        0 AS nu_eff_val,
        0 AS nu_jgf_si_val,
        0 AS nu_pmt_terms_val,
        0 AS nu_datains_val,
        0 AS nu_exp_adj_val,
        0 AS nu_jgf_sd_val,
        (
          CASE WHEN (
            (
              (ecpf.category):: text = ('NU' :: character varying):: text
            )
            AND (
              (ecpf.acct_hier_shrt_desc):: text = ('GTS' :: character varying):: text
            )
          ) THEN (
            (ecpf.amt_obj_crcy):: numeric(38, 2) * currex.exch_rate
          ) ELSE (
            (0):: numeric
          ):: numeric(18, 0) END - CASE WHEN (
            (
              (ecpf.category):: text = ('NU' :: character varying):: text
            )
            AND (
              (ecpf.acct_hier_shrt_desc):: text = ('NTS' :: character varying):: text
            )
          ) THEN (
            (ecpf.amt_obj_crcy):: numeric(38, 2) * currex.exch_rate
          ) ELSE (
            (0):: numeric
          ):: numeric(18, 0) END
        ) AS nu_ciw_tot,
        CASE WHEN (
          (
            (ecpf.category):: text = ('NU' :: character varying):: text
          )
          AND (
            (ecpf.acct_hier_shrt_desc):: text = ('NTS' :: character varying):: text
          )
        ) THEN (
          (ecpf.amt_obj_crcy):: numeric(38, 2) * currex.exch_rate
        ) ELSE (
          (0):: numeric
        ):: numeric(18, 0) END AS nu_nts,
        0 AS nu_cogs,
        0 AS nu_gp,
        0 AS bme_trans,
        0 AS incr_gts,
        0 AS px_case_qty,
        0 AS px_tot_planspend,
        0 AS px_tot_paid,
        0 AS px_committed_spend,
        b.gcph_franchise,
        b.gcph_brand,
        b.gcph_subbrand,
        b.gcph_variant,
        b.gcph_needstate,
        b.gcph_category,
        b.gcph_subcategory,
        b.gcph_segment,
        b.gcph_subsegment
      FROM
        (
          SELECT
            DISTINCT edw_time_dim.jj_mnth,
            edw_time_dim.jj_mnth_shrt,
            edw_time_dim.jj_mnth_long,
            edw_time_dim.jj_qrtr,
            edw_time_dim.jj_year,
            edw_time_dim.jj_mnth_id,
            edw_time_dim.jj_mnth_tot
          FROM
            edw_time_dim
        ) etd,
        (
          SELECT
            vw_jjbr_curr_exch_dim.rate_type,
            vw_jjbr_curr_exch_dim.from_ccy,
            vw_jjbr_curr_exch_dim.to_ccy,
            vw_jjbr_curr_exch_dim.jj_mnth_id,
            vw_jjbr_curr_exch_dim.exch_rate
          FROM
            vw_jjbr_curr_exch_dim
          WHERE
            (
              (
                vw_jjbr_curr_exch_dim.exch_rate = (
                  (
                    (1):: numeric
                  ):: numeric(18, 0)
                ):: numeric(15, 5)
              )
              AND (
                (vw_jjbr_curr_exch_dim.from_ccy):: text = ('AUD' :: character varying):: text
              )
            )
          UNION ALL
          SELECT
            vw_bwar_curr_exch_dim.rate_type,
            vw_bwar_curr_exch_dim.from_ccy,
            vw_bwar_curr_exch_dim.to_ccy,
            vw_bwar_curr_exch_dim.jj_mnth_id,
            vw_bwar_curr_exch_dim.exch_rate
          FROM
            vw_bwar_curr_exch_dim
        ) currex,
        (
          SELECT
            t1.jj_mnth_id AS elpsd_jj_period,
            (
              (t1.jj_mnth_day / t1.jj_mnth_tot) * (
                (100):: numeric
              ):: numeric(18, 0)
            ) AS prcnt_elpsd
          FROM
            edw_time_dim t1
          WHERE
            (
              t1.cal_date::date =
                (
                  current_timestamp()::date + (1):: bigint
                )

            )
        ) prelap,
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
            ):: integer AS prev_jj_period,
            t1.jj_mnth_id AS curr_jj_period
          FROM
            edw_time_dim t1
          WHERE
            (
              t1.cal_date::date = dateadd(day,1,CURRENT_TIMESTAMP)::date
            )
        ) projprd,
        (
          SELECT
            derived_table16.jj_mnth_id,
            "max"(derived_table16.rownum) AS total_wks
          FROM
            (
              SELECT
                derived_table15.jj_mnth_id,
                derived_table15.jj_wk,
                row_number() OVER(
                  PARTITION BY derived_table15.jj_mnth_id
                  ORDER BY
                    derived_table15.jj_wk
                ) AS rownum
              FROM
                (
                  SELECT
                    DISTINCT edw_time_dim.jj_mnth_id,
                    edw_time_dim.jj_wk
                  FROM
                    edw_time_dim
                ) derived_table15
            ) derived_table16
          GROUP BY
            derived_table16.jj_mnth_id
        ) jj_tot_wks,
        (
          (
            (
              (
                (
                  SELECT
                    edw_copa_plan_fact.fisc_yr_per,
                    edw_copa_plan_fact.fisc_yr_vrnt,
                    edw_copa_plan_fact.fisc_yr,
                    edw_copa_plan_fact.cal_day,
                    edw_copa_plan_fact.pstng_per,
                    edw_copa_plan_fact.cal_yr_mo,
                    edw_copa_plan_fact.cal_yr,
                    edw_copa_plan_fact.vers,
                    edw_copa_plan_fact.val_type,
                    edw_copa_plan_fact.co_cd,
                    edw_copa_plan_fact.cntl_area,
                    edw_copa_plan_fact.prft_ctr,
                    edw_copa_plan_fact.sls_emp_hist,
                    edw_copa_plan_fact.sls_org,
                    edw_copa_plan_fact.sls_grp,
                    edw_copa_plan_fact.sls_ofc,
                    edw_copa_plan_fact.cust_grp,
                    edw_copa_plan_fact.dstn_chnl,
                    edw_copa_plan_fact.sls_dstrc,
                    edw_copa_plan_fact.cust_num,
                    edw_copa_plan_fact.matl_num,
                    edw_copa_plan_fact.cust_sls_view,
                    edw_copa_plan_fact.div,
                    edw_copa_plan_fact.plnt,
                    edw_copa_plan_fact.mercia_ref,
                    edw_copa_plan_fact.b3_base_prod,
                    edw_copa_plan_fact.b4_vrnt,
                    edw_copa_plan_fact.b5_put_up,
                    edw_copa_plan_fact.b1_mega_brnd,
                    edw_copa_plan_fact.b2_brnd,
                    edw_copa_plan_fact.rgn,
                    edw_copa_plan_fact.ctry_key,
                    edw_copa_plan_fact.prod_minor,
                    edw_copa_plan_fact.prod_maj,
                    edw_copa_plan_fact.prod_fran,
                    edw_copa_plan_fact.fran,
                    edw_copa_plan_fact.fran_grp,
                    edw_copa_plan_fact.oper_grp,
                    edw_copa_plan_fact.fisc_qtr,
                    edw_copa_plan_fact.matl2,
                    edw_copa_plan_fact.bill_typ,
                    edw_copa_plan_fact.fisc_wk,
                    edw_copa_plan_fact.amt_grp_crcy,
                    edw_copa_plan_fact.amt_obj_crcy,
                    edw_copa_plan_fact.crncy_key,
                    edw_copa_plan_fact.obj_crncy,
                    edw_copa_plan_fact.acct_num,
                    edw_copa_plan_fact.chrt_acct,
                    edw_copa_plan_fact.mgmt_entity,
                    edw_copa_plan_fact.sls_prsn_respons,
                    edw_copa_plan_fact.busn_area,
                    edw_copa_plan_fact.ga,
                    edw_copa_plan_fact.tc,
                    edw_copa_plan_fact.matl_plnt_view,
                    edw_copa_plan_fact.qty,
                    edw_copa_plan_fact.uom,
                    edw_copa_plan_fact.sls_vol_ieu,
                    edw_copa_plan_fact.un_sls_vol__ieu,
                    edw_copa_plan_fact.bpt_dstn_chnl,
                    edw_copa_plan_fact.acct_hier_desc,
                    edw_copa_plan_fact.acct_hier_shrt_desc,
                    edw_copa_plan_fact.category,
                    edw_copa_plan_fact.freq,
                    edw_copa_plan_fact.crt_dttm,
                    edw_copa_plan_fact.updt_dttm
                  FROM
                    edw_copa_plan_fact
                  WHERE
                    (
                      (
                        (
                          (
                            (edw_copa_plan_fact.co_cd):: text = ('7470' :: character varying):: text
                          )
                          OR (
                            (edw_copa_plan_fact.co_cd):: text = ('8361' :: character varying):: text
                          )
                        )
                        AND (
                          (
                            (
                              (edw_copa_plan_fact.category):: text = ('BP' :: character varying):: text
                            )
                            OR (
                              (edw_copa_plan_fact.category):: text = ('JU' :: character varying):: text
                            )
                          )
                          OR (
                            (edw_copa_plan_fact.category):: text = ('NU' :: character varying):: text
                          )
                        )
                      )
                      AND (
                        (
                          (
                            edw_copa_plan_fact.acct_hier_shrt_desc
                          ):: text = ('GTS' :: character varying):: text
                        )
                        OR (
                          (
                            edw_copa_plan_fact.acct_hier_shrt_desc
                          ):: text = ('NTS' :: character varying):: text
                        )
                      )
                    )
                ) ecpf
                LEFT JOIN edw_material_dim vmd ON (
                  (
                    CASE WHEN (
                      (
                        ltrim(
                          (ecpf.matl_num):: text,
                          (
                            (0):: character varying
                          ):: text
                        ) = ('46101731' :: character varying):: text
                      )
                      OR (
                        (
                          ltrim(
                            (ecpf.matl_num):: text,
                            (
                              (0):: character varying
                            ):: text
                          ) IS NULL
                        )
                        AND ('46101731' IS NULL)
                      )
                    ) THEN ('79619630' :: character varying):: text ELSE ltrim(
                      (ecpf.matl_num):: text,
                      (
                        (0):: character varying
                      ):: text
                    ) END = ltrim(
                      (vmd.matl_id):: text,
                      (
                        (0):: character varying
                      ):: text
                    )
                  )
                )
              )
              LEFT JOIN (
                vw_apo_parent_child_dim vapcd
                LEFT JOIN (
                  SELECT
                    DISTINCT vw_apo_parent_child_dim.master_code,
                    vw_apo_parent_child_dim.parent_matl_desc
                  FROM
                    vw_apo_parent_child_dim
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
                    vw_apo_parent_child_dim
                  WHERE
                    (
                      NOT (
                        vw_apo_parent_child_dim.master_code IN (
                          SELECT
                            DISTINCT vw_apo_parent_child_dim.master_code
                          FROM
                            vw_apo_parent_child_dim
                          WHERE
                            (
                              (vw_apo_parent_child_dim.cmp_id):: text = (
                                (7470):: character varying
                              ):: text
                            )
                        )
                      )
                    )
                ) mstrcd ON (
                  (
                    (vapcd.master_code):: text = (mstrcd.master_code):: text
                  )
                )
              ) ON (
                (
                  (
                    (ecpf.co_cd):: text = (vapcd.cmp_id):: text
                  )
                  AND (
                    CASE WHEN (
                      (
                        ltrim(
                          (ecpf.matl_num):: text,
                          (
                            (0):: character varying
                          ):: text
                        ) = ('46101731' :: character varying):: text
                      )
                      OR (
                        (
                          ltrim(
                            (ecpf.matl_num):: text,
                            (
                              (0):: character varying
                            ):: text
                          ) IS NULL
                        )
                        AND ('46101731' IS NULL)
                      )
                    ) THEN ('79619630' :: character varying):: text ELSE ltrim(
                      (ecpf.matl_num):: text,
                      (
                        (0):: character varying
                      ):: text
                    ) END = ltrim(
                      (vapcd.matl_id):: text,
                      (
                        (0):: character varying
                      ):: text
                    )
                  )
                )
              )
            )
            LEFT JOIN (
              SELECT
                DISTINCT edw_sapbw_plan_lkp.cmp_id,
                edw_sapbw_plan_lkp.chnl_cd,
                edw_sapbw_plan_lkp.chnl_desc,
                edw_sapbw_plan_lkp.country,
                edw_sapbw_plan_lkp.pln_chnl_cd,
                edw_sapbw_plan_lkp.pln_chnl_desc
              FROM
                edw_sapbw_plan_lkp
            ) spl ON (
              (
                (
                  (
                    CASE WHEN (
                      (
                        (ecpf.mercia_ref):: text = ('AU5' :: character varying):: text
                      )
                      OR (
                        (ecpf.mercia_ref IS NULL)
                        AND ('AU5' IS NULL)
                      )
                    ) THEN 'AUEX' :: character varying ELSE ecpf.mercia_ref END
                  ):: text = (spl.pln_chnl_cd):: text
                )
                AND (
                  (ecpf.co_cd):: text = (spl.cmp_id):: text
                )
              )
            )
          )
          LEFT JOIN (
            SELECT
              edw_gch_producthierarchy.materialnumber,
              edw_gch_producthierarchy.gcph_franchise,
              edw_gch_producthierarchy.gcph_brand,
              edw_gch_producthierarchy.gcph_subbrand,
              edw_gch_producthierarchy.gcph_variant,
              edw_gch_producthierarchy.gcph_needstate,
              edw_gch_producthierarchy.gcph_category,
              edw_gch_producthierarchy.gcph_subcategory,
              edw_gch_producthierarchy.gcph_segment,
              edw_gch_producthierarchy.gcph_subsegment
            FROM
              edw_gch_producthierarchy
            WHERE
              (
                (
                  ltrim(
                    (
                      edw_gch_producthierarchy.materialnumber
                    ):: text,
                    (
                      (0):: character varying
                    ):: text
                  ) <> ('' :: character varying):: text
                )
                AND (
                  (
                    edw_gch_producthierarchy."region"
                  ):: text = ('APAC' :: character varying):: text
                )
              )
          ) b ON (
            (
              ltrim(
                (ecpf.matl_num):: text,
                (
                  (0):: character varying
                ):: text
              ) = ltrim(
                (b.materialnumber):: text,
                (
                  (0):: character varying
                ):: text
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
                  (
                    (
                      (
                        "substring"(
                          (
                            (ecpf.fisc_yr_per):: character varying
                          ):: text,
                          1,
                          4
                        ) || "substring"(
                          (
                            (ecpf.fisc_yr_per):: character varying
                          ):: text,
                          6,
                          2
                        )
                      )
                    ):: integer
                  ):: numeric
                ):: numeric(18, 0) = etd.jj_mnth_id
              )
              AND (
                (ecpf.obj_crncy):: text = (currex.from_ccy):: text
              )
            )
            AND (
              etd.jj_mnth_id = currex.jj_mnth_id
            )
          )
          AND (
            jj_tot_wks.jj_mnth_id = etd.jj_mnth_id
          )
        )
    ) vsf
    LEFT JOIN (
      SELECT
        EDW_VW_MDS_COGS_RATE_DIM    .jj_year,
        EDW_VW_MDS_COGS_RATE_DIM    .jj_mnth_id,
        EDW_VW_MDS_COGS_RATE_DIM    .matl_id,
        EDW_VW_MDS_COGS_RATE_DIM    .crncy,
        EDW_VW_MDS_COGS_RATE_DIM    .cogs_per_unit
      FROM
        EDW_VW_MDS_COGS_RATE_DIM
    ) cogs ON (
      (
        (
          (
            ltrim(
              (vsf.matl_id):: text,
              (
                (0):: character varying
              ):: text
            ) = ltrim(
              (cogs.matl_id):: text,
              (
                (0):: character varying
              ):: text
            )
          )
          AND (vsf.jj_period = cogs.jj_mnth_id)
        )
        AND (
          (vsf.local_curr_cd):: text = (cogs.crncy):: text
        )
      )
    )
  )

)
select * from final