WITH source AS
(
    SELECT * FROM {{ ref('pcfitg_integration__itg_cust_ciw_plan_fact') }}
),
final AS 
(
    SELECT
        time_period::varchar(10) AS time_period,
        sales_grp_cd::varchar(10) AS sales_grp_cd,
        prod_mjr_cd::varchar(30) AS prod_mjr_cd,
        
        -- GOAL values
        MAX(CASE WHEN UPPER(target_type) = 'GOAL' THEN goal_qty END)::number(18,0) AS goal_qty,
        MAX(CASE WHEN UPPER(target_type) = 'GOAL' THEN goal_gts END)::number(18,2) AS goal_gts,
        MAX(CASE WHEN UPPER(target_type) = 'GOAL' THEN goal_eff_val END)::number(18,2) AS goal_eff_val,
        MAX(CASE WHEN UPPER(target_type) = 'GOAL' THEN goal_jgf_si_val END)::number(18,2) AS goal_jgf_si_val,
        MAX(CASE WHEN UPPER(target_type) = 'GOAL' THEN goal_pmt_terms_val END)::number(18,2) AS goal_pmt_terms_val,
        MAX(CASE WHEN UPPER(target_type) = 'GOAL' THEN goal_datains_val END)::number(18,2) AS goal_datains_val,
        MAX(CASE WHEN UPPER(target_type) = 'GOAL' THEN goal_exp_adj_val END)::number(18,2) AS goal_exp_adj_val,
        MAX(CASE WHEN UPPER(target_type) = 'GOAL' THEN goal_jgf_sd_val END)::number(18,2) AS goal_jgf_sd_val,
        MAX(CASE WHEN UPPER(target_type) = 'GOAL' THEN goal_ciw_tot END)::number(18,2) AS goal_ciw_tot,
        MAX(CASE WHEN UPPER(target_type) = 'GOAL' THEN goal_cogs END)::number(18,2) AS goal_cogs,
        MAX(CASE WHEN UPPER(target_type) = 'GOAL' THEN goal_gp END)::number(18,2) AS goal_gp,
        
        -- JU values
        MAX(CASE WHEN UPPER(target_type) = 'JU' THEN goal_qty END)::number(18,0) AS ju_qty,
        MAX(CASE WHEN UPPER(target_type) = 'JU' THEN goal_gts END)::number(18,2) AS ju_gts,
        MAX(CASE WHEN UPPER(target_type) = 'JU' THEN goal_eff_val END)::number(18,2) AS ju_eff_val,
        MAX(CASE WHEN UPPER(target_type) = 'JU' THEN goal_jgf_si_val END)::number(18,2) AS ju_jgf_si_val,
        MAX(CASE WHEN UPPER(target_type) = 'JU' THEN goal_pmt_terms_val END)::number(18,2) AS ju_pmt_terms_val,
        MAX(CASE WHEN UPPER(target_type) = 'JU' THEN goal_datains_val END)::number(18,2) AS ju_datains_val,
        MAX(CASE WHEN UPPER(target_type) = 'JU' THEN goal_exp_adj_val END)::number(18,2) AS ju_exp_adj_val,
        MAX(CASE WHEN UPPER(target_type) = 'JU' THEN goal_jgf_sd_val END)::number(18,2) AS ju_jgf_sd_val,
        MAX(CASE WHEN UPPER(target_type) = 'JU' THEN goal_ciw_tot END)::number(18,2) AS ju_ciw_tot,
        MAX(CASE WHEN UPPER(target_type) = 'JU' THEN goal_cogs END)::number(18,2) AS ju_cogs,
        MAX(CASE WHEN UPPER(target_type) = 'JU' THEN goal_gp END)::number(18,2) AS ju_gp,

        -- NU values
        MAX(CASE WHEN UPPER(target_type) = 'NU' THEN goal_qty END)::number(18,0) AS nu_qty,
        MAX(CASE WHEN UPPER(target_type) = 'NU' THEN goal_gts END)::number(18,2) AS nu_gts,
        MAX(CASE WHEN UPPER(target_type) = 'NU' THEN goal_eff_val END)::number(18,2) AS nu_eff_val,
        MAX(CASE WHEN UPPER(target_type) = 'NU' THEN goal_jgf_si_val END)::number(18,2) AS nu_jgf_si_val,
        MAX(CASE WHEN UPPER(target_type) = 'NU' THEN goal_pmt_terms_val END)::number(18,2) AS nu_pmt_terms_val,
        MAX(CASE WHEN UPPER(target_type) = 'NU' THEN goal_datains_val END)::number(18,2) AS nu_datains_val,
        MAX(CASE WHEN UPPER(target_type) = 'NU' THEN goal_exp_adj_val END)::number(18,2) AS nu_exp_adj_val,
        MAX(CASE WHEN UPPER(target_type) = 'NU' THEN goal_jgf_sd_val END)::number(18,2) AS nu_jgf_sd_val,
        MAX(CASE WHEN UPPER(target_type) = 'NU' THEN goal_ciw_tot END)::number(18,2) AS nu_ciw_tot,
        MAX(CASE WHEN UPPER(target_type) = 'NU' THEN goal_cogs END)::number(18,2) AS nu_cogs,
        MAX(CASE WHEN UPPER(target_type) = 'NU' THEN goal_gp END)::number(18,2) AS nu_gp,

        -- BP values
        MAX(CASE WHEN UPPER(target_type) = 'BP' THEN goal_qty END)::number(18,0) AS bp_qty,
        MAX(CASE WHEN UPPER(target_type) = 'BP' THEN goal_gts END)::number(18,2) AS bp_gts,
        MAX(CASE WHEN UPPER(target_type) = 'BP' THEN goal_eff_val END)::number(18,2) AS bp_eff_val,
        MAX(CASE WHEN UPPER(target_type) = 'BP' THEN goal_jgf_si_val END)::number(18,2) AS bp_jgf_si_val,
        MAX(CASE WHEN UPPER(target_type) = 'BP' THEN goal_pmt_terms_val END)::number(18,2) AS bp_pmt_terms_val,
        MAX(CASE WHEN UPPER(target_type) = 'BP' THEN goal_datains_val END)::number(18,2) AS bp_datains_val,
        MAX(CASE WHEN UPPER(target_type) = 'BP' THEN goal_exp_adj_val END)::number(18,2) AS bp_exp_adj_val,
        MAX(CASE WHEN UPPER(target_type) = 'BP' THEN goal_jgf_sd_val END)::number(18,2) AS bp_jgf_sd_val,
        MAX(CASE WHEN UPPER(target_type) = 'BP' THEN goal_ciw_tot END)::number(18,2) AS bp_ciw_tot,
        MAX(CASE WHEN UPPER(target_type) = 'BP' THEN goal_cogs END)::number(18,2) AS bp_cogs,
        MAX(CASE WHEN UPPER(target_type) = 'BP' THEN goal_gp END)::number(18,2) AS bp_gp,

        local_ccy::varchar(10) AS local_ccy,
        current_timestamp()::timestamp_ntz(9) AS crt_dttm
    FROM source
    GROUP BY 
        time_period, sales_grp_cd, prod_mjr_cd, local_ccy
)
SELECT * FROM final
