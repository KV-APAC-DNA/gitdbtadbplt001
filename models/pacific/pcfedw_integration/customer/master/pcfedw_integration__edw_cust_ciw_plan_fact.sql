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
        
        -- GOAL values ((target type is null and left(time_period, 4)<2024)?
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'GOAL' OR (target_type IS NULL AND left(time_period, 4)<2024) THEN goal_qty END))::number(18,0) AS goal_qty,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'GOAL' OR (target_type IS NULL AND left(time_period, 4)<2024) THEN goal_gts END))::number(18,2) AS goal_gts,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'GOAL' OR (target_type IS NULL AND left(time_period, 4)<2024) THEN goal_eff_val END))::number(18,2) AS goal_eff_val,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'GOAL' OR (target_type IS NULL AND left(time_period, 4)<2024) THEN goal_jgf_si_val END))::number(18,2) AS goal_jgf_si_val,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'GOAL' OR (target_type IS NULL AND left(time_period, 4)<2024) THEN goal_pmt_terms_val END))::number(18,2) AS goal_pmt_terms_val,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'GOAL' OR (target_type IS NULL AND left(time_period, 4)<2024) THEN goal_datains_val END))::number(18,2) AS goal_datains_val,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'GOAL' OR (target_type IS NULL AND left(time_period, 4)<2024) THEN goal_exp_adj_val END))::number(18,2) AS goal_exp_adj_val,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'GOAL' OR (target_type IS NULL AND left(time_period, 4)<2024) THEN goal_jgf_sd_val END))::number(18,2) AS goal_jgf_sd_val,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'GOAL' OR (target_type IS NULL AND left(time_period, 4)<2024) THEN goal_ciw_tot END))::number(18,2) AS goal_ciw_tot,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'GOAL' OR (target_type IS NULL AND left(time_period, 4)<2024) THEN goal_cogs END))::number(18,2) AS goal_cogs,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'GOAL' OR (target_type IS NULL AND left(time_period, 4)<2024) THEN goal_gp END))::number(18,2) AS goal_gp,
        
        -- JU values
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'JU' THEN goal_qty END))::number(18,0) AS ju_qty,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'JU' THEN goal_gts END))::number(18,2) AS ju_gts,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'JU' THEN goal_eff_val END))::number(18,2) AS ju_eff_val,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'JU' THEN goal_jgf_si_val END))::number(18,2) AS ju_jgf_si_val,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'JU' THEN goal_pmt_terms_val END))::number(18,2) AS ju_pmt_terms_val,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'JU' THEN goal_datains_val END))::number(18,2) AS ju_datains_val,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'JU' THEN goal_exp_adj_val END))::number(18,2) AS ju_exp_adj_val,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'JU' THEN goal_jgf_sd_val END))::number(18,2) AS ju_jgf_sd_val,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'JU' THEN goal_ciw_tot END))::number(18,2) AS ju_ciw_tot,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'JU' THEN goal_cogs END))::number(18,2) AS ju_cogs,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'JU' THEN goal_gp END))::number(18,2) AS ju_gp,

        -- NU values
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'NU' THEN goal_qty END))::number(18,0) AS nu_qty,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'NU' THEN goal_gts END))::number(18,2) AS nu_gts,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'NU' THEN goal_eff_val END))::number(18,2) AS nu_eff_val,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'NU' THEN goal_jgf_si_val END))::number(18,2) AS nu_jgf_si_val,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'NU' THEN goal_pmt_terms_val END))::number(18,2) AS nu_pmt_terms_val,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'NU' THEN goal_datains_val END))::number(18,2) AS nu_datains_val,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'NU' THEN goal_exp_adj_val END))::number(18,2) AS nu_exp_adj_val,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'NU' THEN goal_jgf_sd_val END))::number(18,2) AS nu_jgf_sd_val,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'NU' THEN goal_ciw_tot END))::number(18,2) AS nu_ciw_tot,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'NU' THEN goal_cogs END))::number(18,2) AS nu_cogs,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'NU' THEN goal_gp END))::number(18,2) AS nu_gp,

        -- BP values
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'BP' THEN goal_qty END))::number(18,0) AS bp_qty,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'BP' THEN goal_gts END))::number(18,2) AS bp_gts,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'BP' THEN goal_eff_val END))::number(18,2) AS bp_eff_val,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'BP' THEN goal_jgf_si_val END))::number(18,2) AS bp_jgf_si_val,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'BP' THEN goal_pmt_terms_val END))::number(18,2) AS bp_pmt_terms_val,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'BP' THEN goal_datains_val END))::number(18,2) AS bp_datains_val,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'BP' THEN goal_exp_adj_val END))::number(18,2) AS bp_exp_adj_val,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'BP' THEN goal_jgf_sd_val END))::number(18,2) AS bp_jgf_sd_val,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'BP' THEN goal_ciw_tot END))::number(18,2) AS bp_ciw_tot,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'BP' THEN goal_cogs END))::number(18,2) AS bp_cogs,
        ZEROIFNULL((CASE WHEN UPPER(target_type) = 'BP' THEN goal_gp END))::number(18,2) AS bp_gp,

        local_ccy::varchar(10) AS local_ccy,
        current_timestamp()::timestamp_ntz(9) AS crt_dttm
    FROM source
    --GROUP BY time_period, sales_grp_cd, prod_mjr_cd, local_ccy
)
SELECT * FROM final
