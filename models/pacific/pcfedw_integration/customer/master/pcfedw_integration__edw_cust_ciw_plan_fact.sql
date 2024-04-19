with source as
(
    select * from {{ ref('pcfitg_integration__itg_cust_ciw_plan_fact') }}
),
final as 
(
select
    time_period::varchar(10) as time_period,
	sales_grp_cd::varchar(10) as sales_grp_cd,
	prod_mjr_cd::varchar(30) as prod_mjr_cd,
	goal_qty::number(18,0) as goal_qty,
	goal_gts::number(18,2) as goal_gts,
	goal_eff_val::number(18,2) as goal_eff_val,
	goal_jgf_si_val::number(18,2) as goal_jgf_si_val,
	goal_pmt_terms_val::number(18,2) as goal_pmt_terms_val,
	goal_datains_val::number(18,2) as goal_datains_val,
	goal_exp_adj_val::number(18,2) as goal_exp_adj_val,
	goal_jgf_sd_val::number(18,2) as goal_jgf_sd_val,
	goal_ciw_tot::number(18,2) as goal_ciw_tot,
	goal_cogs::number(18,2) as goal_cogs,
	goal_gp::number(18,2) as goal_gp,
	local_ccy::varchar(10) as local_ccy,
	current_timestamp()::timestamp_ntz(9) as crt_dttm
from source
)
select * from final