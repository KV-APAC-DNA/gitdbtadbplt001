{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['time_period'],
        pre_hook= "
        create or replace TABLE PROD_DNA_CORE.DBT_CLOUD_PR_5458_1369.PCFITG_INTEGRATION__ITG_CUST_CIW_PLAN_FACT (
	TIME_PERIOD VARCHAR(10),
	SALES_GRP_CD VARCHAR(10),
	PROD_MJR_CD VARCHAR(30),
	GOAL_QTY NUMBER(18,0),
	GOAL_GTS NUMBER(18,2),
	GOAL_EFF_VAL NUMBER(18,2),
	GOAL_JGF_SI_VAL NUMBER(18,2),
	GOAL_PMT_TERMS_VAL NUMBER(18,2),
	GOAL_DATAINS_VAL NUMBER(18,0),
	GOAL_EXP_ADJ_VAL NUMBER(18,2),
	GOAL_JGF_SD_VAL NUMBER(18,2),
	GOAL_CIW_TOT NUMBER(18,2),
	GOAL_COGS NUMBER(18,0),
	GOAL_GP NUMBER(18,2),
	LOCAL_CCY VARCHAR(10),
    target_type::varchar(10) as target_type
);
        
        delete from {{this}} where left(time_period, 4) in (
        select left(time_period, 4) from {{ source('pcfsdl_raw', 'sdl_mds_pacific_cust_ciw_plan_temp') }});"
    )
}}
with source as 
(
    select * from {{ source('pcfsdl_raw', 'sdl_mds_pacific_cust_ciw_plan_temp') }}
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
	goal_datains_val::number(18,0) as goal_datains_val,
	goal_exp_adj_val::number(18,2) as goal_exp_adj_val,
	goal_jgf_sd_val::number(18,2) as goal_jgf_sd_val,
	goal_ciw_tot::number(18,2) as goal_ciw_tot,
	goal_cogs::number(18,0) as goal_cogs,
	goal_gp::number(18,2) as goal_gp,
	local_ccy::varchar(10) as local_ccy,
    target_type::varchar(10) as target_type
from source
)
select * from final