{{
    config
    (
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["fisc_yr_per","co_cd","sls_org","dstn_chnl","div","matl","cust_num"],
        pre_hook="{% if is_incremental() %}
        delete from {{this }} as edw_internal_target_fact
        using {{ ref('ntaitg_integration__itg_tw_sales_target') }} as wks_edw_internal_target_fact
        where  wks_edw_internal_target_fact.fisc_yr_per=edw_internal_target_fact.fisc_yr_per
        and wks_edw_internal_target_fact.co_cd=edw_internal_target_fact.co_cd
        and wks_edw_internal_target_fact.sls_org=edw_internal_target_fact.sls_org
        and wks_edw_internal_target_fact.dstn_chnl=edw_internal_target_fact.dstn_chnl
        and wks_edw_internal_target_fact.div=edw_internal_target_fact.div
        and wks_edw_internal_target_fact.matl=edw_internal_target_fact.matl
        and wks_edw_internal_target_fact.cust_num=edw_internal_target_fact.cust_num;
        {% endif %}"
    )
}}
with source as
(
    select * from {{ ref('ntaitg_integration__itg_tw_sales_target') }}
),
final as
(
    select 
	fisc_yr_per::varchar(12) as fisc_yr_per,
	j_j_fisc_wk::varchar(50) as j_j_fisc_wk,
	co_cd::varchar(4) as co_cd,
	val_type::varchar(50) as val_type,
	vers::number(18,0) as vers,
	man_type::varchar(10) as man_type,
	crncy::varchar(5) as crncy,
	sls_org::varchar(4) as sls_org,
	dstn_chnl::varchar(2) as dstn_chnl,
	div::varchar(5) as div,
	matl::varchar(10) as matl,
	mega_brnd_b1::varchar(50) as mega_brnd_b1,
	brnd_b2::varchar(50) as brnd_b2,
	base_prod_b3::varchar(50) as base_prod_b3,
	vrnt_b4::varchar(50) as vrnt_b4,
	put_up_b5::varchar(50) as put_up_b5,
	oper_grp::varchar(50) as oper_grp,
	fran_grp::varchar(50) as fran_grp,
	fran::varchar(50) as fran,
	prod_fran::varchar(50) as prod_fran,
	prod_maj::varchar(50) as prod_maj,
	prod_minor::varchar(50) as prod_minor,
	cur_sls_emp::varchar(50) as cur_sls_emp,
	cust_num::number(18,0) as cust_num,
	lcl_cust_grp_1::varchar(50) as lcl_cust_grp_1,
	lcl_cust_grp_2::varchar(50) as lcl_cust_grp_2,
	lcl_cust_grp_3::varchar(50) as lcl_cust_grp_3,
	lcl_cust_grp_4::varchar(50) as lcl_cust_grp_4,
	lcl_cust_grp_5::varchar(50) as lcl_cust_grp_5,
	lcl_cust_grp_6::varchar(50) as lcl_cust_grp_6,
	sls_trgt::number(20,5) as sls_trgt,
	qty::number(18,0) as qty,
	unit::varchar(50) as unit,
	fisc_vrnt::varchar(5) as fisc_vrnt,
	fisc_yr::number(18,0) as fisc_yr,
	ctry::varchar(5) as ctry,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final