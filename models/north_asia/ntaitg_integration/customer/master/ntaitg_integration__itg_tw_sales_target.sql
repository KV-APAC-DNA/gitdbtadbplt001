{{
    config
    (
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["fisc_yr_per","co_cd","sls_org","dstn_chnl","div","matl","cust_num"],
        pre_hook="delete from {{this}} as itg_tw_sales_target
        using {{ ref('ntawks_integration__wks_itg_tw_sales_target') }} as wks_itg_tw_sales_target
        where  wks_itg_tw_sales_target.fiscal_year_period=itg_tw_sales_target.fisc_yr_per
        and wks_itg_tw_sales_target.company_code=itg_tw_sales_target.co_cd
        and wks_itg_tw_sales_target.sales_organization=itg_tw_sales_target.sls_org
        and wks_itg_tw_sales_target.distribution_channel=itg_tw_sales_target.dstn_chnl
        and wks_itg_tw_sales_target.division=itg_tw_sales_target.div
        and wks_itg_tw_sales_target.material=itg_tw_sales_target.matl
        and wks_itg_tw_sales_target.customer_number=itg_tw_sales_target.cust_num ;"
    )
}}
with source as
(
    select * from {{ ref('ntawks_integration__wks_itg_tw_sales_target') }}
),
final as
(
    select 
	fiscal_year_period::varchar(12) as fisc_yr_per,
	j_j_fiscal_week::varchar(50) as j_j_fisc_wk,
	company_code::varchar(4) as co_cd,
	value_type::varchar(50) as val_type,
	version::number(18,0) as vers,
	manual_type::varchar(10) as man_type,
	currency::varchar(5) as crncy,
	sales_organization::varchar(4) as sls_org,
	distribution_channel::varchar(2) as dstn_chnl,
	division::varchar(5) as div,
	material::varchar(10) as matl,
	b1_mega_brand::varchar(50) as mega_brnd_b1,
	b2_brand::varchar(50) as brnd_b2,
	b3_base_product::varchar(50) as base_prod_b3,
	b4_variant::varchar(50) as vrnt_b4,
	b5_put_up::varchar(50) as put_up_b5,
	operating_group::varchar(50) as oper_grp,
	franchise_group::varchar(50) as fran_grp,
	franchise::varchar(50) as fran,
	product_franchise::varchar(50) as prod_fran,
	product_major::varchar(50) as prod_maj,
	product_minor::varchar(50) as prod_minor,
	current_sales_employee::varchar(50) as cur_sls_emp,
	customer_number::number(18,0) as cust_num,
	local_customer_grp_1::varchar(50) as lcl_cust_grp_1,
	local_customer_grp_2::varchar(50) as lcl_cust_grp_2,
	local_customer_grp_3::varchar(50) as lcl_cust_grp_3,
	local_customer_grp_4::varchar(50) as lcl_cust_grp_4,
	local_customer_grp_5::varchar(50) as lcl_cust_grp_5,
	local_customer_grp_6::varchar(50) as lcl_cust_grp_6,
	sales_target::number(20,5) as sls_trgt,
	quantity::number(18,0) as qty,
	unit::varchar(50) as unit,
	fiscal_variant::varchar(5) as fisc_vrnt,
	fiscal_year::number(18,0) as fisc_yr,
	country::varchar(5) as ctry,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final