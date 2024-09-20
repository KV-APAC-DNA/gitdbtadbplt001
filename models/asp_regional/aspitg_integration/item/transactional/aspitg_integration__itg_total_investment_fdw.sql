with sdl_total_investment_fdw as
(
    select * from {{source('aspsdl_raw', 'sdl_total_investment_fdw')}}
),
trans as
(
    select
        CAST(posting_fiscal_year as integer) as posting_fiscal_year,
        CAST(posting_fiscal_period_number as integer) as posting_fiscal_period_number,
        version_group_code,
        mrc_code,
        mrc_counrty_code,
        mrc_name,
        cluster_name,
        country_name,
        bravo_product_cd,
        bravo_product_name,
        strng_product_lvl_02_desc,
        strng_product_lvl_03_desc,
        franchise,
        stronghold,
        gl_account_code,
        gl_account_desc,
        equalization_flag,
        description_level_00,
        kpi,
        description_level_01,
        description_level_02,
        cast(usdf_mtd_amount as numeric(38,14)) as usdf_mtd_amount,
        file_name,
        run_id,
	    convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) as UPDT_DTTM
    from  sdl_total_investment_fdw
),
final as
(
    select
    posting_fiscal_year::number(38,0) as posting_fiscal_year,
	posting_fiscal_period_number::number(38,0) as posting_fiscal_period_number,
	version_group_code::varchar(50) as version_group_code,
	mrc_code::varchar(50) as mrc_code,
	mrc_counrty_code::varchar(255) as mrc_counrty_code,
	mrc_name::varchar(255) as mrc_name,
	cluster_name::varchar(255) as cluster_name,
	country_name::varchar(255) as country_name,
	bravo_product_cd::varchar(255) as bravo_product_cd,
	bravo_product_name::varchar(255) as bravo_product_name,
	strng_product_lvl_02_desc::varchar(255) as strng_product_lvl_02_desc,
	strng_product_lvl_03_desc::varchar(255) as strng_product_lvl_03_desc,
	franchise::varchar(255) as franchise,
	stronghold::varchar(255) as stronghold,
	gl_account_code::varchar(255) as gl_account_code,
	gl_account_desc::varchar(255) as gl_account_desc,
	equalization_flag::varchar(255) as equalization_flag,
	description_level_00::varchar(255) as description_level_00,
	kpi::varchar(255) as kpi,
	description_level_01::varchar(255) as description_level_01,
	description_level_02::varchar(255) as description_level_02,
	usdf_mtd_amount::number(38,0) as usdf_mtd_amount,
	file_name::varchar(255) as file_name,
	run_id::number(14,0) as run_id,
	updt_dttm
    from trans
)
select * from final