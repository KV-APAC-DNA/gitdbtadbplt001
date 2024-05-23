with source as
(
    select * from {{source('idnsdl_raw','sdl_mds_id_lav_sellin_target')}}
),
final as 
(
    select 
        region_code:: varchar(500)as region_code,
		region_name:: varchar(500)as region_name,
		region_id:: number(18,0) as region_id,
		distrbutor_code_code:: varchar(500)as distrbutor_code_code,
		distrbutor_code_name:: varchar(500) as distrbutor_code_name,
		distrbutor_code_id:: number(18,0) as distrbutor_code_id,
		business_unit_code:: varchar(500) as business_unit_code,
		business_unit_name:: varchar(500) as business_unit_name,
		business_unit_id:: number(18,0) as business_unit_id,
		franchise_code:: varchar(500) as franchise_code,
		franchise_name:: varchar(500) as franchise_name,
		franchise_id:: number(18,0) as franchise_id,
		brand_code:: varchar(500) as brand_code,
		brand_name:: varchar(500) as brand_name,
		brand_id:: number(18,0) as brand_id,
		jj_year_month:: varchar(30) as jj_year_month,
		niv:: number(25,7) as niv,
		hna:: number(25,7) as hna,
		enterdatetime:: timestamp_ntz(9) as enterdatetime,
		lastchgdatetime:: timestamp_ntz(9) as lastchgdatetime
    from source
)

select * from final
