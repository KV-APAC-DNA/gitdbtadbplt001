{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",     
        pre_hook = " {% if is_incremental() %}
        DELETE
        FROM {{this}} 
        WHERE 0 != (SELECT COUNT(*) FROM {{source('idnsdl_raw','sdl_mds_id_lav_sellin_target')}});
        {% endif %}"
    )
}}
with source as
(
    select * from {{source('idnsdl_raw','sdl_mds_id_lav_sellin_target')}}
),
final as 
(
    select 
        upper(TRIM(region_code))::varchar(500)as region_code,
		upper(TRIM(region_name))::varchar(500)as region_name,
		region_id::number(18,0) as region_id,
		upper(TRIM(distrbutor_code_code))::varchar(500)as distrbutor_code_code,
		upper(TRIM(distrbutor_code_name))::varchar(500) as distrbutor_code_name,
		distrbutor_code_id::number(18,0) as distrbutor_code_id,
		upper(TRIM(business_unit_code))::varchar(500) as business_unit_code,
		upper(TRIM(business_unit_name))::varchar(500) as business_unit_name,
		business_unit_id::number(18,0) as business_unit_id,
		upper(TRIM(franchise_code))::varchar(500) as franchise_code,
		upper(TRIM(franchise_name))::varchar(500) as franchise_name,
		franchise_id::number(18,0) as franchise_id,
		upper(TRIM(brand_code))::varchar(500) as brand_code,
		upper(TRIM(brand_name))::varchar(500) as brand_name,
		brand_id::number(18,0) as brand_id,
		jj_year_month::varchar(30) as jj_year_month,
        replace(TRIM(niv),',','.')::number(25,7) as niv,
        replace(TRIM(hna),',','.')::number(25,7) as hna,
		enterdatetime::timestamp_ntz(9) as enterdatetime,
		lastchgdatetime::timestamp_ntz(9) as lastchgdatetime
    from source
)

select * from final
