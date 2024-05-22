{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        unique_key= ["year"],
        pre_hook = "delete from {{this}} where cast(year as integer) in (
        select cast(year as integer) from {{ source('idnsdl_raw', 'sdl_mds_id_lav_hero_list') }});"
    )
}}
with source as 
(
    select * from {{ source('idnsdl_raw', 'sdl_mds_id_lav_hero_list') }}
),
final as 
(
    select
    year::varchar(10) as year,
	region::varchar(50) as region,
	replace(distributor_code, '.0', '')::varchar(50) as jj_sap_dstrbtr_id,
	sap_code::varchar(50) as jj_sap_cd_mp_prod_id,
	target_growth::number(18,4) as target_growth,
	target_coverage::number(18,4) as target_coverage
    from source
)
select * from final