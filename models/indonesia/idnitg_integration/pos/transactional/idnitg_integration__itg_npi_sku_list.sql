{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        unique_key= ["year"],
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} where cast(year as integer) in (
        select cast(year as integer) from {{ source('idnsdl_raw', 'sdl_mds_id_lav_npi_sku_list') }});
        {% endif %}"
    )
}}
with source as 
(
    select * from {{ source('idnsdl_raw', 'sdl_mds_id_lav_npi_sku_list') }}
),
final as 
(
    select
    year::varchar(75) as year,
	npi_sku_code::varchar(75) as npi_sku_code,
	sku_npi::varchar(75) as sku_npi,
	benchmark_sku_code::varchar(75) as benchmark_sku_code,
	sku_benchmark::varchar(75) as sku_benchmark
    from source
)
select * from final