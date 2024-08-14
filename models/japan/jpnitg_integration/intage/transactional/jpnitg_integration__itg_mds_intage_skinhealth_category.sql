{{
    config(
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook = "{% if is_incremental() %}
                    delete from {{ this }} where trim(jan_code) in (
                    select trim(jan_code) from {{ source('jpnsdl_raw', 'sdl_mds_jp_intage_skinhealth_category') }});
                    {% endif %}"
        
    )
}}

with source as(
    select * from {{ source('jpnsdl_raw', 'sdl_mds_jp_intage_skinhealth_category') }}
)
,
result as(
    select 
        trim(jan_code)::varchar(50) as jan_code,
	    nvl(trim(product), 'N/A')::varchar(200) as product,
	    nvl(trim(brand_code), 'N/A')::varchar(100) as brand_code,
	    nvl(trim(brand_name), 'N/A')::varchar(100) as brand_name,
	    nvl(trim(category_code), 'N/A')::varchar(100) as category_code,
	    nvl(trim(category_name), 'N/A')::varchar(100) as category_name,
	    nvl(trim(sub_category_code), 'N/A')::varchar(100) as sub_category_code,
	    nvl(trim(sub_category_name), 'N/A')::varchar(100) as sub_category_name,
	    current_timestamp()::timestamp_ntz(9) as create_dt
    from source
)

select * from result