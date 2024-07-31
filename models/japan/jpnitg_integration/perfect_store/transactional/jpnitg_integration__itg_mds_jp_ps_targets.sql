{{
    config(
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook = "{% if is_incremental() %}
                    delete from {{ this }} where 0 != (
                    select count(*) from {{ source('jpnsdl_raw', 'sdl_mds_jp_ps_targets') }});
                    {% endif %}"
        
    )
}}

with source as(
    select * from {{ source('jpnsdl_raw', 'sdl_mds_jp_ps_targets') }}
)
,
result as(
    select 
        name::varchar(200) as kpi,
	    attribute_1::varchar(255) as attribute_1,
	    attribute_2::varchar(255) as attribute_2,
	    value::number(20,4) as target,
	    current_timestamp()::timestamp_ntz(9) as crtd_dttm,
	    channel::varchar(100) as channel,
	    re::varchar(100) as retail_env
    from source
)

select * from result