{{
    config(
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook = "{% if is_incremental() %}
                    delete from {{ this }} where 0 != (
                    select count(*) from {{ source('jpnsdl_raw', 'sdl_mds_jp_ps_weights') }});
                    {% endif %}"
        
    )
}}

with source as(
    select * from {{ source('jpnsdl_raw', 'sdl_mds_jp_ps_weights') }}
)
,
result as(
    select 
        	channel::varchar(255) as channel,
	        kpi::varchar(255) as kpi,
	        weight::number(20,4) as weight,
	        current_timestamp::timestamp_ntz(9) as crted_dttm,
	        name::varchar(100) as retail_en
    from source
)

select * from result