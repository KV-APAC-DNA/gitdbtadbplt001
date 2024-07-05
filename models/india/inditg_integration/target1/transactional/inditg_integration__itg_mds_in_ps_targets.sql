{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                    delete from {{this}} WHERE (SELECT COUNT(*) from {{ source('indsdl_raw', 'sdl_mds_in_ps_targets') }}) != 0;
                    {% endif %}"
    )
}}

with sdl_mds_in_ps_targets as 
(
    select * from {{ source('indsdl_raw', 'sdl_mds_in_ps_targets') }}
),
final as 
(
    select
	NAME::varchar(50) as kpi,
	RE::varchar(200) as retail_env,
	attribute_1::varchar(50) as attribute_1,
	attribute_2::varchar(100) as attribute_2,
	channel::varchar(100) as channel,
	value::number(20,4) as target,
	current_timestamp()::timestamp_ntz(9) as crtd_dttm
    from sdl_mds_in_ps_targets
)
select * from final
