{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        DELETE FROM {{this}};
        {% endif %}"
    )
}}
with sdl_mds_cn_otc_soldto_mapping as
(
    select * from {{source('chnsdl_raw', 'sdl_mds_cn_otc_soldto_mapping')}}
),
final as
(
    select
    name::varchar(500) as name,
	code::varchar(500) as code,
	xjp_soldto::varchar(200) as xjp_soldto,
	jntl_soldto::varchar(200) as jntl_soldto
    from sdl_mds_cn_otc_soldto_mapping
)
select * from final