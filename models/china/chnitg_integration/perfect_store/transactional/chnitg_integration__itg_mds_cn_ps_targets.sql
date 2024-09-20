with sdl_mds_cn_ps_target as
(
    select * from {{source('chnsdl_raw', 'sdl_mds_cn_ps_target')}}
),
final as
(
    select
    channel::varchar(100) as channel,
	re::varchar(100) as retail_env,
	kpi::varchar(100) as kpi,
	attribute_1::varchar(100) as attribute_1,
	attribute_2::varchar(100) as attribute_2,
	value::number(20,4) as target,
    current_timestamp()::timestamp_ntz(9) as crtd_dttm
    from sdl_mds_cn_ps_target
)
select * from final