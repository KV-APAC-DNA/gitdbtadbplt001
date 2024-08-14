with sdl_mds_cn_ps_weights as
(
    select * from {{source('chnsdl_raw', 'sdl_mds_cn_ps_weights')}}
),
final as
(
    select
    re::varchar(20) as re,
	kpi::varchar(50) as kpi,
	weight::number(38,5) as weight,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	channel::varchar(100) as channel
    from sdl_mds_cn_ps_weights
)
select * from final