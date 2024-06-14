with source as
(
    select * from {{ source('ntasdl_raw', 'sdl_mds_tw_ps_weights') }}
),
final as 
(   
    select 
    channel::varchar(100) as channel,
	re::varchar(100) as retail_env,
	kpi::varchar(100) as kpi,
	weight::number(20,4) as weight,
	current_timestamp()::timestamp_ntz(9) as crtd_dttm
    from source
)
select * from final