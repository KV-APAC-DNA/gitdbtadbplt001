with source as
(
    select * from {{source('idnsdl_raw', 'sdl_mds_id_ps_targets')}}
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
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crtd_dttm
    from source
)
select * from final
