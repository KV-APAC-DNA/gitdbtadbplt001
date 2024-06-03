with source as
(
    select * from {{source('idnsdl_raw', 'sdl_mds_id_ps_weights')}}
),
final as
(
    select
        trim(channel)::varchar(20) as channel,
        cast(trim(weight) as numeric(38,5)) as weight,
        trim(kpi)::varchar(50) as kpi,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm,
        re::varchar(100) as retail_env
    from source
)
select * from final
