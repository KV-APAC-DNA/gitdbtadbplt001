with source as(
    select * from {{ source('phlsdl_raw', 'sdl_mds_ph_ps_weights') }}
),
final as(
    select 
        channel::varchar(100) as channel,
        re_id::varchar(100) as retail_env,
        kpi::varchar(100) as kpi,
        weight::number(20,4) as weight,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        to_date(valid_from) as valid_from,
        to_date(valid_to) as valid_to
    from source
)
select * from final