with source as(
    select * from {{ source('thasdl_raw', 'sdl_mds_th_ps_handeye_level') }}
),

final as(
    select
        country::varchar(20) as country,
        retail_environment_name::varchar(200) as retail_environment,
        total_layers::integer as total_layers,
        hand_eye_layer::integer as hand_eye_layer,
        current_timestamp()::timestampntz(9) as crtd_dttm
    from source
)
select * from final