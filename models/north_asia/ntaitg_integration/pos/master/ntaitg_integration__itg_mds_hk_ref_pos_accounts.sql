with source as(
    select * from {{ source('ntasdl_raw', 'sdl_mds_hk_ref_pos_accounts') }}
),
final as(
    select 
        name::varchar(500) as name,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm
    FROM source
)
select * from final