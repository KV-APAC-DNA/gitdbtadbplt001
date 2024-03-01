with source as(
    select * from {{ source('thasdl_raw', 'sdl_mds_th_ref_district') }}
),

final as(
    select
    trim(code)::varchar(10) as dist,
    trim(name)::varchar(100) as dist_nm,
    to_char(current_timestamp()::timestampntz(9)), 'YYYYMMDDHH24MISSFF3')::varchar(255) as cdl_dttm,
    current_timestamp()::timestampntz(9) as crtd_dttm,
    current_timestamp()::timestampntz(9) as updt_dttm
    from source
)
select * from final