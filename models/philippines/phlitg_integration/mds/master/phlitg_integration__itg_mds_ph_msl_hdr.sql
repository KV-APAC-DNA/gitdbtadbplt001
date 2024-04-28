with source as(
    select * from {{ source('phlsdl_raw', 'sdl_mds_ph_msl_hdr') }}
),
final as(
    select 
        code::varchar(50) as msl_hdr_code,
        csg_code_code::varchar(50) as csg_code,
        csg_code_name::varchar(100) as csg_name,
        csg_code_id::number(18,0) as csg_id,
        to_date (fr_salescycle, 'YYYYMM') as from_salescycle,
        to_date (to_salescycle, 'YYYYMM') as to_salescycle,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final