with source as(
    select * from {{ source('phlsdl_raw', 'sdl_mds_ph_msl_dtls') }}
),
final as(
    select 
        code::varchar(50) as msl_dtl_code,
        msl_hdr_code_code::varchar(50) as msl_hdr_code,
        msl_hdr_code_name::varchar(100) as msl_hdr_name,
        msl_hdr_code_id::number(18,0) as msl_hdr_id,
        sku_code::varchar(50) as sku_code,
        sku_name::varchar(200) as sku_name,
        sku_id::number(18,0) as sku_id,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final