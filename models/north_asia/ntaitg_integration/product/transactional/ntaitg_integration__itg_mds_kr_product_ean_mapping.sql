with source as
(
   -- select * from {{ source('ntasdl_raw', 'sdl_mds_kr_product_ean_mapping') }}
   select * from DEV_DNA_LOAD.SNAPNTASDL_RAW.SDL_MDS_KR_PRODUCT_EAN_MAPPING
),
final as
(
    select
        id::number(18,0) as id,
        code::varchar(500) as code,
        vendoritemid::number(31,0) as vendoritemid,
        skuid::number(31,0) as skuid,
        ean::varchar(200) as ean,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crtd_dttm
    from source
)
select * from final


