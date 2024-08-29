with sdl_mds_cn_ecom_brand as
(
    select * from {{source('chnsdl_raw', 'sdl_mds_cn_ecom_brand')}}
),
final as
(
    select
    convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as load_date,
    map2.code::varchar(500) as code,
    map2.name::varchar(500) as brand_name,
    map2.brand_cn::varchar(255) as brand_cn,
    map2.brand_code::varchar(255) as brand_code
    from sdl_mds_cn_ecom_brand map2
)
select * from final