with source as
(
    select * from {{source('idnsdl_raw', 'sdl_mds_id_ps_msl')}}
),
final as
(
    select 
        trim(brand)::varchar(50) as brand,
        trim(sub_brand)::varchar(50) as sub_brand,
        trim(sku_variant)::varchar(100) as sku_variant,
        trim(sku)::varchar(100) as sku,
        trim(cust_group)::varchar(50) as cust_group,
        trim(channel_group)::varchar(50) as channel_group,
        trim(channel)::varchar(50) as channel,
        cast(trim(qty_min) as integer) as qty_min,
        trim(identifier)::varchar(50) as identifier,
        file_name::varchar(100) as file_name,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final
