with source as(
    select * from {{ source('thasdl_raw', 'sdl_mds_th_distributor_product_group') }}
),
final as(
    select
        trim(sap_code)::varchar(25) as prod_cd,
        trim(product_group_name)::varchar(50) as prod_grp,
        to_char(current_timestamp()::timestampntz(9), 'YYYYMMDDHH24MISSFF3')::varchar(255) as cdl_dttm,
        current_timestamp()::timestampntz(9) as crtd_dttm,
        current_timestamp()::timestampntz(9) as updt_dttm
    from source
)
select * from final