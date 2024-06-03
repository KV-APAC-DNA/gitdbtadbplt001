with source as (
    select * from {{ source('ntasdl_raw', 'sdl_mds_kr_sales_store_map') }}
),
final as 
(
    select 
        sls_ofc_code::varchar(4) as sls_ofc,
        sls_ofc_desc::varchar(40) as sls_ofc_desc,
        channel::varchar(25) as channel,
        store_type::varchar(25) as store_type,
        sales_group_code_code::varchar(18) as sales_group_code,
        sales_group_nm::varchar(100) as sales_group_nm,
        convert_timezone('UTC', current_timestamp)::timestamp_ntz(9) as crt_dttm,
        convert_timezone('UTC', current_timestamp)::timestamp_ntz(9) as updt_dttm,
        customer_segmentation_code::varchar(256) as customer_segmentation_code,
        customer_segmentation_level_2_code::varchar(256) as customer_segmentation_level_2_code
    from source
)
select * from final