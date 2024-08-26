{{
    config(
        materialized="incremental",
        incremental_strategy = "append"
    )
}}

with source as
(
    select * from {{source('thasdl_raw','sdl_la_gt_route_detail')}}
    where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_route_detail__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_route_detail__duplicate_test') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_route_detail__test_format_created_date') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_route_detail__test_format_sale_unit') }}
        )
    
),

final as
(   
    select 
        HASHKEY::VARCHAR(500) as HASHKEY,
        ROUTE_ID::VARCHAR(50) as ROUTE_ID,
        CUSTOMER_ID::VARCHAR(50) as CUSTOMER_ID,
        ROUTE_NO::VARCHAR(10) as ROUTE_NO,
        SALEUNIT::VARCHAR(100) as SALEUNIT,
        SHIP_TO::VARCHAR(50) as SHIP_TO,
        CONTACT_PERSON::VARCHAR(100) as CONTACT_PERSON,
        CREATED_DATE::VARCHAR(20) as CREATED_DATE,
        FILE_UPLOAD_DATE::DATE as FILE_UPLOAD_DATE,
        FILENAME::VARCHAR(50) as FILE_NAME,
        RUN_ID::VARCHAR(14) as RUN_ID,
        CRT_DTTM::TIMESTAMP_NTZ(9) as CRT_DTTM
    from source
    {% if is_incremental() %}
        where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif%}
)
select * from final