{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as (
    select * from {{ source('thasdl_raw', 'sdl_th_gt_route_detail') }}
    where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_route_detail__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_route_detail__duplicate_test') }}
            union all 
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_route_detail__test_format') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_route_detail__test_date_format_odd_eve_leap') }}
            )
),
final as (
    select 
        hashkey,
        cntry_cd,
        crncy_cd,
        routeid,
        customerid,
        routeno,
        saleunit,
        ship_to,
        contact_person,
        created_date,
        filename,
        file_uploaded_date,
        run_id,
        crt_dttm
    from source 
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crt_dttm > (select max(crt_dttm) from {{ this }}) 
 {% endif %}

)

select * from final
