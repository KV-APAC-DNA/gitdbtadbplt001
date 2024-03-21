{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as (
    select * from {{ source('thasdl_raw', 'sdl_th_gt_route_detail') }}
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
