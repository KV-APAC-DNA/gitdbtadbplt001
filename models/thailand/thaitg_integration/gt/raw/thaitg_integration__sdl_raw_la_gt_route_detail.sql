{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as (
    select * from {{ source('thasdl_raw', 'sdl_la_gt_route_detail') }}
),
final as (
    SELECT 
        hashkey,
        route_id,
        customer_id,
        route_no,
        saleunit,
        ship_to,
        contact_person,
        created_date,
        filename,
        run_id,
        crt_dttm
    from source
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
)

select * from final