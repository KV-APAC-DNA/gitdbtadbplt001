{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as (
    select * from {{ source('thasdl_raw', 'sdl_la_gt_route_header') }}
),
final as (
    SELECT 
        hashkey,
        route_id,
        route_name,
        route_desc,
        is_active,
        routesale,
        saleunit,
        route_code,
        description,
        last_updated_date,
        file_upload_date,
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