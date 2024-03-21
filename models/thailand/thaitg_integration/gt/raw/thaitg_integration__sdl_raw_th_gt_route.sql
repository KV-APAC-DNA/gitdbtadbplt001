{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as (
    select * from {{ source('thasdl_raw', 'sdl_th_gt_route') }}
),

final as (
    select 
       hashkey,
       cntry_cd,
       crncy_cd,
       routeid,
       name,
       route_description,
       isactive,
       routesale,
       saleunit,
       routecode,
       description,
       last_updated_date,
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
