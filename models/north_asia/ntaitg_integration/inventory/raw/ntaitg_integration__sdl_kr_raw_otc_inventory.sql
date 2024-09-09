{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}
with source as(
    select * from {{ source('ntasdl_raw', 'sdl_kr_otc_inventory') }}
    where file_name not in
     (select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_otc_inventory__null_test') }}
      union all
      select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_otc_inventory__duplicate_test') }}
     )
),
final as
(
    select 
        mnth_id,
        matl_num,
        brand,
        product_name,
        distributor_cd,
        unit_price,
        inv_qty,
        inv_amt,
        filename,
        crtd_dttm
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    {% endif %}
)
select * from final
