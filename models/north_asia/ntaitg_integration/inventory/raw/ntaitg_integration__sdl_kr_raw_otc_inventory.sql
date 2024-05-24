{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}
with source as(
    select * from {{ source('ntasdl_raw', 'sdl_kr_otc_inventory') }}
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
    where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
)
select * from final
