{{
    config
    (
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["month","brand","item_sku","month_no"]
    )
}}
with source as 
(
    select * from {{source('phlsdl_raw','sdl_hcp_acommerce_load')}}
),
transformed as 
(
    select 

    prefix,
    month,
    month_no,
    order_id,
    partner_order_id,
    territory_manager,
    item_sku,
    acommerce_item_sku,
    product_title,
    Brand,
    Mapping,
    ltp,
    jj_srp,
    quantity,
    gmv
    from source 
),
final as 
(
    select 
    * 
    from 
    transformed
)
select * from final 


