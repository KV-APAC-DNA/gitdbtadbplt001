{{
    config
    (
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["month","brand","item_sku","month_no"]
    )
}}
with hcp_acommerce_load as 
(
    select * from {{source('phlsdl_raw','sdl_hcp_acommerce_load')}}
),
edw_vw_os_time_dim as 
(
select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
transformed as 
(
    select 
   mnth_id,
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
    from hcp_acommerce_load a
    inner join (select distinct mnth_long,mnth_no,mnth_id from edw_vw_os_time_dim where cal_year=year(current_date) ) cal on (a.month=cal.mnth_long and a.month_no=cal.mnth_no)
), 
final as 
(
    select 
    * 
    from 
    transformed
)
select * from final 


