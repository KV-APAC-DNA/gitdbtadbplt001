{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['jj_mnth_id','order_id, item_sku']
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
   cal.mnth_id as jj_mnth_id,
   cal.cal_year as jj_year,
    prefix,
    month,
    month_no,
    order_id,
    partner_order_id,
    territory_manager,
    item_sku,
    acommerce_item_sku,
    product_title,
    customer_email,
    Brand,
    Mapping,
    ltp,
    jj_srp,
    quantity,
    gmv
    from hcp_acommerce_load hce
    inner join (select distinct mnth_long,mnth_no,mnth_id,cal_year from edw_vw_os_time_dim where cal_year=2024 ) 
    cal on (upper(hce.month)=upper(cal.mnth_long) and hce.month_no=cal.mnth_no)
), 
final as 
(
    select 
    * 
    from 
    transformed
)
select * from final 


