with source as(
    select * from {{ source('pcfsdl_raw', 'sdl_dstr_coles_inv') }}
),
itg_dstr_coles_sap_mapping as(
    select * from {{ ref('pcfitg_integration__itg_dstr_coles_sap_mapping') }}
),
final as(
    select 
        inv.inv_date as inv_date,
        inv.order_item as article_code,
        inv.order_item_desc as article_desc,
        ltrim(map.sap_code,0) as matl_id
    from source inv
    left join
    (select distinct sap_code,item_idnt from itg_dstr_coles_sap_mapping where sap_code!='') map
    on ltrim(inv.order_item,0)=ltrim(map.item_idnt,0)
)
select * from final