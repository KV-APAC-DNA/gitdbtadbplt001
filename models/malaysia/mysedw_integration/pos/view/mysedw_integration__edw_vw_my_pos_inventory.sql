with source as
(
    select * from {{ ref('mysitg_integration__itg_my_as_watsons_inventory') }}
),
final as
(   select
        year,
        mnth_id,
        null as inv_week,
        null as inv_date,
        cust_cd,
        matl_num as sap_matl_num,
        matl_num as item_cd,
        null as item_desc,
        inv_qty_pc as end_stock_qty,
        inv_value as end_stock_val
    from source
)
select * from final

