with source as (
    select * from {{ ref('phledw_integration__edw_vw_ph_delivery_fact') }} 
),
final as
(
    select
        cntry_key as "cntry_key",
        cntry_nm as "cntry_nm",
        deliv_num as "deliv_num",
        deliv_item as "deliv_item",
        delvry_dt as "delvry_dt",
        act_delvry_dt as "act_delvry_dt",
        doc_creation_dt as "doc_creation_dt",
        sold_to as "sold_to",
        matl_num as "matl_num",
        item_catgy as "item_catgy",
        ean_num as "ean_num",
        sls_org as "sls_org",
        ship_to as "ship_to",
        billing_block as "billing_block",
        whse_num as "whse_num",
        base_uom as "base_uom",
        delvrd_qty_pc as "delvrd_qty_pc",
        act_delvry_qty as "act_delvry_qty"
    from source
)
select * from final