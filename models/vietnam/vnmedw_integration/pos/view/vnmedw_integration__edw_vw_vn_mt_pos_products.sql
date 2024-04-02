with itg_vn_mt_pos_product_master as
(
    select * from {{ ref('vnmitg_integration__itg_vn_mt_pos_product_master') }}
),
final as 
(
select itg_vn_mt_pos_product_master.barcode,
    itg_vn_mt_pos_product_master.code,
    itg_vn_mt_pos_product_master.customer,
    itg_vn_mt_pos_product_master.customer_sku,
    itg_vn_mt_pos_product_master.name
from itg_vn_mt_pos_product_master
where (
        (itg_vn_mt_pos_product_master.active)::text = ('Y'::character varying)::text
    )
)
select * from final