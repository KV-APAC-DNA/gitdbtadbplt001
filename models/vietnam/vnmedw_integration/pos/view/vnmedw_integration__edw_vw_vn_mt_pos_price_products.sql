with itg_vn_mt_pos_price_products as 
(
    select * from {{ ref('vnmitg_integration__itg_vn_mt_pos_price_products') }}
),
final as 
(
select itg_vn_mt_pos_price_products.jnj_sap_code,
    itg_vn_mt_pos_price_products.franchise,
    itg_vn_mt_pos_price_products.brand,
    itg_vn_mt_pos_price_products.sku,
    itg_vn_mt_pos_price_products.bar_code,
    itg_vn_mt_pos_price_products.pc_per_case,
    itg_vn_mt_pos_price_products.price,
    itg_vn_mt_pos_price_products.product_id_concung
from itg_vn_mt_pos_price_products
where (
        (itg_vn_mt_pos_price_products.active)::text = ('Y'::character varying)::text
    )
)
select * from final