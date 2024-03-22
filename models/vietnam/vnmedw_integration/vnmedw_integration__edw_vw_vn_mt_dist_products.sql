with itg_vn_mt_dksh_product_master as
(
    select * from {{ source('snaposeitg_integration','itg_vn_mt_dksh_product_master') }}
),
final as 
(
select 
    itg_vn_mt_dksh_product_master.code
	,itg_vn_mt_dksh_product_master.product_name
	,(itg_vn_mt_dksh_product_master.barcode)::character varying as barcode
	,(itg_vn_mt_dksh_product_master.jnj_sap_code)::character varying as jnj_sap_code
	,upper((itg_vn_mt_dksh_product_master.franchise)::text) as franchise
	,upper((itg_vn_mt_dksh_product_master.category)::text) as category
	,upper((itg_vn_mt_dksh_product_master.sub_category)::text) as sub_category
	,upper((itg_vn_mt_dksh_product_master.sub_brand)::text) as sub_brand
	,itg_vn_mt_dksh_product_master.base_bundle
	,itg_vn_mt_dksh_product_master.size
from itg_vn_mt_dksh_product_master
where ((itg_vn_mt_dksh_product_master.active)::text = 'Y'::text)
)

select * from final