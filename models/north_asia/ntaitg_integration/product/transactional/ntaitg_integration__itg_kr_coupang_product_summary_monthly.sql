delete from na_itg.itg_kr_coupang_product_summary_monthly
where trim(category_depth1)||trim(category_depth2)||trim(category_depth3)||trim(all_brand)||trim(coupang_sku_id)||trim(coupang_sku_name)||trim(ranking)||trim(jnj_product_flag)||trim(yearmo)
  in
(select distinct trim(category_depth1)||trim(category_depth2)||trim(category_depth3)||trim(all_brand)||trim(coupang_sku_id)||trim(coupang_sku_name)||trim(ranking)||trim(jnj_product_flag)||trim(yearmo)
from na_sdl.sdl_kr_coupang_product_summary_monthly);


INSERT INTO na_itg.itg_kr_coupang_product_summary_monthly
(
	category_depth1
	,category_depth2
	,category_depth3
	,all_brand
	,coupang_sku_id
	,coupang_sku_name
	,ranking
	,jnj_product_flag
    ,run_id
    ,file_name
	,yearmo
    ,updt_dttm
)
SELECT category_depth1
	,category_depth2
	,category_depth3
	,all_brand
	,coupang_sku_id
	,coupang_sku_name
	,ranking
	,jnj_product_flag
    ,run_id
    ,file_name
	,yearmo
    ,convert_timezone('SGT',sysdate) AS crtd_dttm
FROM na_sdl.sdl_kr_coupang_product_summary_monthly;