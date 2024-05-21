Delete from na_itg.itg_mds_hk_product_hierarchy
where (sap_brand,sap_base_product) IN
(Select sap_brand,sap_base_product from na_sdl.sdl_mds_hk_product_hierarchy);

INSERT INTO na_itg.itg_mds_hk_product_hierarchy
SELECT sap_brand,
       sap_base_product,
       hk_brand_code,
	   hk_base_product_code,
       hk_base_product_name
FROM na_sdl.sdl_mds_hk_product_hierarchy;