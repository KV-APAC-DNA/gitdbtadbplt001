DELETE
FROM na_itg.ITG_MDS_KR_Naver_Product_Master
WHERE (TRIM(code)) IN (SELECT TRIM(code) AS code FROM na_sdl.SDL_MDS_KR_Naver_Product_Master);


INSERT INTO na_itg.ITG_MDS_KR_Naver_Product_Master
SELECT
       TRIM(code),
       TRIM(category_l_code) AS Category_L_Code,
       TRIM(category_m_code) AS Category_M_Code,
       TRIM(category_s_code) AS Category_S_Code,
	   TRIM(brands_name) AS brand_name,
	   TRIM(product_name) as product_name,
	   lastchgdatetime as lastchgdatetime,
	   sysdate as refresh_date
FROM na_sdl.SDL_MDS_KR_Naver_Product_Master;