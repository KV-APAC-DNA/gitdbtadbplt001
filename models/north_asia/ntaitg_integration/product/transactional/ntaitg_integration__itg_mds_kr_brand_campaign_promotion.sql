DELETE
FROM NA_ITG.ITG_MDS_KR_Brand_Campaign_Promotion
WHERE (TRIM(code)) IN (SELECT TRIM(code) AS code FROM na_sdl.SDL_MDS_KR_Brand_Campaign_Promotion);


INSERT INTO NA_ITG.ITG_MDS_KR_Brand_Campaign_Promotion
SELECT
       TRIM(code),
       TRIM(brand_code) AS brand_code,
       TRIM(brand_name) AS brand_name,
       TRIM(brand_id) AS brand_id,
	   lastchgdatetime as lastchgdatetime
FROM na_sdl.SDL_MDS_KR_Brand_Campaign_Promotion;