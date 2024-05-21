DELETE
FROM na_itg.itg_mds_kr_keyword_classifications
WHERE (UPPER(TRIM(keyword))) IN (SELECT UPPER(TRIM(keyword)) AS keyword

                                                               FROM na_sdl.sdl_mds_kr_keyword_classifications);


INSERT INTO na_itg.itg_mds_kr_keyword_classifications
SELECT
       TRIM(code),
       TRIM(keyword) AS keyword,
       TRIM(keyword_group) AS keyword_group,
       lastchgdatetime as lastchgdatetime
FROM na_sdl.sdl_mds_kr_keyword_classifications;