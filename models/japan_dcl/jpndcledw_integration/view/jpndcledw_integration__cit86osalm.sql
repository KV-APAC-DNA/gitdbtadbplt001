WITH cit86osalm_uri_mv
AS (
    SELECT *
    FROM snapjpdcledw_integration.cit86osalm_uri_mv
    ),
cit86osalm_hen_mv
AS (
    SELECT *
    FROM snapjpdcledw_integration.cit86osalm_hen_mv
    ),
t1
AS (
    SELECT ourino,
        gyono,
        itemcode,
        itemcode_hanbai,
        suryo,
        tanka,
        hensu,
        henusu,
        kingaku,
        meisainukikingaku,
        meisaitax,
        anbunmeisainukikingaku,
        tanka_tuka,
        kingaku_tuka,
        meisainukikingaku_tuka,
        meisaitax_tuka,
        kakokbn,
        kkng_kbn,
        dispourino,
        shimebi,
        shohzei_ritsu
    FROM cit86osalm_uri_mv
    WHERE ((kkng_kbn)::TEXT = ('1'::CHARACTER VARYING)::TEXT)
    ),
t2
AS (
    SELECT ourino,
        gyono,
        itemcode,
        itemcode_hanbai,
        suryo,
        tanka,
        hensu,
        henusu,
        kingaku,
        meisainukikingaku,
        meisaitax,
        anbunmeisainukikingaku,
        tanka_tuka,
        kingaku_tuka,
        meisainukikingaku_tuka,
        meisaitax_tuka,
        kakokbn,
        kkng_kbn,
        dispourino,
        shimebi,
        shohzei_ritsu
    FROM cit86osalm_hen_mv
    WHERE ((kkng_kbn)::TEXT = ('1'::CHARACTER VARYING)::TEXT)
    ),
FINAL
AS (
    SELECT *
    FROM T1
    
    UNION ALL
    
    SELECT *
    FROM T2
    )
SELECT *
FROM FINAL
