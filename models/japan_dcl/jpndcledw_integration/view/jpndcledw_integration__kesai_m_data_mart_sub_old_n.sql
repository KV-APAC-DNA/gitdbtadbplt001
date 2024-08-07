WITH cit80saleh_ikou
AS (
    SELECT *
    FROM snapjpdcledw_integration.cit80saleh_ikou
    ),
cit81salem_ikou
AS (
    SELECT *
    FROM snapjpdcledw_integration.cit81salem_ikou
    ),
old_tokutencd_master
AS (
    SELECT *
    FROM snapjpdcledw_integration.old_tokutencd_master
    ),
 final 
 AS (
        SELECT (('T'::CHARACTER VARYING)::TEXT || (cit81.saleno)::TEXT) AS saleno,
            cit81.gyono,
            CASE 
                WHEN (tokuten.itemcode IS NOT NULL)
                    THEN '特典'::CHARACTER VARYING
                WHEN ((cit81.itemcode_hanbai)::TEXT = ('9990000100'::CHARACTER VARYING)::TEXT)
                    THEN '利用ポイント'::CHARACTER VARYING
                WHEN ((cit81.itemcode_hanbai)::TEXT = ('9990000200'::CHARACTER VARYING)::TEXT)
                    THEN '利用ポイント'::CHARACTER VARYING
                ELSE '商品'::CHARACTER VARYING
                END AS meisaikbn,
            cit81.itemcode_hanbai AS itemcode,
            cit81.wariritu,
            cit81.tanka,
            cit81.warimaekomitanka,
            cit81.suryo,
            cit81.hensu,
            cit81.kingaku,
            cit81.warimaekomikingaku,
            cit81.meisainukikingaku,
            cit81.warimaenukikingaku,
            cit81.meisaitax,
            cit81.saleno AS kesaiid,
            trim((cit81.saleno)::TEXT) AS saleno_trim
        FROM (
            (
                cit80saleh_ikou cit80 JOIN cit81salem_ikou cit81 ON (
                        (
                            ((cit80.saleno)::TEXT = (cit81.saleno)::TEXT)
                            AND (cit80.cancelflg = (0)::SMALLINT)
                            )
                        )
                ) LEFT JOIN old_tokutencd_master tokuten ON (((cit81.itemcode_hanbai)::TEXT = (tokuten.itemcode)::TEXT))
            )
        )

SELECT *
FROM final
