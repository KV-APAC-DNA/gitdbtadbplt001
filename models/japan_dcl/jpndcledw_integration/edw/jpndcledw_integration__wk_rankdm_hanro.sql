{{
    config(
        post_hook ="UPDATE {{this}} HANRO SET HANRO_RANK = '店舗'
                        FROM (
                            SELECT LIST.KOKYANO         AS KOKYANO
                            ,NVL(TENPO.MAX,0)     AS TENPOMAX
                            ,NVL(TENPO.CNT,0)     AS TEMPCNT
                            ,NVL(TUHAN.MAX,0)     AS TUHANMAX
                            ,NVL(TUHAN.CNT,0)     AS TUHANCNT
                            ,CASE WHEN NVL(TENPO.CNT,0) > NVL(TUHAN.CNT,0) THEN '店舗'
                                    WHEN NVL(TENPO.CNT,0) < NVL(TUHAN.CNT,0) THEN '通販'
                                    WHEN NVL(TENPO.CNT,0) = NVL(TUHAN.CNT,0) AND NVL(TENPO.MAX,0) > NVL(TUHAN.MAX,0) THEN '店舗'
                                    WHEN NVL(TENPO.CNT,0) = NVL(TUHAN.CNT,0) AND NVL(TENPO.MAX,0) < NVL(TUHAN.MAX,0) THEN '通販'
                                    WHEN NVL(TENPO.CNT,0) = NVL(TUHAN.CNT,0) AND NVL(TENPO.MAX,0) = NVL(TUHAN.MAX,0) THEN '通販'
                            END HANTEI
                                FROM (SELECT DISTINCT SUMMARY.KOKYANO FROM snapjpdcledw_integration.WK_TENPOJUDGE_SUMMARY SUMMARY) LIST
                                LEFT JOIN (SELECT KOKYANO, MAX, CNT FROM snapjpdcledw_integration.WK_TENPOJUDGE_SUMMARY WHERE TORIKEIKBN = '店舗') TENPO
                                ON TENPO.KOKYANO = LIST.KOKYANO
                                LEFT JOIN (SELECT KOKYANO, MAX, CNT FROM snapjpdcledw_integration.WK_TENPOJUDGE_SUMMARY WHERE TORIKEIKBN = '通販') TUHAN
                                ON TUHAN.KOKYANO = LIST.KOKYANO
                            ) REJUDGE
                        WHERE (LPAD(HANRO.DIUSRID,10,0) = REJUDGE.KOKYANO AND REJUDGE.HANTEI = '店舗');"
    )
}}

WITH WK_BIRTHDAY_HANRO
AS (
    SELECT *
    FROM snapjpdcledw_integration.WK_BIRTHDAY_HANRO
    ),
TBUSRPRAM
AS (
    SELECT *
    FROM snapjpdclitg_integration.TBUSRPRAM
    ),
WK_STORE_KONYU
AS (
    SELECT *
    FROM snapjpdcledw_integration.WK_STORE_KONYU
    ),
final
AS (
    SELECT BDDM.DIUSRID::NUMBER(10, 0) AS DIUSRID,
        USR.dsdat60::VARCHAR(40) AS dsdat60,
        USR.dsdat61::VARCHAR(40) AS dsdat61,
        NVL(URI_TENPO.KINGAKU_SUM, 0)::NUMBER(10, 0) AS KINGAKU_SUM,
        BDDM.HANRO::VARCHAR(40) AS HANRO_BDDM,
        (CASE 
            WHEN USR.dsdat60 = '拒否'
                AND USR.dsdat61 = '希望'
                AND NVL(URI_TENPO.KINGAKU_SUM, 0) <= 0
                THEN '通販'
            ELSE BDDM.HANRO
            END)::VARCHAR(40) AS HANRO_RANK,
        (CASE 
            WHEN USR.dsdat60 = '拒否'
                AND USR.dsdat61 = '希望'
                AND NVL(URI_TENPO.KINGAKU_SUM, 0) <= 0
                THEN '1'
            ELSE '0'
            END)::VARCHAR(4) AS FORCED_TUHAN_DM_SEND_FLG
    FROM WK_BIRTHDAY_HANRO BDDM
    LEFT JOIN TBUSRPRAM USR ON BDDM.DIUSRID = USR.DIUSRID
    LEFT JOIN WK_STORE_KONYU URI_TENPO ON BDDM.DIUSRID = URI_TENPO.DIUSRID
    )
SELECT *
FROM final
