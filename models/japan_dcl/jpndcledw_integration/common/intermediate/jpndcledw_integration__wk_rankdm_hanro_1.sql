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
