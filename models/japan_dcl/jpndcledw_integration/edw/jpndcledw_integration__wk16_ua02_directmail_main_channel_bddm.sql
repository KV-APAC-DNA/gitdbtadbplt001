WITH cim01kokya
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__cim01kokya') }}
    ),
WK_RANKDM_HANRO
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__wk_rankdm_hanro') }}
    ),
WK_RANKDM_HANRO_BK
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__wk_rankdm_hanro_bk') }}
    ),
a
AS (
    SELECT DISTINCT NVL(LPAD(DIUSRID, 10, '0'), '0000000000') AS kokyano,
        HANRO_RANK
    FROM WK_RANKDM_HANRO
    ),
final
AS (
    SELECT DISTINCT ck.kokyano::VARCHAR(68) AS Customer_No,
        a.HANRO_RANK::VARCHAR(10) AS main_channel_bddm
    FROM cim01kokya ck
    LEFT JOIN a ON ck.kokyano = a.kokyano
    WHERE ck.testusrflg = '通常ユーザ'
    )
SELECT *
FROM final
