{{
    config(
        post_hook ="UPDATE {{ ref('jpndcledw_integration__wk_rankdm_hanro') }} HANRO SET HANRO_RANK = '店舗'
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
                                FROM (SELECT DISTINCT SUMMARY.KOKYANO FROM {{ ref('jpndcledw_integration__wk_tenpojudge_summary') }} SUMMARY) LIST
                                LEFT JOIN (SELECT KOKYANO, MAX, CNT FROM {{ ref('jpndcledw_integration__wk_tenpojudge_summary') }} WHERE TORIKEIKBN = '店舗') TENPO
                                ON TENPO.KOKYANO = LIST.KOKYANO
                                LEFT JOIN (SELECT KOKYANO, MAX, CNT FROM {{ ref('jpndcledw_integration__wk_tenpojudge_summary') }} WHERE TORIKEIKBN = '通販') TUHAN
                                ON TUHAN.KOKYANO = LIST.KOKYANO
                            ) REJUDGE
                        WHERE (LPAD(HANRO.DIUSRID,10,0) = REJUDGE.KOKYANO AND REJUDGE.HANTEI = '店舗');"
    )
}}


with WK_RANKDM_HANRO as (
select * from {{ ref('jpndcledw_integration__wk_rankdm_hanro') }}
),
------add reference from sourav's model 
final as (
SELECT 
*
FROM WK_RANKDM_HANRO
)
select * from final