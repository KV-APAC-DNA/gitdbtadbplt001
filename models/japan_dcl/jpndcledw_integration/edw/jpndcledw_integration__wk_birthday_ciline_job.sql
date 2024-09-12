WITH WK_BIRTHDAY_HEADER_JOB
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__wk_birthday_header_job') }}
    ),
AA
AS (
    SELECT KOKYANO,
        KOUNYU_CNT_180,
        URITOTAL_180,
        URITOTAL,
        SHUKADATE,
        JUCHKBN_10,
        JUCHKBN_18,
        0 AS TENPO_KOUNYU_CNT_180,
        0 AS TENPO_URITOTAL_180,
        0 AS TENPO_URITOTAL,
        0 AS TENPO_SHUKADATE,
        0 AS TENPO_KBN
    FROM (
        SELECT KOKYANO,
            COUNT(SALENO) AS KOUNYU_CNT_180, --購入回数(直近180日)
            SUM(NVL(URITOTAL_180, 0) + NVL(RETURNTOTAL_180, 0)) AS URITOTAL_180, --累計購入金額(直近180日)
            SUM(NVL(URITOTAL, 0) + NVL(RETURNTOTAL, 0)) AS URITOTAL, --累計購入金額
            MAX(SHUKADATE) AS SHUKADATE,
            DECODE(MIN(JUCHKBN), '10', '1', '0') AS JUCHKBN_10, --通常が一つでもあるなら、通常になる。
            DECODE(MAX(JUCHKBN), '18', '1', '0') AS JUCHKBN_18 --定期が一つでもあるなら、定期になる。
        FROM (
            --３年レコード
            SELECT KOKYANO,
                NULL AS SALENO, --購入回数数えない
                NULL AS URITOTAL_180, --直近180日の金額は数えない
                NULL AS RETURNTOTAL_180,
                URITOTAL,
                RETURNTOTAL,
                SHUKADATE,
                JUCHKBN
            FROM WK_BIRTHDAY_HEADER_JOB
            WHERE TENPOCODE IS NULL
            
            UNION ALL
            
            --直近180日レコード
            SELECT KOKYANO,
                SALENO, --購入回数数える
                URITOTAL AS URITOTAL_180, --直近180日の金額
                RETURNTOTAL AS RETURNTOTAL_180,
                NULL AS URITOTAL,
                NULL AS RETURNTOTAL,
                NULL AS SHUKADATE,
                NULL AS JUCHKBN
            FROM WK_BIRTHDAY_HEADER_JOB
            WHERE SHUKADATE >= CAST(TO_CHAR(DATEADD(DAY, - 181, current_timestamp()), 'YYYYMMDD') AS NUMERIC) --181日前～
                AND TENPOCODE IS NULL
            )
        GROUP BY KOKYANO
        )
    
    UNION ALL
    
    --店舗
    SELECT KOKYANO,
        0 AS KOUNYU_CNT_180,
        0 AS URITOTAL_180,
        0 AS URITOTAL,
        0 AS SHUKADATE,
        0 AS JUCHKBN_10,
        0 AS JUCHKBN_18,
        KOUNYU_CNT_180 AS TENPO_KOUNYU_CNT_180,
        URITOTAL_180 AS TENPO_URITOTAL_180,
        URITOTAL AS TENPO_URITOTAL,
        SHUKADATE AS TENPO_SHUKADATE,
        1 AS TENPO_KBN
    FROM (
        SELECT KOKYANO,
            COUNT(SALENO) AS KOUNYU_CNT_180, --購入回数(直近180日)
            SUM(NVL(URITOTAL_180, 0) + NVL(RETURNTOTAL_180, 0)) AS URITOTAL_180, --累計購入金額(直近180日)
            SUM(NVL(URITOTAL, 0) + NVL(RETURNTOTAL, 0)) AS URITOTAL, --累計購入金額
            SUM(NVL(RETURNTOTAL, 0)) AS HENGOKEI, -- 返金額(直近3年)
            MAX(SHUKADATE) AS SHUKADATE
        FROM (
            --３年レコード
            SELECT KOKYANO,
                NULL AS SALENO, --購入回数数えない
                NULL AS URITOTAL_180, --直近180日の金額は数えない
                NULL AS RETURNTOTAL_180,
                URITOTAL,
                RETURNTOTAL,
                SHUKADATE,
                JUCHKBN
            FROM WK_BIRTHDAY_HEADER_JOB
            WHERE TENPOCODE IS NOT NULL
            
            UNION ALL
            
            --直近180日レコード
            SELECT KOKYANO,
                SALENO, --購入回数数える
                URITOTAL AS URITOTAL_180, --直近180日の金額
                RETURNTOTAL AS RETURNTOTAL_180,
                NULL AS URITOTAL,
                NULL AS RETURNTOTAL,
                NULL AS SHUKADATE,
                NULL AS JUCHKBN
            FROM WK_BIRTHDAY_HEADER_JOB
            WHERE SHUKADATE >= CAST(TO_CHAR(DATEADD(DAY, - 181, current_timestamp()), 'YYYYMMDD') AS VARCHAR) --181日前～
                AND TENPOCODE IS NOT NULL
            )
        GROUP BY KOKYANO
        )
    ),
final
AS (
    SELECT AA.KOKYANO::NUMBER(10,0) AS DIUSRID,
        MAX(AA.KOUNYU_CNT_180)::NUMBER(5,0) AS DIBUYCNT,
        MAX(AA.URITOTAL_180)::NUMBER(10,0) AS DIBUYTOTAL_180,
        0::NUMBER(10,0) AS DIBUYTOTAL_BD,
        MAX(AA.URITOTAL)::NUMBER(10,0) AS DIBUYTOTAL,
        MAX(AA.SHUKADATE)::NUMBER(8,0) AS DSLATESTDATE,
        MAX(AA.JUCHKBN_10)::VARCHAR(2) AS DSCHKNORMALFLG,
        MAX(AA.JUCHKBN_18)::VARCHAR(2) AS DSCHKTEIKIFLG,
        MAX(AA.TENPO_KOUNYU_CNT_180)::NUMBER(5,0) AS DISTOREBUYCNT,
        MAX(AA.TENPO_URITOTAL_180)::NUMBER(10,0) AS DISTOREBUYTOTAL_180,
        0::NUMBER(10,0) AS DISTOREBUYTOTAL_BD,
        MAX(AA.TENPO_URITOTAL)::NUMBER(10,0) AS DISTOREBUYTOTAL,
        MAX(AA.TENPO_SHUKADATE)::NUMBER(8,0) AS DSSTORELATESTDATE,
        MAX(AA.TENPO_KBN)::VARCHAR(2) AS DISTOREFLG
    FROM AA
    GROUP BY AA.KOKYANO
    )
SELECT *
FROM final

