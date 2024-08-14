with CIY80SALEH_IKOU
as
(
    SELECT * FROM {{ source('jpdcledw_integration', 'ciy80saleh_ikou') }}
),
CIT80SALEH_IKOU AS
(
    SELECT * FROM {{ source('jpdcledw_integration', 'cit80saleh_ikou') }}
),
WORK_BIOPTRON AS
(
    SELECT * FROM {{ source('jpdcledw_integration', 'work_bioptron') }}
),
CIM01KOKYA
as
(
    SELECT * FROM {{ ref('jpndcledw_integration__cim01kokya') }}
),
T1
AS (
    SELECT A.KOKYANO,
        A.SALENO,
        A.BK_JUCHKBN,
        A.JUCHDATE AS SHUKADATE,
        (A.SOGOKEI - A.RIYOPOINT - A.SORYO) AS URITOTAL
    FROM CIT80SALEH_IKOU A
    WHERE A.TORIKEIKBN = '01'
        AND A.BK_JUCHKBN IN ('10', '13') --10:通常受注、13:輸送破損、18:定期購買
        AND A.JUCHDATE >= CAST(TO_CHAR(DATEADD(DAY,- 1 - (365 * 3),SYSDATE()), 'YYYYMMDD') AS NUMERIC) --３年前～
        AND A.JUCHDATE <= CAST(TO_CHAR(DATEADD(DAY,-1,SYSDATE()), 'YYYYMMDD') AS NUMERIC) --DWHサーバの場合、前日までしかデータがない
        AND A.CANCELFLG = 0
        AND A.SOGOKEI > 0 --送料抜きの商品金額のみ
        AND A.BK_HANROCODE NOT IN ('70') --41:職域FAX(OG・OB)、70:社販を除く
    ),
T2
AS (
    SELECT A.KOKYANO,
        A.SALENO,
        A.BK_JUCHKBN,
        A.KIBOUDATE AS SHUKADATE,
        (A.SOGOKEI - A.RIYOPOINT - A.SORYO) AS URITOTAL
    FROM CIT80SALEH_IKOU A
    WHERE A.TORIKEIKBN = '01'
        AND A.BK_JUCHKBN IN ('18') --10:通常受注、13:輸送破損、18:定期購買
        AND A.KIBOUDATE >= CAST(TO_CHAR(DATEADD(DAY,- 1 - (365 * 3),SYSDATE()), 'YYYYMMDD') AS NUMERIC) --３年前～
        AND A.KIBOUDATE <= CAST(TO_CHAR(DATEADD(DAY,- 1,SYSDATE()), 'YYYYMMDD') AS NUMERIC) --DWHサーバの場合、前日までしかデータがない
        AND A.CANCELFLG = 0
        AND A.SOGOKEI > 0
        AND A.BK_HANROCODE NOT IN ('70') --41:職域FAX(OG・OB)、70:社販を除く
    ),
T3
AS (
    SELECT A.KOKYANO,
        'BIO_' || A.NO AS SALENO,
        '10' AS BK_JUCHKBN,
        A.INSERTDATE AS SHUKADATE,
        A.SOGOKEI AS URITOTAL
    FROM WORK_BIOPTRON A
    WHERE A.KUBUN IN ('通販') --通販、社販
        AND A.INSERTDATE >= CAST(TO_CHAR(DATEADD(DAY,- 1 - (365 * 3),SYSDATE()), 'YYYYMMDD') AS NUMERIC) --３年前～
        AND A.INSERTDATE <= CAST(TO_CHAR(DATEADD(DAY,- 1,SYSDATE()), 'YYYYMMDD') AS NUMERIC) --DWHサーバの場合、前日までしかデータがない
        AND A.SOGOKEI > 0
    ),
UNION_OF_A
AS (
    SELECT *
    FROM T1
    
    UNION
    
    SELECT *
    FROM T2
    
    UNION
    
    SELECT *
    FROM T3
    ),
ins
AS (
    SELECT /*+ INDEX(B) */
        A.KOKYANO,
        A.SALENO,
        A.BK_JUCHKBN,
        A.SHUKADATE,
        A.URITOTAL,
        0 AS RETURNTOTAL,
        '' AS TENPOCODE,
        '0' AS ITHIBU_HEPIN_FLG
    FROM UNION_OF_A A
    INNER JOIN CIM01KOKYA B ON B.KOKYANO = A.KOKYANO
    --返品があった売上レコード以外(つまり一番最新)の売上レコードを抽出する条件
    WHERE NOT EXISTS (
            SELECT 'X'
            FROM CIT80SALEH_IKOU C
            WHERE A.KOKYANO = C.KOKYANO
                AND A.SALENO = C.HENMOTOJUCHNO
                AND C.TORIKEIKBN = '01' --01：通販
                AND C.BK_JUCHKBN IN ('20', '21', '22', '23') --返品
                AND C.JUCHDATE >= CAST(TO_CHAR(DATEADD(DAY,- 1 - (365 * 3),SYSDATE()), 'YYYYMMDD') AS NUMERIC)
                AND C.CANCELFLG = 0
                AND C.SOGOKEI < 0 --返品交換のような返金が発生していないレコードは除く(返品数量を正しくカウントしたいので邪魔なレコードは除く)
            )
    ),
ins2
AS (
    SELECT /*+ INDEX(D) */
        C.KOKYANO,
        C.SALENO,
        C.BK_JUCHKBN,
        C.SHUKADATE,
        C.URITOTAL,
        C.RETURNTOTAL,
        '',
        C.ITHIBU_HEPIN_FLG
    FROM (
        SELECT A.KOKYANO,
            A.SALENO,
            MAX(A.BK_JUCHKBN) AS BK_JUCHKBN,
            MAX(A.SHUKADATE) AS SHUKADATE,
            MAX(A.URITOTAL) AS URITOTAL,
            SUM(B.RETURNTOTAL) AS RETURNTOTAL,
            '1' AS ITHIBU_HEPIN_FLG
        FROM UNION_OF_A A,
            (
                SELECT A.KOKYANO,
                    A.SALENO, --返品NO
                    A.HENMOTOJUCHNO, --返品の元になった売上NO
                    A.BK_JUCHKBN,
                    A.JUCHDATE AS SHUKADATE,
                    (A.SOGOKEI - A.RIYOPOINT - A.SORYO) AS RETURNTOTAL
                FROM CIT80SALEH_IKOU A
                WHERE A.TORIKEIKBN = '01' --01：通販
                    AND A.BK_JUCHKBN IN ('20', '21', '22', '23') --返品
                    AND A.JUCHDATE >= CAST(TO_CHAR(DATEADD(DAY,- 1 - (365 * 3),SYSDATE()), 'YYYYMMDD') AS NUMERIC)
                    AND A.CANCELFLG = 0
                    AND A.SOGOKEI < 0 --返品交換のような返金が発生していないレコードは除く(返品数量を正しくカウントしたいので邪魔なレコードは除く)
                ) B
        WHERE A.KOKYANO = B.KOKYANO
            AND A.SALENO = B.HENMOTOJUCHNO
        --同じ商品を２回以上に分けて返品した場合にサマリする。
        GROUP BY A.KOKYANO,
            A.SALENO
        ) C
    INNER JOIN CIM01KOKYA D ON D.KOKYANO = C.KOKYANO
    --支払金額 > 返金額(つまり一部返品のみ抽出)
    WHERE ABS(C.URITOTAL) > ABS(C.RETURNTOTAL)
    ),
INSERT1
AS (
    SELECT *
    FROM ins
    
    UNION
    
    SELECT *
    FROM ins2
    ),
aa
AS (
    SELECT A.KOKYANO,
        A.PSALENO,
        A.URIKBN,
        A.URIDATE,
        (A.SOGOKEI - A.RIYOPOINT) AS URITOTAL,
        A.TENPOCODE
    FROM CIY80SALEH_IKOU A
    WHERE A.URIKBN = '10' --10:通常受注
        AND A.KOKYANO <> '00000000' -- 00000000以外
        AND A.KESSAIKBN <> '99' -- ポイント交換を除く
        AND A.SOUSAIPSALENO = '00000000000000000' -- 未訂正
        AND A.URIDATE >= CAST(TO_CHAR(DATEADD(DAY,- 1 - (365 * 3),SYSDATE()), 'YYYYMMDD') AS NUMERIC) --３年前～
        AND A.URIDATE <= CAST(TO_CHAR(DATEADD(DAY,- 1,SYSDATE()), 'YYYYMMDD') AS NUMERIC) --DWHサーバの場合、前日までしかデータがない
        AND A.CANCELFLG = 0
        AND A.SOGOKEI > 0
    ),
ins3
AS (
    SELECT /*+ INDEX(B) */
        A.KOKYANO,
        A.PSALENO,
        A.URIKBN,
        A.URIDATE,
        A.URITOTAL,
        0,
        A.TENPOCODE,
        '0'
    FROM aa A
    INNER JOIN CIM01KOKYA B ON B.KOKYANO = A.KOKYANO
    --返品があった売上レコード以外(つまり一番最新)の売上レコードを抽出する条件
    WHERE NOT EXISTS (
            SELECT 1
            FROM CIY80SALEH_IKOU C
            WHERE A.KOKYANO = C.KOKYANO
                AND A.PSALENO = C.HENMOTOJUCHNO
                AND C.URIKBN = '20' --返品
                AND C.KOKYANO <> '00000000' -- 00000000以外
                AND C.KESSAIKBN <> '99' -- ポイント交換を除く
                AND C.SOUSAIPSALENO = '00000000000000000' -- 未訂正
                AND C.URIDATE >= CAST(TO_CHAR(DATEADD(DAY,- 1 - (365 * 3),SYSDATE()), 'YYYYMMDD') AS NUMERIC)
                AND C.CANCELFLG = 0
                AND C.SOGOKEI < 0 --返品交換のような返金が発生していないレコードは除く(返品数量を正しくカウントしたいので邪魔なレコードは除く)
            )
    ),
ins4
AS (
    SELECT /*+ INDEX(D) */
        C.KOKYANO,
        C.PSALENO,
        C.URIKBN,
        C.URIDATE,
        C.URITOTAL,
        C.RETURNTOTAL,
        C.TENPOCODE,
        C.ITHIBU_HEPIN_FLG
    FROM (
        SELECT A.KOKYANO,
            A.PSALENO,
            MAX(A.URIKBN) AS URIKBN,
            MAX(A.URIDATE) AS URIDATE,
            MAX(A.URITOTAL) AS URITOTAL,
            SUM(B.RETURNTOTAL) AS RETURNTOTAL,
            MAX(A.TENPOCODE) AS TENPOCODE,
            '1' AS ITHIBU_HEPIN_FLG
        FROM aa A,
            (
                SELECT A.KOKYANO,
                    A.PSALENO, --返品NO
                    A.HENMOTOJUCHNO, --返品の元になった売上NO
                    A.URIKBN,
                    A.URIDATE,
                    (A.SOGOKEI - A.RIYOPOINT) AS RETURNTOTAL
                FROM CIY80SALEH_IKOU A
                WHERE A.URIKBN = '20' --返品
                    AND A.KOKYANO <> '00000000' -- 00000000以外
                    AND A.KESSAIKBN <> '99' -- ポイント交換を除く
                    AND A.SOUSAIPSALENO = '00000000000000000' -- 未訂正
                    AND A.URIDATE >= CAST(TO_CHAR(DATEADD(DAY,- 1 - (365 * 3),SYSDATE()), 'YYYYMMDD') AS NUMERIC)
                    AND A.CANCELFLG = 0
                    AND A.SOGOKEI < 0 --返品交換のような返金が発生していないレコードは除く(返品数量を正しくカウントしたいので邪魔なレコードは除く)
                ) B
        WHERE A.KOKYANO = B.KOKYANO
            AND A.PSALENO = B.HENMOTOJUCHNO
        --同じ商品を２回以上に分けて返品した場合にサマリする。
        GROUP BY A.KOKYANO,
            A.PSALENO
        ) C
    INNER JOIN CIM01KOKYA D ON D.KOKYANO = C.KOKYANO
    --支払金額 > 返金額(つまり一部返品のみ抽出)
    WHERE ABS(C.URITOTAL) > ABS(C.RETURNTOTAL)
    ),
INSERT2
AS (
    SELECT *
    FROM INS3
    
    UNION
    
    SELECT *
    FROM INS4
    ),
UNION_OF_INSERTS
AS (
    SELECT *
    FROM INSERT1
    
    UNION
    
    SELECT *
    FROM INSERT2
    ),
FINAL AS
(
    SELECT KOKYANO::NUMBER(10,0) AS KOKYANO,
        SALENO::VARCHAR(40) AS SALENO,
        BK_JUCHKBN::VARCHAR(3) AS JUCHKBN,
        SHUKADATE::NUMBER(8,0) AS SHUKADATE,
        URITOTAL::NUMBER(12,0) AS URITOTAL,
        RETURNTOTAL::NUMBER(12,0) AS RETURNTOTAL,
        TENPOCODE::VARCHAR(8) AS TENPOCODE,
        ITHIBU_HEPIN_FLG::VARCHAR(2) AS ITHIBU_HEPIN_FLG
    FROM UNION_OF_INSERTS
)
SELECT *
FROM FINAL