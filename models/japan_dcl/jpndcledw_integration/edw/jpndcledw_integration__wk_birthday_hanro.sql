WITH WK_BIRTHDAY_CINEXTUSRPRAM
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__wk_birthday_cinextusrpram') }}
    ),
WK_BIRTHDAY_CINEXTBIOPTRON as
(
    SELECT *
    FROM {{ ref('jpndcledw_integration__wk_birthday_cinextbioptron') }}
),
WK_BIRTHDAY_CINEXTSTORE as
(
    SELECT *
    FROM {{ ref('jpndcledw_integration__wk_birthday_cinextstore') }}
),
union_of as
(
   SELECT meminfo.diusrid AS diusrid,
        CASE 
            WHEN NVL(mailorder.diOrderCnt, 0) > NVL(store.diOrderCnt, 0)
                AND meminfo.dsdat60 = '希望'
                THEN '通販'
            WHEN NVL(mailorder.diOrderCnt, 0) > NVL(store.diOrderCnt, 0)
                AND meminfo.dsdat60 = '拒否'
                AND meminfo.dsdat61 = '拒否'
                THEN '通販'
            WHEN NVL(mailorder.diOrderCnt, 0) > NVL(store.diOrderCnt, 0)
                AND meminfo.dsdat60 = '拒否'
                AND meminfo.dsdat61 = '希望'
                THEN '店舗'
            WHEN NVL(mailorder.diOrderCnt, 0) < NVL(store.diOrderCnt, 0)
                AND meminfo.dsdat61 = '希望'
                THEN '店舗'
            WHEN NVL(mailorder.diOrderCnt, 0) < NVL(store.diOrderCnt, 0)
                AND meminfo.dsdat61 = '拒否'
                AND meminfo.dsdat60 = '拒否'
                THEN '店舗'
            WHEN NVL(mailorder.diOrderCnt, 0) < NVL(store.diOrderCnt, 0)
                AND meminfo.dsdat61 = '拒否'
                AND meminfo.dsdat60 = '希望'
                THEN '通販'
            WHEN NVL(mailorder.diOrderCnt, 0) <> 0
                AND NVL(mailorder.diOrderCnt, 0) = NVL(store.diOrderCnt, 0)
                AND mailorder.c_dsShukkaDate >= store.c_dsShukkaDate
                AND meminfo.dsdat60 = '希望'
                THEN '通販'
            WHEN NVL(mailorder.diOrderCnt, 0) <> 0
                AND NVL(mailorder.diOrderCnt, 0) = NVL(store.diOrderCnt, 0)
                AND mailorder.c_dsShukkaDate >= store.c_dsShukkaDate
                AND meminfo.dsdat60 = '拒否'
                AND meminfo.dsdat61 = '拒否'
                THEN '通販'
            WHEN NVL(mailorder.diOrderCnt, 0) <> 0
                AND NVL(mailorder.diOrderCnt, 0) = NVL(store.diOrderCnt, 0)
                AND mailorder.c_dsShukkaDate >= store.c_dsShukkaDate
                AND meminfo.dsdat60 = '拒否'
                AND meminfo.dsdat61 = '希望'
                THEN '店舗'
            WHEN NVL(mailorder.diOrderCnt, 0) <> 0
                AND NVL(mailorder.diOrderCnt, 0) = NVL(store.diOrderCnt, 0)
                AND mailorder.c_dsShukkaDate < store.c_dsShukkaDate
                AND meminfo.dsdat61 = '希望'
                THEN '店舗'
            WHEN NVL(mailorder.diOrderCnt, 0) <> 0
                AND NVL(mailorder.diOrderCnt, 0) = NVL(store.diOrderCnt, 0)
                AND mailorder.c_dsShukkaDate < store.c_dsShukkaDate
                AND meminfo.dsdat61 = '拒否'
                AND meminfo.dsdat60 = '拒否'
                THEN '店舗'
            WHEN NVL(mailorder.diOrderCnt, 0) <> 0
                AND NVL(mailorder.diOrderCnt, 0) = NVL(store.diOrderCnt, 0)
                AND mailorder.c_dsShukkaDate < store.c_dsShukkaDate
                AND meminfo.dsdat61 = '拒否'
                AND meminfo.dsdat60 = '希望'
                THEN '通販'
            WHEN NVL(mailorder.diOrderCnt, 0) = 0
                AND NVL(store.diOrderCnt, 0) = 0
                AND meminfo.dsdat60 = '希望'
                AND meminfo.dsdat61 = '希望'
                AND (
                    SUBSTRING(meminfo.ShukkaDT_RouteID, 10, 1) <> '7'
                    AND SUBSTRING(meminfo.ShukkaDT_RouteID, 10, 1) <> '8'
                    AND SUBSTRING(meminfo.ShukkaDT_RouteID, 10, 1) <> '9'
                    )
                THEN '通販'
            WHEN NVL(mailorder.diOrderCnt, 0) = 0
                AND NVL(store.diOrderCnt, 0) = 0
                AND meminfo.dsdat60 = '希望'
                AND meminfo.dsdat61 = '希望'
                AND (
                    SUBSTRING(meminfo.ShukkaDT_RouteID, 10, 1) = '7'
                    OR SUBSTRING(meminfo.ShukkaDT_RouteID, 10, 1) = '8'
                    OR SUBSTRING(meminfo.ShukkaDT_RouteID, 10, 1) = '9'
                    )
                THEN '店舗'
            WHEN NVL(mailorder.diOrderCnt, 0) = 0
                AND NVL(store.diOrderCnt, 0) = 0
                AND meminfo.dsdat60 = '希望'
                AND meminfo.dsdat61 = '拒否'
                THEN '通販'
            WHEN NVL(mailorder.diOrderCnt, 0) = 0
                AND NVL(store.diOrderCnt, 0) = 0
                AND meminfo.dsdat60 = '拒否'
                AND meminfo.dsdat61 = '希望'
                THEN '店舗'
            WHEN NVL(mailorder.diOrderCnt, 0) = 0
                AND NVL(store.diOrderCnt, 0) = 0
                AND meminfo.dsdat60 = '拒否'
                AND meminfo.dsdat61 = '拒否'
                THEN '通販'
            ELSE ''
            END custclass,
        CASE 
            WHEN NVL(mailorder.c_dsShukkaDate, '00000000') <> '00000000'
                AND NVL(mailorder.c_dsShukkaDate, '00000000') >= NVL(store.c_dsShukkaDate, '00000000')
                THEN mailorder.c_dsShukkaDate
            WHEN NVL(store.c_dsShukkaDate, '00000000') <> '00000000'
                THEN store.c_dsShukkaDate
            ELSE SUBSTRING(meminfo.ShukkaDT_RouteID, 1, 8)
            END shipdt
    FROM WK_BIRTHDAY_CINEXTUSRPRAM meminfo
    LEFT OUTER JOIN WK_BIRTHDAY_CINEXTBIOPTRON mailorder ON meminfo.diusrid = mailorder.diusrid
    LEFT OUTER JOIN WK_BIRTHDAY_CINEXTSTORE store ON meminfo.diusrid = store.diusrid
    WHERE meminfo.disecessionflg = '0'
        AND meminfo.dielimflg = '0'
),
final
AS (
    SELECT DIUSRID::NUMBER(10,0) AS DIUSRID,
        SUBSTRING(MAX(shipdt || '_' || custclass), POSITION('_' IN MAX(shipdt || '_' || custclass)) + 1, LENGTH(MAX(shipdt || '_' || custclass)))::VARCHAR(40) AS HANRO
        ,
        MAX(shipdt)::VARCHAR(32) AS SHUKADATE
    FROM union_of
    GROUP BY DIUSRID
    )
SELECT *
FROM final
