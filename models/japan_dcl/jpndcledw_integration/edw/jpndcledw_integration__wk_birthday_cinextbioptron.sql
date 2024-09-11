{{
    config(
        pre_hook = "{{build_wk_birthday_cinextbioptron()}}"
    )
}}

WITH tbEcOrder
AS (
    SELECT *
    FROM {{ ref('jpndclitg_integration__tbecorder') }}
    ),
c_tbEcShippingResults
AS (
    SELECT *
    FROM {{ ref('jpndclitg_integration__c_tbecshippingresults') }}
    ),
c_tbEcRankAddAmountAdm
AS (
    SELECT *
    FROM {{ ref('jpndclitg_integration__c_tbecrankaddamountadm') }}
    ),
TBUSRPRAM
AS (
    SELECT *
    FROM {{ ref('jpndclitg_integration__tbusrpram') }}
    ),
order_join_shipping
AS (
    SELECT o.diEcUsrID,
        o.diOrderID,
        TO_CHAR(s.c_dsShukkaDate, 'YYYYMMDD') AS c_dsShukkaDate
    FROM tbEcOrder o
    INNER JOIN c_tbEcShippingResults s ON o.diOrderID = s.diOrderID
    WHERE o.dirouteid NOT IN ('7', '8', '9', '12', '13')
        AND TO_CHAR(s.c_dsShukkaDate, 'YYYYMMDD') >= TO_CHAR(dateadd(day,-181,(convert_timezone('Asia/Tokyo', current_timestamp()))) , 'YYYYMMDD')
        --TO_CHAR(dateadd(day,-181,(convert_timezone('Asia/Tokyo', current_timestamp()))) , 'YYYYMMDD')
        --TO_CHAR((TRUNC(convert_timezone('Asia/Tokyo', SYSDATE)) - 181), 'YYYYMMDD')
        --TO_CHAR((TO_DATE('&1','YYYYMMDD') - 181),'YYYYMMDD')
        AND o.c_diallhenpinflg = '0'
        AND o.diCancel = '0'
        AND o.dielimflg = '0'
    ),
rankadd
AS (
    SELECT diEcUsrid,
        c_diaddadmid,
        TO_CHAR(dsOrderDt, 'YYYYMMDD') AS c_dsShukkaDate
    FROM c_tbEcRankAddAmountAdm
    WHERE TO_CHAR(dsOrderDt, 'YYYYMMDD') >= TO_CHAR(dateadd(day,-181,(convert_timezone('Asia/Tokyo', current_timestamp()))) , 'YYYYMMDD')
    --TO_CHAR(dateadd(day,-181,(convert_timezone('Asia/Tokyo', current_timestamp()))) , 'YYYYMMDD')
    --TO_CHAR((TRUNC(convert_timezone('Asia/Tokyo', SYSDATE)) - 181), 'YYYYMMDD')
    --TO_CHAR((TO_DATE('&1','YYYYMMDD') - 181),'YYYYMMDD')
        AND dielimflg = 0
    ),
WK
AS (
    SELECT diEcUsrID,
        COUNT(diOrderID) AS diOrderCnt,
        MAX(c_dsShukkaDate) AS c_dsShukkaDate
    FROM (
        SELECT *
        FROM order_join_shipping

        UNION

        SELECT *
        FROM rankadd
        )
    GROUP BY diEcUsrID
    ),
t1
AS (
    SELECT WK.diEcUsrID::NUMBER(10,0) AS DIUSRID,
        WK.diOrderCnt::NUMBER(10,0) AS DIORDERCNT,
        WK.c_dsShukkaDate::VARCHAR(64) AS C_DSSHUKKADATE,
        ''::VARCHAR(16) AS JUDGEKBN,
        ''::VARCHAR(32) AS DIMONTH
    FROM WK
    INNER JOIN TBUSRPRAM AS UP ON UP.DIUSRID = WK.diEcUsrID

    ),
final as
(
    select *
    from t1
    WHERE NOT EXISTS (SELECT 'X'
                        FROM {{this}}
                       WHERE DIUSRID = t1.DIUSRID)
)
SELECT *
FROM final
