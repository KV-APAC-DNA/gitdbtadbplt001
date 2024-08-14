{{
    config(
        pre_hook = "{{build_wk_birthday_cinextusrpram()}}"     
    )
}}
WITH TBUSRPRAM
AS (
    SELECT *
    FROM {{ ref('jpndclitg_integration__tbusrpram') }}
    ),
tbEcOrder
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
A
AS (
    SELECT memberinfo.diusrid,
        memberinfo.dsdat60,
        memberinfo.dsdat61,
        TO_CHAR(orderinfo.DSORDERDT, 'YYYYMMDD') AS c_dsShukkaDate,
        left(orderinfo.dirouteid,1)::char AS dirouteid --,TO_CHAR(orderinfo.dirouteid) AS dirouteid
        ,
        memberinfo.disecessionflg,
        memberinfo.dielimflg
    FROM TBUSRPRAM memberinfo
    INNER JOIN tbEcOrder orderinfo ON rtrim(orderinfo.diEcUsrID) = rtrim(memberinfo.diusrid)
        AND orderinfo.dirouteid NOT IN ('7', '8', '9', '12', '13')
    INNER JOIN c_tbEcShippingResults ship ON rtrim(orderinfo.diOrderID) = rtrim(ship.diOrderID)
    WHERE TO_CHAR(orderinfo.DSORDERDT, 'YYYYMMDD') >= TO_CHAR(ADD_MONTHS(CONVERT_TIMEZONE( 'Asia/Tokyo', SYSDATE()), - 36), 'YYYYMMDD')
        -- 
        AND orderinfo.c_diallhenpinflg = '0'
        AND orderinfo.diCancel = '0'
        AND orderinfo.dielimflg = '0'
    ),
B
AS (
    SELECT memberinfo.diusrid,
        memberinfo.dsdat60,
        memberinfo.dsdat61,
        TO_CHAR(orderinfo.DSORDERDT, 'YYYYMMDD') AS c_dsShukkaDate,
        left(orderinfo.dirouteid,1)::char AS dirouteid --,TO_CHAR(orderinfo.dirouteid) AS dirouteid
        ,
        memberinfo.disecessionflg,
        memberinfo.dielimflg
    FROM TBUSRPRAM memberinfo
    INNER JOIN tbEcOrder orderinfo ON rtrim(orderinfo.diEcUsrID) = rtrim(memberinfo.diusrid)
        AND (
            orderinfo.dirouteid = '7'
            OR orderinfo.dirouteid = '8'
            OR orderinfo.dirouteid = '9'
            )
    WHERE TO_CHAR(orderinfo.DSORDERDT, 'YYYYMMDD') >= TO_CHAR(ADD_MONTHS(CONVERT_TIMEZONE('Asia/Tokyo', SYSDATE()), - 36), 'YYYYMMDD')
        
        AND orderinfo.c_diallhenpinflg = '0'
        AND orderinfo.diCancel = '0'
        AND orderinfo.dielimflg = '0'
    ),
C
AS (
    SELECT memberinfo.diUsrid,
        memberinfo.dsdat60,
        memberinfo.dsdat61,
        TO_CHAR(amountadm.dsOrderDt, 'YYYYMMDD') AS c_dsShukkaDate,
        '1'::VARCHAR AS dirouteid,
        memberinfo.disecessionflg,
        memberinfo.dielimflg
    FROM c_tbEcRankAddAmountAdm amountadm
    INNER JOIN TBUSRPRAM memberinfo ON rtrim(memberinfo.diusrid) = rtrim(amountadm.diecusrid)
    WHERE TO_CHAR(amountadm.dsOrderDt, 'YYYYMMDD') >=  TO_CHAR(ADD_MONTHS(CONVERT_TIMEZONE( 'Asia/Tokyo', SYSDATE()), - 36), 'YYYYMMDD')
    --TO_CHAR(ADD_MONTHS(CONVERT_TIMEZONE('UTC', 'Asia/Tokyo', '2024-07-01'::DATE), - 36), 'YYYYMMDD')
        AND amountadm.dielimflg = 0
    ),
union_of AS
 (
        SELECT *
        FROM A
        
        UNION
        
        SELECT *
        FROM B
        
        UNION
        
        SELECT *
        FROM C
        ),
WK
AS (
    SELECT diusrid::NUMBER(10, 0) AS DIUSRID,
        dsdat60::VARCHAR(4000) AS DSDAT60,
        dsdat61::VARCHAR(4000) AS DSDAT61,
        max(c_dsshukkadate || '_' || dirouteid)::VARCHAR(400) AS SHUKKADT_ROUTEID,
        disecessionflg::VARCHAR(100) AS DISECESSIONFLG,
        dielimflg::VARCHAR(100) AS DIELIMFLG
    FROM union_of
    GROUP BY diusrid,
        dsdat60,
        dsdat61,
        disecessionflg,
        dielimflg
    ),
t1
AS (
    SELECT WK.diusrid::NUMBER(10, 0) AS DIUSRID,
        WK.dsdat60::VARCHAR(4000) AS DSDAT60,
        WK.dsdat61::VARCHAR(4000) AS DSDAT61,
        WK.SHUKKADT_ROUTEID::VARCHAR(400) AS SHUKKADT_ROUTEID,
        WK.disecessionflg::VARCHAR(4) AS DISECESSIONFLG,
        WK.dielimflg::VARCHAR(4) AS DIELIMFLG,
        ''::VARCHAR(4) AS JUDGEKBN,
        ''::VARCHAR(32) AS DIMONTH
    FROM WK
    INNER JOIN TBUSRPRAM UP 
        ON rtrim(UP.DIUSRID) = rtrim(WK.DIUSRID)
    ),
final as
(
    select * from t1
    WHERE NOT EXISTS (SELECT 'X'
                        FROM {{this}}
                       WHERE DIUSRID = t1.DIUSRID)
)
SELECT *
FROM FINAL
