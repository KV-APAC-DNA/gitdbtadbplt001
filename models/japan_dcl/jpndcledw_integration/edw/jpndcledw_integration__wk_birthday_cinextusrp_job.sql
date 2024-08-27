{{
    config(
        materialized='incremental',
        incremental_strategy = 'append',
        pre_hook = "{{build_wk_birthday_cinextusrp_job()}}"
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
    FROM  {{ ref('jpndclitg_integration__c_tbecrankaddamountadm') }}
    ),
t1
AS (
    SELECT tbus.diusrid,
        tbus.dsdat60,
        tbus.dsdat61,
        TO_CHAR(tbec.DSORDERDT, 'YYYYMMDD') AS c_dsShukkaDate,
        CAST(tbec.dirouteid AS VARCHAR) AS dirouteid,
        tbus.disecessionflg,
        tbus.dielimflg
    FROM TBUSRPRAM tbus
    INNER JOIN tbEcOrder tbec ON tbec.diEcUsrID = tbus.diusrid
        AND tbec.dirouteid <> '7'
        AND tbec.dirouteid <> '8'
        AND tbec.dirouteid <> '9'
    INNER JOIN c_tbEcShippingResults ship ON tbec.diOrderID = ship.diOrderID
    WHERE TO_CHAR(tbec.DSORDERDT, 'YYYYMMDD') >= TO_CHAR(ADD_MONTHS(current_timestamp(), - 36), 'YYYYMMDD')
    --TO_CHAR(ADD_MONTHS('2024-07-25'::DATE, - 36), 'YYYYMMDD')
        AND tbec.c_diallhenpinflg = '0'
        AND tbec.diCancel = '0'
        AND tbec.dielimflg = '0'
    ),
t2
AS (
    SELECT tbus.diusrid,
        tbus.dsdat60,
        tbus.dsdat61,
        TO_CHAR(tbec.DSORDERDT, 'YYYYMMDD') AS c_dsShukkaDate,
        CAST(tbec.dirouteid AS VARCHAR) AS dirouteid,
        tbus.disecessionflg,
        tbus.dielimflg
    FROM TBUSRPRAM tbus
    INNER JOIN tbEcOrder tbec ON tbec.diEcUsrID = tbus.diusrid
        AND (
            tbec.dirouteid = '7'
            OR tbec.dirouteid = '8'
            OR tbec.dirouteid = '9'
            )
    WHERE TO_CHAR(tbec.DSORDERDT, 'YYYYMMDD') >= TO_CHAR(ADD_MONTHS(current_timestamp(), - 36), 'YYYYMMDD')
    --TO_CHAR(ADD_MONTHS('2024-07-25'::DATE, - 36), 'YYYYMMDD')
    --TO_CHAR(ADD_MONTHS(current_timestamp(), - 36), 'YYYYMMDD')
        AND tbec.c_diallhenpinflg = '0'
        AND tbec.diCancel = '0'
        AND tbec.dielimflg = '0'
    ),
t3
AS (
    SELECT tbus.diUsrid,
        tbus.dsdat60,
        tbus.dsdat61,
        TO_CHAR(tbecrank.dsOrderDt, 'YYYYMMDD') AS c_dsShukkaDate,
        '1' AS dirouteid,
        tbus.disecessionflg,
        tbus.dielimflg
    FROM c_tbEcRankAddAmountAdm tbecrank
    INNER JOIN TBUSRPRAM tbus ON tbus.diusrid = tbecrank.diecusrid
    WHERE TO_CHAR(tbecrank.dsOrderDt, 'YYYYMMDD') >= TO_CHAR(ADD_MONTHS(current_timestamp(), - 36), 'YYYYMMDD')
    --TO_CHAR(ADD_MONTHS('2024-07-25'::DATE, - 36), 'YYYYMMDD')
    --TO_CHAR(ADD_MONTHS(current_timestamp(), - 36), 'YYYYMMDD')
        AND tbecrank.dielimflg = 0
    ),
UNION_OF
AS (
    SELECT *
    FROM t1
    
    UNION
    
    SELECT *
    FROM t2
    
    UNION
    
    SELECT *
    FROM t3
    ),
WK
AS (
    SELECT diusrid,
        dsdat60,
        dsdat61,
        max(c_dsshukkadate || '_' || dirouteid) AS SHUKKADT_ROUTEID,
        disecessionflg,
        dielimflg
    FROM UNION_OF
    GROUP BY diusrid,
        dsdat60,
        dsdat61,
        disecessionflg,
        dielimflg
    ),
prefinal
AS (
    SELECT WK.diusrid,
        WK.dsdat60,
        WK.dsdat61,
        WK.SHUKKADT_ROUTEID,
        WK.disecessionflg,
        WK.dielimflg,
        '' AS JUDGEKBN,
        '' AS DIMONTH
    FROM WK
    INNER JOIN TBUSRPRAM UP ON UP.DIUSRID = WK.DIUSRID
),
final AS
(
    SELECT WK.diusrid::NUMBER(10,0) AS diusrid,
        WK.dsdat60::VARCHAR(6000) AS dsdat60,
        WK.dsdat61::VARCHAR(6000) AS dsdat61,
        WK.SHUKKADT_ROUTEID::VARCHAR(150) AS SHUKKADT_ROUTEID,
        WK.disecessionflg::VARCHAR(1) AS disecessionflg,
        WK.dielimflg::VARCHAR(1) AS dielimflg,
        WK.JUDGEKBN::VARCHAR(1) AS JUDGEKBN,
        WK.DIMONTH::VARCHAR(3) AS DIMONTH
    FROM prefinal WK
    WHERE NOT EXISTS (
                        SELECT 'X' 
                        FROM {{this}} a
                        WHERE a.DIUSRID = WK.DIUSRID
                    )
)
SELECT *
FROM

final