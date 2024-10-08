{{
    config(
        materialized='incremental',
        incremental_strategy = 'append',
        pre_hook = "{{build_wk_birthday_cinextbio_job()}}"
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
WK
AS (
    SELECT diEcUsrID,
        COUNT(diOrderID) AS diOrderCnt,
        MAX(c_dsShukkaDate) AS c_dsShukkaDate
    FROM (
        /* バイオプトロンの購入回数は通販に含める */
        SELECT o.diEcUsrID,
            o.diOrderID,
            TO_CHAR(s.c_dsShukkaDate, 'YYYYMMDD') AS c_dsShukkaDate
        FROM tbEcOrder o
        INNER JOIN c_tbEcShippingResults s ON o.diOrderID = s.diOrderID
        WHERE (
                o.dirouteid <> '7'
                AND o.dirouteid <> '8'
                AND o.dirouteid <> '9'
                )
            AND TO_CHAR(s.c_dsShukkaDate, 'YYYYMMDD') >= TO_CHAR(dateadd(day, - 181, current_timestamp()), 'YYYYMMDD')
            --TO_CHAR(dateadd(day, - 181, current_timestamp()), 'YYYYMMDD')
            AND o.c_diallhenpinflg = '0'
            AND o.diCancel = '0'
            AND o.dielimflg = '0'

        UNION

        SELECT diEcUsrid,
            c_diaddadmid,
            TO_CHAR(dsOrderDt, 'YYYYMMDD') AS c_dsShukkaDate
        FROM c_tbEcRankAddAmountAdm
        WHERE TO_CHAR(dsOrderDt, 'YYYYMMDD') >= TO_CHAR(dateadd(day, - 181, current_timestamp()), 'YYYYMMDD')
        --TO_CHAR(dateadd(day, - 181, current_timestamp()), 'YYYYMMDD')
        --TO_CHAR(dateadd(day, - 181, current_timestamp()), 'YYYYMMDD')
            AND dielimflg = 0
        )
    GROUP BY diEcUsrID
    ),
T1
AS (
    SELECT WK.diEcUsrID::NUMBER(10,0) AS DIUSRID,
        WK.diOrderCnt::NUMBER(10,0) AS DIORDERCNT,
        WK.c_dsShukkaDate::VARCHAR(8) AS C_DSSHUKKADATE,
        ''::VARCHAR(1) AS JUDGEKBN,
        ''::VARCHAR(3) AS DIMONTH
    FROM WK
    INNER JOIN TBUSRPRAM UP ON UP.DIUSRID = WK.diEcUsrID

    ),
final AS
(
    select DIUSRID::NUMBER(10,0) AS DIUSRID,
        DIORDERCNT::NUMBER(10,0) AS DIORDERCNT,
        C_DSSHUKKADATE::VARCHAR(8) AS C_DSSHUKKADATE,
        JUDGEKBN::VARCHAR(1) AS JUDGEKBN,
        DIMONTH::VARCHAR(3) AS DIMONTH
    from t1
    WHERE NOT EXISTS (SELECT 'X'
                        FROM {{this}}
                       WHERE DIUSRID = t1.DIUSRID
            )
)
SELECT *
FROM final