{{
    config(
        materialized='table',
        pre_hook = "{{build_wk_birthday_cinextstore_job()}}"
    )
}}


with tbEcOrder
as
(
    select * from {{ ref('jpndclitg_integration__tbecorder') }}
),
TBUSRPRAM as
(
    select * from {{ ref('jpndclitg_integration__tbusrpram') }}
),
final as
(
    SELECT WK.diEcUsrID::NUMBER(10,0) AS DIUSRID,
         WK.diOrderCnt::NUMBER(10,0) AS DIORDERCNT,
         WK.c_dsShukkaDate::VARCHAR(8) AS C_DSSHUKKADATE,
         null::VARCHAR(1) AS JUDGEKBN,
         null::VARCHAR(3) AS DIMONTH
    FROM (
    SELECT o.diEcUsrID,
           COUNT(o.diOrderID) AS diOrderCnt,
           MAX(TO_CHAR(o.DSORDERDT,'YYYYMMDD')) AS c_dsShukkaDate
      FROM tbEcOrder o
     WHERE (o.dirouteid = '7' OR o.dirouteid = '8' OR o.dirouteid = '9' )
    --    AND TO_CHAR(o.DSORDERDT,'YYYYMMDD') >= TO_CHAR(dateadd(day,-181,'2024-07-25'),'YYYYMMDD')
       AND TO_CHAR(o.DSORDERDT,'YYYYMMDD') >= TO_CHAR(dateadd(day,-181,SYSDATE()),'YYYYMMDD')
       AND o.c_diallhenpinflg = '0'
       AND o.diCancel = '0'
       AND o.dielimflg = '0'
     GROUP BY o.diEcUsrID) WK
   INNER JOIN TBUSRPRAM UP
      ON UP.DIUSRID = WK.diEcUsrID
   WHERE NOT EXISTS (SELECT 'X'
                       FROM {{this}}
                      WHERE DIUSRID = UP.DIUSRID)
)
select * from final