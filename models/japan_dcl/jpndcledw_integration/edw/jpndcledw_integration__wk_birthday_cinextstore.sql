{{
    config(
        pre_hook = "{{build_wk_birthday_cinextstore()}}"     
    )
}}
with tbEcOrder
as
(
    select * from {{ ref('jpndclitg_integration__tbecorder') }}
),
UP AS
(
 SELECT * FROM {{ ref('jpndclitg_integration__tbusrpram') }}
),
WK as
(
    SELECT o.diEcUsrID,
           COUNT(o.diOrderID) AS diOrderCnt,
           MAX(TO_CHAR(o.DSORDERDT,'YYYYMMDD')) AS c_dsShukkaDate
      FROM tbEcOrder o
     WHERE (o.dirouteid = '7' OR o.dirouteid = '8' OR o.dirouteid = '9' )
       AND TO_CHAR(o.DSORDERDT,'YYYYMMDD') >= TO_CHAR(DATEADD(DAY, -181, CONVERT_TIMEZONE('UTC','Asia/Tokyo','2024-07-01'::DATE)),'YYYYMMDD') 
       --TO_CHAR(DATEADD(DAY, -181, CONVERT_TIMEZONE('UTC','Asia/Tokyo',current_timestamp())),'YYYYMMDD') 
       AND o.c_diallhenpinflg = '0'
       AND o.diCancel = '0'
       AND o.dielimflg = '0'
     GROUP BY o.diEcUsrID
),
final as
(
    SELECT WK.diEcUsrID::NUMBER(10,0) AS DIUSRID,
         WK.diOrderCnt::NUMBER(10,0) AS DIORDERCNT,
         WK.c_dsShukkaDate::VARCHAR(32) AS C_DSSHUKKADATE,
         ''::VARCHAR(4) AS JUDGEKBN,
         ''::VARCHAR(10) AS DIMONTH
    FROM WK
   INNER JOIN UP
      ON UP.DIUSRID = WK.diEcUsrID
   WHERE NOT EXISTS (SELECT 'X'
                       FROM {{this}}
                      WHERE DIUSRID = UP.DIUSRID)
)
select * from final