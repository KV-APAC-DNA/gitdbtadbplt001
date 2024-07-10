with tbecorder as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.TBECORDER
),
TBUSRPRAM as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.TBUSRPRAM
),
transformed as (
  SELECT WK.diEcUsrID,
         SUM(WK.KINGAKU) AS KINGAKU_SUM
    FROM 
    (
    SELECT o.diEcUsrID AS diEcUsrID,
           SUM(DISEIKYUPRC) AS KINGAKU
      FROM tbEcOrder o
      INNER JOIN TBUSRPRAM UP
      ON UP.DIUSRID = o.diEcUsrID
     WHERE (o.dirouteid = '7' OR o.dirouteid = '8' OR o.dirouteid = '9' )
       AND TO_CHAR(o.DSORDERDT,'YYYYMM') >= TO_CHAR(ADD_MONTHS(CONVERT_TIMEZONE('UTC','Asia/Tokyo',current_timestamp()),-36) ,'YYYYMM')
       AND TO_CHAR(o.DSORDERDT,'YYYYMMDD') <= TO_CHAR(LAST_DAY(ADD_MONTHS(CONVERT_TIMEZONE('UTC','Asia/Tokyo',current_timestamp()),-0)) ,'YYYYMMDD') 
       AND o.c_diallhenpinflg = '0'
       AND o.diCancel = '0'
       AND o.dielimflg = '0'
     GROUP BY o.diEcUsrID
    ) WK
GROUP BY
    WK.diEcUsrID
),
final as (
select
diEcUsrID::NUMBER(10,0) as DIUSRID ,
KINGAKU_SUM::NUMBER(10,0) as KINGAKU_SUM
from transformed
)
select * from final