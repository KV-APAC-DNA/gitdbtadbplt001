with KR_054_SLOST_MEISAI as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.KR_054_SLOST_MEISAI
),
transformed as (
SELECT
       '102_失効P' as lost_label ,
       SUBSTRING(LPOINT_LIMIT,1,6) ::varchar as LOST_YM,
       SUBSTRING(LPOINT_LIMIT,1,4) ::varchar as LOST_YY,
       SUBSTRING(LPOINT_LIMIT,5,2) ::varchar as LOST_MM,
       SUM(NVL(LPOINT,0))*(-1) as LPOINT
 FROM
       KR_054_SLOST_MEISAI
 GROUP BY
       lost_label,
       SUBSTRING(LPOINT_LIMIT,1,6) ::varchar,
       SUBSTRING(LPOINT_LIMIT,1,4) ::varchar,
       SUBSTRING(LPOINT_LIMIT,5,2) ::varchar
),
final as (
select
lost_label::varchar(30) as lost_label,
lost_ym::varchar(9) as lost_ym,
lost_yy::varchar(6) as lost_yy,
lost_mm::varchar(3) as lost_mm,
lpoint::number(38,0) as lpoint
from transformed
)
select * from final