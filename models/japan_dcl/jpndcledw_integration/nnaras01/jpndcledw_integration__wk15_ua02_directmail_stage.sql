with kr_this_stage_point_daily as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.KR_THIS_STAGE_POINT_DAILY
),
cim01kokya as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIM01KOKYA
),
kr_new_stage_point as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.KR_NEW_STAGE_POINT
),
max_yyyy_dly AS (
SELECT 
 DISTINCT usrid
,LAST_VALUE(yyyymm IGNORE nulls) OVER (PARTITION BY usrid ORDER BY yyyymm ROWS BETWEEN unbounded preceding AND unbounded following) AS yyyymm
FROM  kr_this_stage_point_daily
)
,max_yyyy_monthly AS (
SELECT 
 DISTINCT usrid
,LAST_VALUE(yyyymm IGNORE nulls) OVER (PARTITION BY usrid ORDER BY yyyymm ROWS BETWEEN unbounded preceding AND unbounded following) AS yyyymm
FROM  kr_new_stage_point
),
transformed as (
SELECT 
 ck.kokyano AS customer_no
,NVL(kr.promo_stage,'ゴールド')  AS promo_stage               
,NVL(kr.next_promo_stage_amt,15000) AS next_promo_stage_amt
,NVL(kr.next_promo_stage_point,500) AS next_promo_stage_point
,NVL(kr.point_tobe_granted,0) AS point_tobe_granted
,NVL(kr.stage,'レギュラー') AS stage
,NVL(kr_new.stage,'レギュラー') AS stage_monthly
,NVL(kr.thistotalprc,0) AS totalprc_this_year 
FROM cim01kokya ck
LEFT JOIN max_yyyy_dly
ON ck.kokyano=NVL(LPAD(max_yyyy_dly.usrid,10,'0'),'0000000000')
LEFT JOIN kr_this_stage_point_daily kr
ON kr.usrid=max_yyyy_dly.usrid AND kr.yyyymm=max_yyyy_dly.yyyymm
LEFT JOIN max_yyyy_monthly
ON ck.kokyano=NVL(LPAD(max_yyyy_monthly.usrid,10,'0'),'0000000000')
LEFT JOIN kr_new_stage_point kr_new
ON kr_new.usrid=max_yyyy_monthly.usrid AND kr_new.yyyymm=max_yyyy_monthly.yyyymm
WHERE ck.testusrflg = '通常ユーザ' 
),
final as (
select 
customer_no::varchar(68) as customer_no,
promo_stage::varchar(18) as promo_stage,
next_promo_stage_amt::number(18,0) as next_promo_stage_amt,
next_promo_stage_point::number(18,0) as next_promo_stage_point,
point_tobe_granted::number(18,0) as point_tobe_granted,
stage::varchar(18) as stage,
stage_monthly::varchar(18) as stage_monthly,
totalprc_this_year::number(38,0) as totalprc_this_year
from 
transformed
)
select * from final