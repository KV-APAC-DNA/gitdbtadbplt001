with wk22_ua02_behavior02_ml_prediction_result as (
select * from {{ ref('jpndcledw_integration__wk22_ua02_behavior02_ml_prediction_result') }}),
wk21_ua02_behavior02_subscription_flg as (
select * from {{ ref('jpndcledw_integration__wk21_ua02_behavior02_subscription_flg') }}
),
ua01_base_cim01kokya_v as (
select * from {{ ref('jpndcledw_integration__ua01_base_cim01kokya_v') }}
),
wk20_ua02_behavior02_outcall_flg as (
select * from {{ ref('jpndcledw_integration__wk20_ua02_behavior02_outcall_flg') }}
),
wk19_ua02_behavior02_expired_point as (
select * from {{ ref('jpndcledw_integration__wk19_ua02_behavior02_expired_point') }}
),
transformed as (
SELECT 
DISTINCT ckv.kokyano 	 
,a.expired_point_this_month
,a.expired_point_next_month
,b.outcall_exc_flg
,b.outcall_hist_flg_3m
,c.subscription_flg
,d.ecpropensity
,d.ccpropensity
,d.acgelpropensity
,d.vc100propensity
,d.cluster5_cd
,d.cluster5_nm
FROM ua01_base_cim01kokya_v ckv 
LEFT JOIN wk19_ua02_behavior02_expired_point a
ON ckv.kokyano=a.customer_no
LEFT JOIN wk20_ua02_behavior02_outcall_flg b
ON ckv.kokyano=NVL(LPAD(b.customer_no,10,'0'),'0000000000')
LEFT JOIN wk21_ua02_behavior02_subscription_flg c
ON ckv.kokyano=c.customer_no
LEFT JOIN wk22_ua02_behavior02_ml_prediction_result d
ON ckv.kokyano=d.customer_no
),
final as (
select
kokyano::varchar(60) as kokyano,
expired_point_this_month::number(38,0) as expired_point_this_month,
expired_point_next_month::number(38,0) as expired_point_next_month,
outcall_exc_flg::varchar(1) as outcall_exc_flg,
outcall_hist_flg_3m::varchar(1) as outcall_hist_flg_3m,
subscription_flg::varchar(1) as subscription_flg,
ecpropensity::varchar(60) as ecpropensity,
ccpropensity::varchar(60) as ccpropensity,
acgelpropensity::varchar(60) as acgelpropensity,
vc100propensity::varchar(60) as vc100propensity,
cluster5_cd::number(18,0) as cluster5_cd,
cluster5_nm::varchar(300) as cluster5_nm
from transformed
)
select * from final