{% if build_month_end_job_models()  %}
with tbusrpram as (
select * from {{ ref('jpndclitg_integration__tbusrpram') }}
),
kr_054_allhist as (
select * from  {{ ref('jpndcledw_integration__kr_054_allhist') }}
),
kr_054_point_hist as (
select * from  {{ ref('jpndcledw_integration__kr_054_point_hist') }}
),
KR_054_CAL_V as (
select * from {{ ref('jpndcledw_integration__kr_054_cal_v') }}
),
KR_054_SFUYO_MEISAI as (
select * from {{this}}                  ---macro needs to be added at start
),
transformed as (
 SELECT
       PH.DIECUSRID as DIECUSRID,
       SUBSTRING(PH.P_DATE,1,6) as POINT_YM,
       SUBSTRING(PH.P_DATE,1,4) as POINT_YY,
       SUBSTRING(PH.P_DATE,5,2) as POINT_MM,
       PH.DIPOINT as POINT,
       PH.POINT_ID as POINT_ID,
       HIS.C_DSPOINTLIMITDATE as POINTLIMITDATE,
       PH.HIST_ID as HIST_ID,
       PH.DIORDERID as ORDERID,
       HIS.DSREN as DSREN
 FROM
       KR_054_POINT_HIST PH
 INNER JOIN 
    KR_054_ALLHIST HIS
  ON
       PH.HIST_ID=HIS.DIPOINTHISTID
 INNER JOIN 
    TBUSRPRAM USR
  ON
       PH.DIECUSRID=USR.DIUSRID
 WHERE   
       1=1 AND  
       PH.DIPOINT>0 AND  
	   PH.DIVNAME_MAE      = 'ステージアップ' AND
       -- PH.DIVNAME_MAE='ステージアップボーナスポイント' AND
		PH.DIREGISTDIVCODE  =  '20005' AND
       -- PH.DIREGISTDIVCODE='50006' AND  
       HIS.DIELIMFLG='0' AND  
       HIS.DIPOINTCODE='1' AND  
       HIS.DIVALIDFLG='1' AND  
       NVL2(HIS.DSPOINTMEMO,HIS.DSPOINTMEMO,'0')<>'移行ポイント' AND  
       SUBSTRING(PH.P_DATE,1,4)>='2020' AND  
       USR.DIELIMFLG='0' AND  
       USR.DISECESSIONFLG='0' AND  
       USR.DSDAT93='通常ユーザ'
 ),
final as (
select
diecusrid::number(38,0) as diecusrid,
point_ym::varchar(9) as point_ym,
point_yy::varchar(6) as point_yy,
point_mm::varchar(3) as point_mm,
point::number(38,0) as point,
point_id::number(38,0) as point_id,
pointlimitdate::varchar(12) as pointlimitdate,
hist_id::number(38,0) as hist_id,
orderid::number(38,0) as orderid,
dsren::timestamp_ntz(9) as dsren
from transformed
)
select * from final
{% else %}
    select * from {{this}}
{% endif %}