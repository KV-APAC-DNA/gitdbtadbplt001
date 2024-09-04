{% if build_month_end_job_models()  %}
with kr_054_sfuyo_meisai as (
select * from {{ ref('jpndcledw_integration__kr_054_sfuyo_meisai') }}
),
kr_054_sfuyo_meisai as (
select * from  {{ ref('jpndcledw_integration__kr_054_sfuyo_meisai') }}
),
kr_054_allhist as (
select * from {{ ref('jpndcledw_integration__kr_054_allhist') }}
),
tbecorder as (
select * from {{ ref('jpndclitg_integration__tbecorder') }}
),
kr_054_point_hist as (
select * from  {{ ref('jpndcledw_integration__kr_054_point_hist') }}
),
kr_comm_point_para as (
select * from {{ source('jpdcledw_integration', 'kr_comm_point_para') }}
),
tbusrpram as ( 
select * from {{ ref('jpndclitg_integration__tbusrpram') }}
),
transformed as (
SELECT
       PH.DIECUSRID as DIECUSRID,
       SUBSTRING(PH.P_DATE,1,6) as USE_YM,
       SUBSTRING(PH.P_DATE,1,4) as USE_YY,
       SUBSTRING(PH.P_DATE,5,2) as USE_MM,
       WK.POINT_YM as POINT_YM,
       WK.POINT_YY as POINT_YY,
       WK.POINT_MM as POINT_MM,
       PH.DIPOINT as UPOINT,
       PH.POINT_ID as UPOINT_ID,
       PH.HIST_ID as HIST_ID,
       HIS.DSREN as DSREN
 FROM
       KR_054_SFUYO_MEISAI WK
 INNER JOIN 
    KR_054_POINT_HIST PH
  ON
       PH.POINT_ID=WK.POINT_ID
 INNER JOIN 
   KR_054_ALLHIST HIS
  ON
       HIS.DIECUSRID=WK.DIECUSRID AND 
       HIS.C_DIPOINTISSUEID=WK.POINT_ID AND 
       HIS.DIPOINTHISTID=WK.HIST_ID
 LEFT JOIN 
    TBECORDER ODR
  ON
       ODR.DIORDERID=PH.DIORDERID AND 
       ODR.DICANCEL=0 AND 
       ODR.DIELIMFLG=0
 WHERE   
       1=1 AND 
	   PH.DIREGISTDIVCODE NOT IN ('10008','20005') AND
       --PH.DIREGISTDIVCODE NOT IN('10008','50006') AND  
	   PH.DIVNAME_MAE      = 'ステージアップ' AND
       --PH.DIVNAME_MAE='ステージアップボーナスポイント' AND  
       HIS.DIELIMFLG='0' AND  
       HIS.DIVALIDFLG<>0 AND  
       HIS.DIPOINTCODE=1 AND  
       NVL2(HIS.DSPOINTMEMO,HIS.DSPOINTMEMO,'0')<>'・移行ポイント' AND  
       WK.POINT_YY>='2020' AND  
       SUBSTRING(PH.P_DATE,1,4)>='2020' AND  
       PH.P_DATE<=(select Term_End from KR_COMM_POINT_PARA) AND  
      PH.DIECUSRID IN
 (SELECT
       USR.DIUSRID
 FROM
       TBUSRPRAM USR
 WHERE   
       1=1 AND  
       USR.DIELIMFLG='0' AND  
       USR.DISECESSIONFLG='0' AND  
       USR.DSDAT93='通常ユーザ')
),
final as (
select
diecusrid::number(38,0) as diecusrid,
use_ym::varchar(9) as use_ym,
use_yy::varchar(6) as use_yy,
use_mm::varchar(3) as use_mm,
point_ym::varchar(9) as point_ym,
point_yy::varchar(6) as point_yy,
point_mm::varchar(3) as point_mm,
upoint::number(38,0) as upoint,
upoint_id::number(38,0) as upoint_id,
hist_id::number(38,0) as hist_id,
dsren::timestamp_ntz(9) as dsren
from transformed
)
select * from final
{% else %}
    select * from {{this}}
{% endif %}