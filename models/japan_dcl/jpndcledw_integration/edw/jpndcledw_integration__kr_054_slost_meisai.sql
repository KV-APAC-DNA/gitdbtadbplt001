with kr_054_sfuyo_meisai as (
select * from  {{ ref('jpndcledw_integration__kr_054_sfuyo_meisai') }}
),
kr_054_allhist as (
select * from {{ ref('jpndcledw_integration__kr_054_allhist') }}
),
kr_054_point_hist as (
select * from {{ ref('jpndcledw_integration__kr_054_point_hist') }}
),
tbusrpram as (
select * from {{ ref('jpndclitg_integration__tbusrpram') }}
),
KR_COMM_POINT_PARA as (
select * from {{ source('jpdcledw_integration', 'kr_comm_point_para') }}
),
transformed as (
SELECT
       PH.DIECUSRID as DIECUSRID,
       SUBSTRING(PH.P_DATE,1,6) as LOST_YM,
       SUBSTRING(PH.P_DATE,1,4) as LOST_YY,
       SUBSTRING(PH.P_DATE,5,2) as LOST_MM,
       PH.DIPOINT as LPOINT,
       PH.POINT_ID as LPOINT_ID,
       HIS.C_DSPOINTLIMITDATE as LPOINT_LIMIT,
       PH.HIST_ID as HIST_ID,
       HIS.DSREN as DSREN
 FROM
        KR_054_POINT_HIST PH
 INNER JOIN 
     KR_054_ALLHIST HIS
  ON
       PH.HIST_ID=HIS.DIPOINTHISTID
 INNER JOIN 
 (SELECT
 distinct
       DIECUSRID,
       POINT_ID
 FROM
       KR_054_SFUYO_MEISAI) WK
  ON
       HIS.DIECUSRID=WK.DIECUSRID AND 
       HIS.C_DIPOINTISSUEID=WK.POINT_ID
 WHERE   
       1=1 AND  
       PH.DIPOINT<0 AND 
	   PH.DIVNAME_MAE     = 'ステージアップ' AND
       --PH.DIVNAME_MAE='ステージアップボーナスポイント' AND     
       PH.DIREGISTDIVCODE='10008' AND  
       HIS.DIELIMFLG='0' AND  
       HIS.DIVALIDFLG='1' AND  
       SUBSTRING(PH.P_DATE,1,4)>='2020' AND  
	   TO_CHAR(HIS.DSREN,'YYYYMMDD') <= (select Term_End from KR_COMM_POINT_PARA) AND
      HIS.DIECUSRID IN
 (SELECT
       USR.DIUSRID
 FROM
       TBUSRPRAM USR
 WHERE   
       1=1 AND  
       USR.DIELIMFLG='0' AND  
       USR.DISECESSIONFLG='0' AND  
       USR.DSDAT93 = '通常ユーザ')
),
final as (
select
diecusrid::number(38,0) as diecusrid,
lost_ym::varchar(9) as lost_ym,
lost_yy::varchar(6) as lost_yy,
lost_mm::varchar(3) as lost_mm,
lpoint::number(38,0) as lpoint,
lpoint_id::number(38,0) as lpoint_id,
lpoint_limit::varchar(12) as lpoint_limit,
hist_id::number(38,0) as hist_id,
dsren::timestamp_ntz(9) as dsren
from transformed
)
select * from final