{% if build_month_end_job_models()  %}
with C_TBECRANKSUMAMOUNT as (
select * from {{ ref('jpndclitg_integration__c_tbecranksumamount') }}
),
KR_COMM_POINT_PARA as (
select * from {{ source('jpdcledw_integration', 'kr_comm_point_para') }}
),
C_TBECRANKADDAMOUNTADM as (
select * from {{ ref('jpndclitg_integration__c_tbecrankaddamountadm') }}
),
transformed as (
 SELECT
       SUB.DIECUSRID,
       SUB.THISTOTALPRC,
-- STARTMOD*20220217
       CASE --WHEN STP.STAGE = 'ダイヤモンド' THEN 'ダイヤモンド'
	   WHEN SUB.THISTOTALPRC>=80000 THEN 'ダイヤモンド' 
	   WHEN SUB.THISTOTALPRC>=50000 THEN 'プラチナ' 
	   WHEN SUB.THISTOTALPRC>=15000 THEN 'ゴールド' 
	   ELSE 'レギュラー' END as STAGE,
       CASE  --WHEN STP.STAGE = 'ダイヤモンド' THEN 8500
	   WHEN SUB.THISTOTALPRC>=80000 THEN 8500 
	   WHEN SUB.THISTOTALPRC>=50000 THEN 3500 
	   WHEN SUB.THISTOTALPRC>=15000 THEN 500 ELSE 0 END as POINT
-- ENDMOD*20220217
 FROM(
 SELECT
       RUIKEI.DIECUSRID,
       SUM(RUIKEI.PRC) as THISTOTALPRC
 FROM
 (SELECT
       DIECUSRID as DIECUSRID,
       C_DSAGGREGATEYM as RANKDT,
       C_DSRANKTOTALPRCBYMONTH as PRC
 FROM
       C_TBECRANKSUMAMOUNT
 WHERE   
       DIELIMFLG='0' AND  
       C_DSAGGREGATEYM BETWEEN (select Target_Year from KR_COMM_POINT_PARA)||'01' AND  
       SUBSTRING((select Term_End from KR_COMM_POINT_PARA),1,6)
 UNION ALL
 SELECT
       DIECUSRID as USRID,
       TO_CHAR(DSORDERDT,'YYYYMM') as RANKDT,
       SUM(C_DSRANKADDPRC) as PRC
 FROM
       C_TBECRANKADDAMOUNTADM ADDA
 WHERE   
       DIELIMFLG='0' AND  
       TO_CHAR(DSORDERDT,'YYYYMM')BETWEEN (select Target_Year from KR_COMM_POINT_PARA)||'01' AND  
       SUBSTRING((select Term_End from KR_COMM_POINT_PARA),1,6)
 GROUP BY
       DIECUSRID,
       TO_CHAR(DSORDERDT,'YYYYMM')) RUIKEI
 GROUP BY
       RUIKEI.DIECUSRID) SUB
-- STARTDEL*20220217 
	    --LEFT JOIN KR_NEW_STAGE_POINT  STP
        --   ON STP.USRID = SUB.DIECUSRID
	--Modification for correction 20210114 Start
    --WHERE STP.YYYYMM = (select  substring(term_end,1,6) from KR_COMM_POINT_PARA)-1
	--WHERE STP.YYYYMM = case when (select substring(term_end,7,2) from KR_COMM_POINT_PARA) < 15 then 
    --(select to_char(trunc(add_months(to_date(substring(term_end,1,6), 'YYYYMM'), -2)),'YYYYMM') from KR_COMM_POINT_PARA)  else
    --(select  to_char(trunc(add_months(to_date(substring(term_end,1,6), 'YYYYMM'), -1)),'YYYYMM') from KR_COMM_POINT_PARA)  end
	--Modification for correction 20210114 End
-- ENDDEL*20220217
),
final as (
select
diecusrid::number(38,0) as diecusrid,
thistotalprc::number(38,18) as thistotalprc,
stage::varchar(18) as stage,
point::number(38,18) as point
from transformed
)
select * from final
{% else %}
    select * from {{this}}
{% endif %}