with KR_054_STOTAL_POINT as (
select * from {{ ref('jpndcledw_integration__kr_054_stotal_point') }}
),
KR_054_SFUYO_MEISAI as (
select * from {{ ref('jpndcledw_integration__kr_054_sfuyo_meisai') }}
),
KR_COMM_POINT_PARA as (
select * from {{ source('jpdcledw_integration', 'kr_comm_point_para') }}
),
TBUSRPRAM as (
select * from {{ ref('jpndclitg_integration__tbusrpram') }}
),
transformed as (
SELECT
       SOUTEIMAXP.DIECUSRID,
       SUBSTRING((select Term_End from KR_COMM_POINT_PARA),1,6) as POINT_YM,
       SUBSTRING((select Term_End from KR_COMM_POINT_PARA),1,4) as POINT_YY,
       SUBSTRING((select Term_End from KR_COMM_POINT_PARA),5,2) as POINT_MM,
       NVL(FUYOZUMI.FUYOZUMIP,0) as fuyozumi_point,
       (NVL(SOUTEIMAXP.TOTALP,0)-NVL(FUYOZUMI.FUYOZUMIP,0)) as FUYOYOTEI_POINT,
       SOUTEIMAXP.KONYU_KINGAKU
 FROM
 (SELECT
       DIECUSRID,
       SUM(POINT) as TOTALP,
       SUM(THISTOTALPRC) as KONYU_KINGAKU
 FROM
      KR_054_STOTAL_POINT
 GROUP BY
       DIECUSRID) SOUTEIMAXP
 LEFT JOIN 
 (SELECT
       DIECUSRID,
       SUM(POINT) as FUYOZUMIP
 FROM
      KR_054_SFUYO_MEISAI
-- STARTADD*20220217 
			--WHERE POINT_YM BETWEEN '202202' AND '202301'
			WHERE POINT_YM BETWEEN 
			(case when (select substring(term_end,7,2) from KR_COMM_POINT_PARA) < 15 then 
			(select to_char(to_date(add_months(to_date(substring(term_end,1,6), 'YYYYMM'), -1)),'YYYYMM') from KR_COMM_POINT_PARA)  else
			(select substring(term_end,1,6) from KR_COMM_POINT_PARA)  end)
			AND
			(to_char(add_months((case when (select substring(term_end,7,2) from KR_COMM_POINT_PARA) < 15 then 
			(select to_date(add_months(to_date(substring(term_end,1,6), 'YYYYMM'), -1)) from KR_COMM_POINT_PARA)  else
			(select  to_date(to_date(substring(term_end,1,6), 'YYYYMM')) from KR_COMM_POINT_PARA)  end),11),'YYYYMM'))
-- ENDADD*20220217
 GROUP BY
       DIECUSRID) FUYOZUMI
  ON
       FUYOZUMI.DIECUSRID=SOUTEIMAXP.DIECUSRID
 INNER JOIN 
    TBUSRPRAM USR
  ON
       SOUTEIMAXP.DIECUSRID=USR.DIUSRID
 WHERE   
       USR.DSDAT12<>'ブラック' AND  
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
fuyozumi_point::number(38,0) as fuyozumi_point,
fuyoyotei_point::number(38,0) as fuyoyotei_point,
konyu_kingaku::number(38,0) as konyu_kingaku
from transformed
)
select * from final