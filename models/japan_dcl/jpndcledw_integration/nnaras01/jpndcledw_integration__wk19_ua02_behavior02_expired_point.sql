with cim01kokya as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIM01KOKYA
),
tbecpointhistory as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.TBECPOINTHISTORY
),
tbusrpram as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.TBUSRPRAM
),
this_month AS (
  SELECT 
    phi.diecusrid, 
    SUBSTRING(phi.c_dspointlimitdate, 1, 6) AS c_dspointlimitdate, 
    SUM(phi.dipoint) AS expired_point_this_month 
  FROM 
    tbecpointhistory phi 
    INNER JOIN tbusrpram urs ON phi.diecusrid = urs.diusrid 
  WHERE 
    1 = 1 
    AND SUBSTRING(phi.c_dspointlimitdate, 1, 6) IN (
      TO_CHAR(convert_timezone('UTC',current_timestamp()), 'yyyymm')
    ) --this_month
    AND phi.dipoint <> 0 
    AND phi.divalidflg = 1 
    AND phi.dielimflg = 0 
    AND urs.disecessionflg = '0' 
    AND urs.dielimflg = '0' 
  GROUP BY 
    phi.diecusrid, 
    SUBSTRING(phi.c_dspointlimitdate, 1, 6) 
  HAVING 
    SUM(phi.dipoint) <> 0
), 
next_month as (
  SELECT 
    phi.diecusrid, 
    SUBSTRING(phi.c_dspointlimitdate, 1, 6) AS c_dspointlimitdate, 
    SUM(phi.dipoint) AS expired_point_next_month 
  FROM 
    tbecpointhistory phi 
    INNER JOIN tbusrpram urs ON phi.diecusrid = urs.diusrid 
  WHERE 
    1 = 1 
    AND SUBSTRING(phi.c_dspointlimitdate, 1, 6) IN (
      TO_CHAR(
        ADD_MONTHS(convert_timezone('UTC',current_timestamp()), 1), 
        'yyyymm'
      )
    ) --next_month
    AND phi.dipoint <> 0 
    AND phi.divalidflg = 1 
    AND phi.dielimflg = 0 
    AND urs.disecessionflg = '0' 
    AND urs.dielimflg = '0' 
  GROUP BY 
    phi.diecusrid, 
    SUBSTRING(phi.c_dspointlimitdate, 1, 6) 
  HAVING 
    SUM(phi.dipoint) <> 0
),
final as (
SELECT 
  ck.kokyano::varchar(68) AS customer_no, 
  this_month.expired_point_this_month::number(38,0) AS expired_point_this_month, 
  next_month.expired_point_next_month::number(38,0) AS expired_point_next_month 
FROM 
  cim01kokya ck 
  LEFT JOIN this_month on ck.kokyano = NVL(
    LPAD(this_month.diecusrid, 10, '0'), 
    '0000000000'
  ) 
  LEFT JOIN next_month on ck.kokyano = NVL(
    LPAD(next_month.diecusrid, 10, '0'), 
    '0000000000'
  ) 
WHERE 
  ck.testusrflg = '通常ユーザ'
)
select * from final