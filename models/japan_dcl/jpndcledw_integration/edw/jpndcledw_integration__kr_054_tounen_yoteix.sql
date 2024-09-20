{% if build_month_end_job_models()  %}
with C_TBECRANKSUMAMOUNT as(
    select * from {{ ref('jpndclitg_integration__c_tbecranksumamount') }}
),
KR_COMM_POINT_PARA as (
    select * from {{ source('jpdcledw_integration', 'kr_comm_point_para') }}
),
TBUSRPRAM as (
    select * from {{ ref('jpndclitg_integration__tbusrpram') }}
),
C_TBECRANKADDAMOUNTADM as (
    select * from  {{ ref('jpndclitg_integration__c_tbecrankaddamountadm') }}
),
TBECORDER as (
    select * from {{ ref('jpndclitg_integration__tbecorder') }}
),
KR_054_ALLADM as (
    select * from {{ ref('jpndcledw_integration__kr_054_alladm') }}
),
union_1 as (
    SELECT
        DIECUSRID                    AS USRID
        ,TO_CHAR(DSORDERDT,'YYYYMM') AS RANKDT
        ,SUM(C_DSRANKADDPRC)         AS PRC
      FROM
        C_TBECRANKADDAMOUNTADM
      WHERE
        DIELIMFLG = '0'
        AND TO_CHAR(DSORDERDT,'YYYYMM') BETWEEN (select Last_YearYm from KR_COMM_POINT_PARA) AND (select This_YearYm from KR_COMM_POINT_PARA)
      GROUP BY
        DIECUSRID
        ,TO_CHAR(DSORDERDT,'YYYYMM')
),
union_2 as (
    SELECT
        DIECUSRID                             AS USRID
        ,TO_CHAR(C_DSORDERREFERDATE,'YYYYMM') AS RANKDT
        ,SUM(DISEIKYUPRC - DIHAISOPRC - C_DICOLLECTPRC - C_DITOUJITSUHAISOPRC) AS PRC
      FROM
        TBECORDER
      WHERE
        DIELIMFLG = '0'
        AND DICANCEL = '0'
        AND DIROUTEID = '10'
        AND TO_CHAR(C_DSORDERREFERDATE,'YYYYMM') BETWEEN (select Last_YearYm from KR_COMM_POINT_PARA) AND (select This_YearYm from KR_COMM_POINT_PARA)
      GROUP BY
        DIECUSRID
        ,TO_CHAR(C_DSORDERREFERDATE,'YYYYMM') 
),
final as(

 SELECT
   SUB.USRID::number(38,18) AS KOKYANO
  ,CASE 
    WHEN SUB.LASTTOTALPRC >= 1     AND SUB.LASTTOTALPRC < 30000  THEN 600
    WHEN SUB.LASTTOTALPRC >= 30000 AND SUB.LASTTOTALPRC < 60000  THEN TRUNC(LASTTOTALPRC * 0.03)
    WHEN SUB.LASTTOTALPRC >= 60000 AND SUB.LASTTOTALPRC < 150000 THEN TRUNC(LASTTOTALPRC * 0.05)
    WHEN SUB.LASTTOTALPRC >= 150000  THEN TRUNC(LASTTOTALPRC * 0.08)
   -- WHEN SUB.LASTTOTALPRC = 0      AND SUB.THISTOTALPRC >= 1  AND SUBSTRING(USR.DSBIRTHDAY,6,2) IN ('06','07')  THEN 600  
    --WHEN SUB.LASTTOTALPRC = 0      AND SUB.THISTOTALPRC >= 1  THEN 0 
		 WHEN SUB.LASTTOTALPRC = 0     THEN 0 
   END :: number(38,18)     AS POINT
 FROM
 (
  SELECT
    A.USRID
    ,SUM(CASE WHEN A.RANKDT LIKE (select Last_Year from KR_COMM_POINT_PARA) THEN A.PRC ELSE 0 END) AS LASTTOTALPRC
    ,SUM(CASE WHEN A.RANKDT LIKE (select This_Year from KR_COMM_POINT_PARA) THEN A.PRC ELSE 0 END) AS THISTOTALPRC
    ,SUM(A.PRC) AS TOTALPRC
  FROM (
      SELECT
        DIECUSRID                AS USRID
        ,C_DSAGGREGATEYM         AS RANKDT
        ,C_DSRANKTOTALPRCBYMONTH AS PRC
      FROM
        C_TBECRANKSUMAMOUNT
      WHERE
        DIELIMFLG = '0'
        AND C_DSAGGREGATEYM BETWEEN (select Last_YearYm from KR_COMM_POINT_PARA) AND (select This_YearYm from KR_COMM_POINT_PARA)
    UNION ALL
      select * from union_1 
    UNION ALL
      select * from union_2
    ) A
  WHERE
    EXISTS (
      SELECT
        'X'
      FROM
        TBUSRPRAM B
      WHERE
            B.DIELIMFLG = '0'
        AND A.USRID = B.DIUSRID
        AND B.DSBIRTHDAY NOT LIKE '1600%'
        AND B.DISECESSIONFLG = '0'
        AND B.DSDAT93 = '通常ユーザ'
        AND B.DSDAT12 <> 'ブラック'
    )
  AND NOT EXISTS (
      SELECT 
        'X'
      FROM
        KR_054_ALLADM X                            
      WHERE
        X.DIELIMFLG = '0'
        AND A.USRID = X.DIECUSRID
        AND X.DIREGISTDIVCODE = '10104'
 -- STARTMOD*20220217
		--AND TO_CHAR(X.DSPOINTREN,'YYYY') = (select Target_Year from EDW_SCHEMA.KR_COMM_POINT_PARA)
        AND TO_CHAR(X.DSPOINTREN,'YYYY') = (select BD_Target_Year from KR_COMM_POINT_PARA)
 -- ENDMOD*20220217
  )
  GROUP BY
    A.USRID
  HAVING
    SUM(A.PRC) >= 1
 ) SUB 
LEFT JOIN TBUSRPRAM USR
       ON SUB.USRID = USR.DIUSRID)


select * from final
{% else %}
    select * from {{this}}
{% endif %}