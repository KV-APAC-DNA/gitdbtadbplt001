WITH C_TBECRANKSUMAMOUNT
AS (
  SELECT *
  FROM {{ ref('jpndclitg_integration__c_tbecranksumamount') }}
  ),
C_TBECRANKADDAMOUNTADM
AS (
  SELECT *
  FROM {{ ref('jpndclitg_integration__c_tbecrankaddamountadm') }}
  ),
kr_comm_point_para
AS (
  SELECT *
  FROM {{ source('jpdcledw_integration', 'kr_comm_point_para') }}
  ),
TBECORDER
AS (
  SELECT *
  FROM {{ ref('jpndclitg_integration__tbecorder') }}
  ),
TBUSRPRAM
AS (
  SELECT *
  FROM {{ ref('jpndclitg_integration__tbusrpram') }}
  ),
cte1
AS (
  SELECT DIECUSRID AS USRID,
    C_DSAGGREGATEYM AS RANKDT,
    C_DSRANKTOTALPRCBYMONTH AS PRC
  FROM C_TBECRANKSUMAMOUNT
  WHERE DIELIMFLG = '0'
    AND TO_CHAR(TO_DATE(C_DSAGGREGATEYM, 'YYYYMM'), 'YYYY') = (
      SELECT Target_Year
      FROM kr_comm_point_para
      )
  ),
cte2
AS (
  SELECT DIECUSRID AS USRID,
    TO_CHAR(DSORDERDT, 'YYYYMM') AS RANKDT,
    SUM(C_DSRANKADDPRC) AS PRC
  FROM C_TBECRANKADDAMOUNTADM
  WHERE DIELIMFLG = '0'
    AND TO_CHAR(DSORDERDT, 'YYYY') = (
      SELECT Target_Year
      FROM kr_comm_point_para
      )
  GROUP BY DIECUSRID,
    TO_CHAR(DSORDERDT, 'YYYYMM')
  ),
cte3
AS (
  SELECT DIECUSRID AS USRID,
    TO_CHAR(C_DSORDERREFERDATE, 'YYYYMM') AS RANKDT,
    SUM(DISEIKYUPRC - DIHAISOPRC - C_DICOLLECTPRC - C_DITOUJITSUHAISOPRC) AS PRC
  FROM TBECORDER
  WHERE DIELIMFLG = '0'
    AND DICANCEL = '0'
    AND DIROUTEID = '10'
    AND TO_CHAR(C_DSORDERREFERDATE, 'YYYY') = (
      SELECT Target_Year
      FROM kr_comm_point_para
      )
  GROUP BY DIECUSRID,
    TO_CHAR(C_DSORDERREFERDATE, 'YYYYMM')
  )
SELECT A.USRID,
  SUM(CASE 
      WHEN A.RANKDT LIKE (
          SELECT This_Year
          FROM KR_COMM_POINT_PARA
          )
        THEN A.PRC
      ELSE 0
      END) AS LASTTOTALPRC,
  SUM(CASE 
      WHEN A.RANKDT LIKE (
          SELECT Next_Year
          FROM KR_COMM_POINT_PARA
          )
        THEN A.PRC
      ELSE 0
      END) AS THISTOTALPRC,
  SUM(A.PRC) AS TOTALPRC
FROM (
  SELECT *
  FROM cte1
  --   SELECT
  --     DIECUSRID                AS USRID
  --     ,C_DSAGGREGATEYM         AS RANKDT
  --     ,C_DSRANKTOTALPRCBYMONTH AS PRC
  --   FROM
  --     ITG_SCHEMA.C_TBECRANKSUMAMOUNT
  --   WHERE
  --     DIELIMFLG = '0'
  --     AND TO_CHAR(TO_DATE(C_DSAGGREGATEYM,'YYYYMM'),'YYYY') = (select Target_Year from EDW_SCHEMA.kr_comm_point_para) 
  
  UNION ALL
  
  SELECT *
  FROM cte2
  --   SELECT
  --     DIECUSRID                    AS USRID
  --     ,TO_CHAR(DSORDERDT,'YYYYMM') AS RANKDT
  --     ,SUM(C_DSRANKADDPRC)         AS PRC
  --   FROM
  --     ITG_SCHEMA.C_TBECRANKADDAMOUNTADM
  --   WHERE
  --     DIELIMFLG = '0'
  --     AND TO_CHAR(DSORDERDT,'YYYY') = (select Target_Year from EDW_SCHEMA.kr_comm_point_para) 
  --   GROUP BY
  --     DIECUSRID
  --     ,TO_CHAR(DSORDERDT,'YYYYMM') 
  
  UNION ALL
  
  SELECT *
  FROM cte3
  ) A
WHERE EXISTS (
    SELECT 'X'
    FROM TBUSRPRAM B
    WHERE B.DIELIMFLG = '0'
      AND A.USRID = B.DIUSRID
      AND B.DSBIRTHDAY NOT LIKE '1600%'
      AND B.DISECESSIONFLG = '0'
      AND B.DSDAT93 = '通常ユーザ' -- テストユーザ除外
      -- 要検討（ZZ03付与、ZZ07失効予定）条件合わせる
      AND B.DSDAT12 <> 'ブラック'
    )
GROUP BY A.USRID
HAVING SUM(A.PRC) >= 1
