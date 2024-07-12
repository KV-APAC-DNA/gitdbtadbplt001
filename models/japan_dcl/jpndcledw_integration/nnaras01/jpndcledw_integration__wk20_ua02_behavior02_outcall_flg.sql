
with tbusrpram as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.TBUSRPRAM
) ,
tbEcOrder as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.TBECORDER
),
tbEcSalesRouteMst as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.TBECSALESROUTEMST
),
c_tbEcInquire as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.C_TBECINQUIRE
),
C_TBECUSRCOMMENT as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.C_TBECUSRCOMMENT
),
C_TBDMSNDHIST as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.C_TBDMSNDHIST
),
c_tbeckesai as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.C_TBECKESAI
),
conditions AS (
  SELECT 
    diusrid, 
    CASE WHEN dsdat10 <> '通常' 
    OR dsdat11 <> '正常' 
    OR dsdat12 <> '正常' THEN '1' ELSE '0' END AS excflg_1, 
    CASE dsdat4 WHEN '固定' THEN dstel WHEN '携帯' THEN dsdat2 ELSE dsdat3 END AS maintel, 
    CASE WHEN maintel NOT LIKE '01%' 
    AND maintel NOT LIKE '02%' 
    AND maintel NOT LIKE '03%' 
    AND maintel NOT LIKE '04%' 
    AND maintel NOT LIKE '05%' 
    AND maintel NOT LIKE '06%' 
    AND maintel NOT LIKE '07%' 
    AND maintel NOT LIKE '08%' 
    AND maintel NOT LIKE '09%' THEN '2' ELSE '0' END AS excflg_2, 
    CASE WHEN dsdat62 = '拒否' THEN '4' ELSE '0' END AS excflg_4, 
    CASE WHEN disecessionflg = '1' 
    OR dielimflg = '1' THEN '5' ELSE '0' END AS excflg_5, 
    CASE WHEN dszip IS NULL THEN '6' ELSE '0' END AS excflg_6, 
    CASE WHEN maintel LIKE '010%' 
    OR maintel LIKE '020%' 
    OR maintel LIKE '030%' 
    OR maintel LIKE '040%' 
    OR maintel LIKE '060%' 
    OR maintel LIKE '0120%' 
    OR maintel LIKE '0130%' 
    OR maintel LIKE '0140%' 
    OR maintel LIKE '0150%' 
    OR maintel LIKE '0160%' 
    OR maintel LIKE '0170%' 
    OR maintel LIKE '0180%' 
    OR maintel LIKE '0190%' 
    OR maintel LIKE '0450%' 
    OR maintel LIKE '0570%' 
    OR maintel LIKE '0750%' 
    OR maintel LIKE '0800%' 
    OR maintel LIKE '0910%' 
    OR maintel LIKE '0990%' 
    OR maintel LIKE '010800%' THEN '9' ELSE '0' END AS excflg_9, 
        TO_DATE (dsbirthday) AS birthday, 
    TRUNC(
      MONTHS_BETWEEN(convert_timezone('UTC',current_timestamp()), birthday)/ 12
    ) AS age, 
    CASE WHEN age < 20 THEN '13' ELSE '0' END AS excflg_13 
  FROM 
    tbusrpram
) --update 7
, 
condition_7 AS (
  with diinquire as (
    select 
      diUsrId, 
      diinquireid 
    from 
      (
        SELECT 
          "抽出結果".diUsrId, 
          "受注情報".diOrderID, 
          "販売経路".dsroutename, 
          MAX("返品交換".diinquireid) AS diinquireid 
        FROM 
          tbusrpram "抽出結果" 
          INNER JOIN tbEcOrder "受注情報" ON "抽出結果".diUsrId = "受注情報".diEcUsrID 
          INNER JOIN (
            SELECT 
              "抽出結果".diUsrId, 
              MAX(diOrderID) AS MAX_orderID 
            FROM 
              tbusrpram "抽出結果" 
              INNER JOIN tbEcOrder "受注情報" ON "抽出結果".diUsrId = "受注情報".diEcUsrID 
            WHERE 
              "受注情報".diCancel = '0' 
              AND "受注情報".dielimflg = '0' 
            GROUP BY 
              "抽出結果".diUsrId
          )"最新受注" ON "受注情報".diEcUsrID ="最新受注".diUsrId 
          AND "受注情報".diOrderID ="最新受注".MAX_orderID 
          LEFT OUTER JOIN tbEcSalesRouteMst "販売経路" ON "受注情報".dirouteid = "販売経路".dirouteid 
          LEFT OUTER JOIN c_tbEcInquire "返品交換" ON "受注情報".diOrderID = "返品交換".diorderid 
          AND "返品交換".diElimFlg = '0' 
        GROUP BY 
          "抽出結果".diUsrId, 
          "受注情報".diOrderID, 
          "販売経路".dsroutename
      )
  ) 
  SELECT 
    a.diusrid, 
    CASE WHEN diinquireid IS NOT NULL THEN '7' ELSE '0' END AS excflg_7 
  FROM 
    tbusrpram a 
    LEFT JOIN diinquire b ON a.diusrid = b.diUsrId
) , 
condition_8 AS (
  WITH last_year AS (
    SELECT 
      a.diusrid 
    FROM 
      tbusrpram a 
      INNER JOIN tbecorder b ON a.diusrid = b.diecusrid 
    WHERE 
      b.dirouteid = '6' 
      AND TO_CHAR(b.dsorderdt, 'YYYYMMDD') >= TO_CHAR(
        ADD_MONTHS(convert_timezone('UTC',current_timestamp()), -12), 
        'YYYYMMDD'
      ) 
      AND b.dicancel = '0' 
      AND b.dielimflg = '0'
  ) 
  SELECT 
    DISTINCT up.diusrid, 
    CASE WHEN up.diusrid = last_year.diusrid THEN '8' ELSE '0' END AS excflg_8 
  FROM 
    tbusrpram up 
    LEFT JOIN last_year ON up.diusrid = last_year.diusrid
) , 
invalid_phone_number AS (
    SELECT 
      "会員基本情報".diUsrID 
    FROM 
      tbusrpram "会員基本情報" 
      INNER JOIN C_TBECUSRCOMMENT "コメント" ON "会員基本情報".diusrid = "コメント".diEcUsrID 
      INNER JOIN (
        SELECT 
          diEcUsrID AS diUsrID, 
          MAX(
            c_dsUsrCommentDate || '_' || c_diUsrCommentId
          ) AS max_diUsrCommentID 
        FROM 
          C_TBECUSRCOMMENT 
        WHERE 
          c_dsUsrCommentClassKbn = '80' 
          AND dielimflg = '0' 
        GROUP BY 
          diEcUsrID
      ) "最新電話状況" ON "会員基本情報".diUsrID = "最新電話状況".diUsrID 
      AND "コメント".c_diUsrCommentId = SUBSTRING(
        "最新電話状況".max_diUsrCommentID, 
        position( 
          '_',"最新電話状況".max_diUsrCommentID,1
        )+ 1, 
        LENGTH(
          "最新電話状況".max_diUsrCommentID
        )
      ) 
    WHERE 
      "コメント".c_dsUsrComment LIKE '%無効%' 
      AND "会員基本情報".dielimflg = '0'
  ) ,
  condition_10 as (
  SELECT 
    DISTINCT up.diusrid, 
    CASE WHEN up.diusrid = invalid_phone_number.diusrid THEN '10' ELSE '0' END AS excflg_10 
  FROM 
    tbusrpram up 
    LEFT JOIN invalid_phone_number ON up.diusrid = invalid_phone_number.diusrid
) , 
condition_15 AS (
  WITH outcall_3month AS (
    SELECT 
      DISTINCT up3.diUsrId, 
      up3.dsTel, 
      up3.dsDat2, 
      up3.dsDat3 
    FROM 
      C_TBDMSNDHIST ds 
      INNER JOIN TBUSRPRAM up3 ON up3.diUsrId = ds.c_diUsrId 
    WHERE 
      ds.c_dsdmsendkubun = '4' 
      AND ds.diElimFlg = '0' 
      AND TO_CHAR(ds.c_dsDmSendDate, 'YYYYMMDD') >= TO_CHAR(
        ADD_MONTHS(convert_timezone('UTC',current_timestamp()), -3), 
        'YYYYMMDD'
      ) 
      AND up3.diSecessionFlg = '0' 
      AND up3.diElimFlg = '0'
  ) 
  SELECT 
    DISTINCT a.diusrid, 
    CASE WHEN(
      a.dsTel IS NOT NULL 
      AND a.dsTel NOT LIKE '000%' 
      AND a.dsTel = outcall_3month.dsTel
    ) 
    OR (
      a.dsdat2 IS NOT NULL 
      AND a.dsDat2 NOT LIKE '000%' 
      AND a.dsDat2 = outcall_3month.dsDat2
    ) 
    OR (
      a.dsdat3 IS NOT NULL 
      AND a.dsDat3 NOT LIKE '000%' 
      AND a.dsDat3 = outcall_3month.dsDat3
    ) THEN '15' ELSE '0' END AS excflg_15 
  FROM 
    tbusrpram a 
    LEFT JOIN outcall_3month ON a.diusrid = outcall_3month.diusrid
), 
condition_18 AS (
  WITH unpaid_amt AS (
    SELECT 
      up.diusrid 
    FROM 
      tbusrpram up 
      LEFT JOIN (
        SELECT 
          kesai.diecusrid, 
          kesai.diseikyuremain 
        FROM 
          c_tbeckesai kesai 
        WHERE 
          kesai.dishukkasts = '1060' --出荷ステータス:出荷済
          AND kesai.diseikyuremain > 0 --請求残合計
          AND TO_CHAR(kesai.dsuriagedt, 'YYYYMMDD') >= TO_CHAR(
            ADD_MONTHS(convert_timezone('UTC',current_timestamp()),-1), 
            'YYYYMMDD'
          ) --出荷日
          AND NOT(
            kesai.dinyukinsts = '1' 
            AND dicardnyukinsts = '1'
          ) --入金ステータス、入金照合ステータス
          AND dicancel = '0' --キャンセルフラグ
          AND dielimflg = '0' --削除flg
          AND diecusrid <> 0 --顧客no
          ) kesai ON kesai.diecusrid = up.diusrid 
    WHERE 
      up.dielimflg = '0' 
      AND up.disecessionflg = '0' 
      AND (
        CAST(up.dsdat40 AS integer) - NVL(kesai.diseikyuremain, 0)
      ) > 0
  ) 
  SELECT 
    DISTINCT a.diusrid, 
    CASE WHEN a.diusrid = unpaid_amt.diusrid THEN '18' ELSE '0' END AS excflg_18 
  FROM 
    tbusrpram a 
    LEFT JOIN unpaid_amt ON a.diusrid = unpaid_amt.diusrid
) ,
transformed as (
SELECT 
  a.diusrid AS customer_no, 
  CASE WHEN conditions.excflg_1 = '1' 
  OR conditions.excflg_2 = '2' 
  OR conditions.excflg_4 = '4' 
  OR conditions.excflg_5 = '5' 
  OR conditions.excflg_6 = '6' 
  OR condition_7.excflg_7 = '7' 
  OR condition_8.excflg_8 = '8' 
  OR conditions.excflg_9 = '9' 
  OR condition_10.excflg_10 = '10' 
  OR conditions.excflg_13 = '13' 
  OR condition_18.excflg_18 = '18' THEN '1' ELSE '0' END AS outcall_exc_flg, 
  CASE WHEN condition_15.excflg_15 = '15' THEN '1' ELSE '0' END AS outcall_hist_flg_3m 
FROM 
  tbusrpram a 
  LEFT JOIN conditions ON a.diusrid = conditions.diusrid 
  LEFT JOIN condition_7 ON a.diusrid = condition_7.diusrid 
  LEFT JOIN condition_8 ON a.diusrid = condition_8.diusrid 
  LEFT JOIN condition_10 ON a.diusrid = condition_10.diusrid 
  LEFT JOIN condition_15 ON a.diusrid = condition_15.diusrid 
  LEFT JOIN condition_18 ON a.diusrid = condition_18.diusrid
),
final as (
select 
customer_no::varchar(68) as customer_no,
outcall_exc_flg::varchar(1) as outcall_exc_flg,
outcall_hist_flg_3m::varchar(1) as outcall_hist_flg_3m
from transformed
)
select * from final 