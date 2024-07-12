WITH c_tbecranksumamount
AS (
      SELECT *
      FROM DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.C_TBECRANKSUMAMOUNT
      ),
c_tbecrankaddamountadm
AS (
      SELECT *
      FROM DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.C_TBECRANKADDAMOUNTADM
      ),
tbusrpram
AS (
      SELECT *
      FROM DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.TBUSRPRAM
      ),
kr_this_stage_point_wk_rescue
AS (
      SELECT *
      FROM DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.KR_THIS_STAGE_POINT_WK_RESCUE
      ),
DCL_CALENDAR_SYSDATE
AS (
      SELECT *
      FROM DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.DCL_CALENDAR_SYSDATE
      ),
sum_pre_month
AS (
      SELECT amt.diecusrid AS usrid,
            amt.c_dsaggregateym AS rankdt,
            amt.c_dsranktotalprcbymonth AS prc
      FROM c_tbecranksumamount amt
      WHERE amt.dielimflg = '0'
      
      UNION ALL
      
      SELECT diecusrid AS usrid,
            to_char(dsorderdt, 'YYYYMM') AS rankdt,
            c_dsrankaddprc AS prc
      FROM c_tbecrankaddamountadm adda
      WHERE dielimflg = '0'
      
      UNION ALL
      
      SELECT usrid AS usrid,
            yyyymm AS rankdt,
            diff_amout AS prc
      FROM kr_this_stage_point_wk_rescue
      ),
ruikei
AS (
      SELECT to_char(add_months(cal.curr_date, - 1), 'yyyymm') yyyymm,
            sum_pre_month.usrid,
            sum(sum_pre_month.prc) AS thistotalprc
      FROM sum_pre_month
      JOIN dcl_calendar_sysdate cal ON cal.is_active = true
            AND 1 = 1
      WHERE sum_pre_month.rankdt BETWEEN CASE 
                              WHEN to_char(cal.curr_date, 'dd') <= 20
                                    THEN to_char(add_months(cal.curr_date, - 1), 'yyyy') || '01'
                              WHEN to_char(cal.curr_date, 'dd') > 20
                                    THEN to_char(cal.curr_date, 'yyyy') || '01'
                              END
                  AND CASE 
                              WHEN to_char(cal.curr_date, 'dd') <= 20
                                    THEN to_char(add_months(cal.curr_date, - 1), 'yyyymm')
                              END
      GROUP BY to_char(add_months(cal.curr_date, - 1), 'yyyymm'),
            sum_pre_month.usrid
      ),
transformed
AS (
      SELECT ruikei.yyyymm,
            ruikei.usrid,
            CASE 
                  WHEN ruikei.thistotalprc >= 80000
                        THEN 'ダイヤモンド'
                  WHEN ruikei.thistotalprc >= 50000
                        THEN 'プラチナ'
                  WHEN ruikei.thistotalprc >= 15000
                        THEN 'ゴールド'
                  ELSE 'レギュラー'
                  END AS tstage,
            CASE 
                  WHEN ruikei.thistotalprc >= 80000
                        THEN '04'
                  WHEN ruikei.thistotalprc >= 50000
                        THEN '03'
                  WHEN ruikei.thistotalprc >= 15000
                        THEN '02'
                  ELSE '01'
                  END AS tstage_cd,
            nvl(ruikei.thistotalprc, 0) AS thistotalprc,
            CASE 
                  WHEN ruikei.thistotalprc >= 80000
                        THEN 8500
                  WHEN ruikei.thistotalprc >= 50000
                        THEN 3500
                  WHEN ruikei.thistotalprc >= 15000
                        THEN 500
                  ELSE 0
                  END AS goalp,
            to_char(convert_timezone('UTC', 'Asia/Tokyo', current_timestamp()), 'YYYYMMDD HH24:MI:SS') insertdate
      FROM ruikei
      WHERE ruikei.usrid IN (
                  SELECT diusrid
                  FROM tbusrpram usr
                  WHERE usr.disecessionflg = 0
                  )
      ),
final
AS (
      SELECT yyyymm::VARCHAR(14) AS yyyymm,
            usrid::NUMBER(38, 0) AS usrid,
            tstage::VARCHAR(18) AS tstage,
            tstage_cd::VARCHAR(2) AS tstage_cd,
            thistotalprc::NUMBER(38, 0) AS thistotalprc,
            goalp::NUMBER(18, 0) AS goalp,
            insertdate::VARCHAR(27) AS insertdate
      FROM transformed
      )
SELECT *
FROM final
