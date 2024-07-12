WITH kr_this_stage_point_wk_curr
AS (
    SELECT *
    FROM dev_dna_core.snapjpdcledw_integration.kr_this_stage_point_wk_curr
    ),
kr_last_stage_point
AS (
    SELECT *
    FROM dev_dna_core.snapjpdcledw_integration.kr_last_stage_point
    ),
kr_this_stage_point_daily_prv
AS (
    SELECT *
    FROM dev_dna_core.snapjpdcledw_integration.kr_this_stage_point_daily_prv
    ),
kr_this_point_granted
AS (
    SELECT *
    FROM dev_dna_core.snapjpdcledw_integration.kr_this_point_granted
    ),
kr_this_stage_point_wk_rescue
AS (
    SELECT *
    FROM dev_dna_core.snapjpdcledw_integration.kr_this_stage_point_wk_rescue
    ),
dcl_calendar_sysdate
AS (
    SELECT *
    FROM dev_dna_core.snapjpdcledw_integration.dcl_calendar_sysdate
    ),
stage_prv
AS (
    SELECT yyyymm,
        usrid,
        stage,
        stage_cd
    FROM kr_this_stage_point_daily_prv
    JOIN dcl_calendar_sysdate cal ON cal.is_active = true
    WHERE yyyymm = cal.target_month
        AND to_char(cal.curr_date, 'mmdd') BETWEEN '0101'
            AND '0120'
    ),
prp_prv
AS (
    SELECT yyyymm,
        usrid,
        point_granted_sum AS sump,
        point_tobe_granted,
        point_confirmed
    FROM kr_this_stage_point_daily_prv
    JOIN dcl_calendar_sysdate cal ON cal.is_active = true
    WHERE yyyymm = to_char(add_months(cal.curr_date, - 1), 'yyyymm')
        AND to_char(cal.curr_date, 'mm') > '01'
    ),
pgrnt
AS (
    SELECT usrid,
        sum(nvl(point, 0)) sump
    FROM (
        SELECT usrid,
            nvl(point, 0) point
        FROM kr_this_point_granted
        
        UNION ALL
        
        SELECT rsc.usrid,
            nvl(rsc.point_granted, 0) point
        FROM kr_this_stage_point_wk_rescue rsc
        JOIN dcl_calendar_sysdate cal1 ON cal1.is_active = true
            AND 1 = 1
        WHERE rsc.yyyymm BETWEEN CASE 
                        WHEN to_char(cal1.curr_date, 'dd') <= 20
                            THEN to_char(add_months(cal1.curr_date, - 1), 'yyyy') || '01'
                        WHEN to_char(cal1.curr_date, 'dd') > 20
                            THEN to_char(cal1.curr_date, 'yyyy') || '01'
                        END
                AND to_char(cal1.curr_date, 'yyyymm')
        )
    GROUP BY usrid
    ),
base
AS (
    SELECT to_char(cal.curr_date, 'yyyymm') AS yyyymm,
        nvl2(tp.usrid, tp.usrid, lp.usrid) AS usrid,
        CASE 
            WHEN to_char(cal.curr_date, 'mm') = '12'
                THEN 'レギュラー'
            ELSE nvl(nvl(stage_prv.stage, lp.stage), 'レギュラー')
            END AS lstage,
        CASE 
            WHEN to_char(cal.curr_date, 'mm') = '12'
                THEN '01'
            ELSE nvl(nvl(stage_prv.stage_cd, lp.stage_cd), '01')
            END AS lstage_cd,
        nvl(tp.tstage, 'レギュラー') AS tstage,
        nvl(tp.tstage_cd, '01') AS tstage_cd,
        CASE 
            WHEN nvl(lstage_cd, '01') >= nvl(tstage_cd, '01')
                THEN lstage
            WHEN nvl(lstage_cd, '01') < nvl(tstage_cd, '01')
                THEN tstage
            END AS stage,
        CASE 
            WHEN nvl(lstage_cd, '01') >= nvl(tstage_cd, '01')
                THEN lstage_cd
            WHEN nvl(lstage_cd, '01') < nvl(tstage_cd, '01')
                THEN tstage_cd
            END AS stage_cd,
        nvl(tp.thistotalprc, 0) AS thistotalprc,
        nvl(tp.goalp, 0) AS goalp,
        CASE 
            WHEN to_char(cal.curr_date, 'mm') BETWEEN '02'
                    AND '12'
                THEN CASE 
                        WHEN to_char(cal.curr_date, 'dd') = '20'
                            THEN nvl(pgrnt.sump, 0) + nvl(prp_prv.point_confirmed, 0)
                        ELSE nvl(pgrnt.sump, 0)
                        END
            ELSE 0
            END AS point_granted_sum,
        greatest_ignore_nulls(goalp, point_granted_sum) - point_granted_sum AS point_tobe_granted,
        NULL::NUMERIC AS point_confirmed,
        CASE 
            WHEN greatest(nvl(lstage_cd, '01'), nvl(tstage_cd, '01')) = '04'
                THEN 'ダイヤモンド'
            WHEN greatest(nvl(lstage_cd, '01'), nvl(tstage_cd, '01')) = '03'
                THEN 'ダイヤモンド'
            WHEN greatest(nvl(lstage_cd, '01'), nvl(tstage_cd, '01')) = '02'
                THEN 'プラチナ'
            ELSE 'ゴールド'
            END AS promo_stage_1,
        CASE 
            WHEN greatest(nvl(lstage_cd, '01'), nvl(tstage_cd, '01')) = '04'
                THEN '04'
            WHEN greatest(nvl(lstage_cd, '01'), nvl(tstage_cd, '01')) = '03'
                THEN '04'
            WHEN greatest(nvl(lstage_cd, '01'), nvl(tstage_cd, '01')) = '02'
                THEN '03'
            ELSE '02'
            END AS promo_stage_cd_1,
        CASE 
            WHEN greatest(goalp, point_granted_sum) >= 8500
                THEN 'ダイヤモンド'
            WHEN greatest(goalp, point_granted_sum) >= 3500
                THEN 'ダイヤモンド'
            WHEN greatest(goalp, point_granted_sum) >= 500
                THEN 'プラチナ'
            ELSE 'ゴールド'
            END AS promo_stage_2,
        CASE 
            WHEN greatest(goalp, point_granted_sum) >= 8500
                THEN '04'
            WHEN greatest(goalp, point_granted_sum) >= 3500
                THEN '04'
            WHEN greatest(goalp, point_granted_sum) >= 500
                THEN '03'
            ELSE '02'
            END AS promo_stage_cd_2,
        greatest((
                CASE 
                    WHEN greatest(goalp, point_granted_sum) >= 8500
                        THEN 80000
                    WHEN greatest(goalp, point_granted_sum) >= 3500
                        THEN 80000
                    WHEN greatest(goalp, point_granted_sum) >= 500
                        THEN 50000
                    ELSE 15000
                    END
                ), nvl(tp.thistotalprc, 0)) - nvl(tp.thistotalprc, 0) AS next_promo_stage_amt,
        CASE 
            WHEN greatest(goalp, point_granted_sum) >= 8500
                THEN 8500 - greatest(goalp, point_granted_sum)
            WHEN greatest(goalp, point_granted_sum) >= 3500
                THEN 8500 - greatest(goalp, point_granted_sum)
            WHEN greatest(goalp, point_granted_sum) >= 500
                THEN 3500 - greatest(goalp, point_granted_sum)
            ELSE 500
            END 
            AS next_promo_stage_point,
        to_char(convert_timezone('UTC', 'Asia/Tokyo', current_timestamp()), 'yyyymmdd hh24:mi:ss') AS upddate
    FROM kr_this_stage_point_wk_curr tp
    FULL OUTER JOIN kr_last_stage_point lp ON tp.usrid = lp.usrid
    LEFT JOIN stage_prv ON nvl2(tp.usrid, tp.usrid, lp.usrid) = stage_prv.usrid
    LEFT JOIN prp_prv ON nvl2(tp.usrid, tp.usrid, lp.usrid) = prp_prv.usrid
    LEFT JOIN pgrnt ON nvl2(tp.usrid, tp.usrid, lp.usrid) = pgrnt.usrid
    JOIN dcl_calendar_sysdate cal ON cal.is_active = true
        AND 1 = 1
    ),
transformed
AS (
    SELECT base.yyyymm,
        base.usrid,
        base.lstage,
        base.lstage_cd,
        base.tstage,
        base.tstage_cd,
        base.stage,
        base.stage_cd,
        base.thistotalprc,
        base.goalp,
        base.point_granted_sum,
        base.point_tobe_granted,
        base.point_confirmed,
        CASE 
            WHEN base.promo_stage_cd_1 = base.promo_stage_cd_2
                AND base.promo_stage_cd_1 <> base.stage_cd
                THEN base.promo_stage_1
            ELSE NULL
            END AS promo_stage,
        CASE 
            WHEN base.promo_stage_cd_1 = base.promo_stage_cd_2
                AND base.promo_stage_cd_1 <> base.stage_cd
                THEN base.promo_stage_cd_1
            ELSE NULL
            END AS promo_stage_cd,
        next_promo_stage_amt,
        greatest(next_promo_stage_point, 0) AS next_promo_stage_point,
        upddate
    FROM base
    )
    -- select * from transformed
    ,
final
AS (
    SELECT yyyymm::VARCHAR(6) AS yyyymm,
        usrid::number(38, 0) AS usrid,
        lstage::VARCHAR(18) AS lstage,
        lstage_cd::VARCHAR(2) AS lstage_cd,
        tstage::VARCHAR(18) AS tstage,
        tstage_cd::VARCHAR(2) AS tstage_cd,
        stage::VARCHAR(18) AS stage,
        stage_cd::VARCHAR(2) AS stage_cd,
        thistotalprc::number(38, 0) AS thistotalprc,
        goalp::number(18, 0) AS goalp,
        point_granted_sum::number(18, 0) AS point_granted_sum,
        point_tobe_granted::number(18, 0) AS point_tobe_granted,
        point_confirmed::number(18, 0) AS point_confirmed,
        promo_stage::VARCHAR(18) AS promo_stage,
        promo_stage_cd::VARCHAR(2) AS promo_stage_cd,
        next_promo_stage_amt::number(18, 0) AS next_promo_stage_amt,
        next_promo_stage_point::number(18, 0) AS next_promo_stage_point,
        upddate::VARCHAR(17) AS upddate
    FROM transformed
    )
SELECT *
FROM final