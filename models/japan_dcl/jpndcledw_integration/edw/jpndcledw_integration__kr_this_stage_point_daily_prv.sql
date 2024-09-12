{{
    config
    (
        materialized = 'incremental',
        incremental_strategy = 'append',
        pre_hook = "{% if is_incremental() %}
                    DELETE FROM {{this}} WHERE YYYYMM in  (SELECT DISTINCT YYYYMM FROM {{ ref('jpndcledw_integration__kr_this_stage_point_wk_prv') }} );
                    {% endif %} "

    )
}}

WITH kr_this_stage_point_wk_rescue
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__kr_this_stage_point_wk_rescue') }}
    ),
kr_this_point_granted
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__kr_this_point_granted') }}
    ),
kr_this_stage_point_wk_prv
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__kr_this_stage_point_wk_prv') }}
    ),
kr_last_stage_point
AS (
    SELECT *
    FROM {{ source('jpdcledw_integration', 'kr_last_stage_point') }}   --using as source as cycle is created
    ),
dcl_calendar_sysdate
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__dcl_calendar_sysdate') }}
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
                AND cal1.target_month
        )
    GROUP BY usrid
    ),
transformed
AS (
    SELECT to_char(add_months(cal.curr_date, - 1), 'yyyymm') AS yyyym,
        nvl2(tp.usrid, tp.usrid, lp.usrid) AS usrid,
        nvl(lp.stage, 'レギュラー') AS lstage,
        nvl(lp.stage_cd, '01') AS lstage_cd,
        nvl(tp.tstage, 'レギュラー') AS tstage,
        nvl(tp.tstage_cd, '01') AS tstage_cd,
        CASE
            WHEN nvl(lp.stage_cd, '01') >= nvl(tp.tstage_cd, '01')
                THEN nvl(lp.stage, 'レギュラー')
            WHEN nvl(lp.stage_cd, '01') < nvl(tp.tstage_cd, '01')
                THEN nvl(tp.tstage, 'レギュラー')
            END AS stage,
        CASE
            WHEN nvl(lp.stage_cd, '01') >= nvl(tp.tstage_cd, '01')
                THEN nvl(lp.stage_cd, '01')
            WHEN nvl(lp.stage_cd, '01') < nvl(tp.tstage_cd, '01')
                THEN nvl(tp.tstage_cd, '01')
            END AS stage_cd,
        nvl(tp.thistotalprc, 0) AS thistotalprc,
        nvl(tp.goalp, 0) AS goalp,
        nvl(pgrnt.sump, 0) AS point_granted_sum,
        CASE
            WHEN to_char(cal.curr_date, 'dd') < '20'
                THEN greatest(nvl(tp.goalp, 0), nvl(pgrnt.sump, 0)) - nvl(pgrnt.sump, 0)
            ELSE 0
            END AS point_tobe_granted,
        CASE
            WHEN to_char(cal.curr_date, 'dd') = '20'
                THEN greatest(nvl(tp.goalp, 0), nvl(pgrnt.sump, 0)) - nvl(pgrnt.sump, 0)
            ELSE 0
            END AS point_confirmed,
        NULL AS promo_stage,
        NULL AS promo_stage_cd,
        NULL::INTEGER AS next_promo_stage_amt,
        NULL::INTEGER AS next_promo_stage_point,
        to_char(convert_timezone('Asia/Tokyo', current_timestamp()), 'yyyymmdd hh24:mi:ss') AS upddate
    FROM kr_this_stage_point_wk_prv tp
    FULL OUTER JOIN kr_last_stage_point lp ON tp.usrid = lp.usrid
    LEFT JOIN pgrnt ON nvl2(tp.usrid, tp.usrid, lp.usrid) = pgrnt.usrid
    JOIN dcl_calendar_sysdate cal ON cal.is_active = true
        AND 1 = 1
    WHERE (
            SELECT count(*)
            FROM kr_this_stage_point_wk_prv
            ) > 0
    ),
final as
(
    SELECT
        yyyym::VARCHAR(6) AS yyyymm,
        usrid::NUMBER(38,0) AS usrid,
        lstage::VARCHAR(18) AS lstage,
        lstage_cd::VARCHAR(2) AS lstage_cd,
        tstage::VARCHAR(18) AS tstage,
        tstage_cd::VARCHAR(2) AS tstage_cd,
        stage::VARCHAR(18) AS stage,
        stage_cd::VARCHAR(2) AS stage_cd,
        thistotalprc::NUMBER(38,0) AS thistotalprc,
        goalp::NUMBER(18,0) AS goalp,
        point_granted_sum::NUMBER(18,0) AS point_granted_sum,
        point_tobe_granted::NUMBER(18,0) AS point_tobe_granted,
        point_confirmed::NUMBER(18,0) AS point_confirmed,
        promo_stage::VARCHAR(18) AS promo_stage,
        promo_stage_cd::VARCHAR(2) AS promo_stage_cd,
        next_promo_stage_amt::NUMBER(18,0) AS next_promo_stage_amt,
        next_promo_stage_point::NUMBER(18,0) AS next_promo_stage_point,
        upddate::VARCHAR(17) AS upddate
    FROM transformed
)
SELECT *
FROM final