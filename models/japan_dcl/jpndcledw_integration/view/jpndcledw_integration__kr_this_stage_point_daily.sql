with kr_this_stage_point_daily_curr
as
(
    select * from jpdcledw_integration.kr_this_stage_point_daily_curr
),
kr_this_stage_point_daily_prv as
(
    select * from jpdcledw_integration.kr_this_stage_point_daily_prv
),
t1 as
(
    SELECT kr_this_stage_point_daily_curr.yyyymm,
        kr_this_stage_point_daily_curr.usrid,
        kr_this_stage_point_daily_curr.lstage,
        kr_this_stage_point_daily_curr.lstage_cd,
        kr_this_stage_point_daily_curr.tstage,
        kr_this_stage_point_daily_curr.tstage_cd,
        kr_this_stage_point_daily_curr.stage,
        kr_this_stage_point_daily_curr.stage_cd,
        kr_this_stage_point_daily_curr.thistotalprc,
        kr_this_stage_point_daily_curr.goalp,
        kr_this_stage_point_daily_curr.point_granted_sum,
        kr_this_stage_point_daily_curr.point_tobe_granted,
        kr_this_stage_point_daily_curr.point_confirmed,
        kr_this_stage_point_daily_curr.promo_stage,
        kr_this_stage_point_daily_curr.promo_stage_cd,
        kr_this_stage_point_daily_curr.next_promo_stage_amt,
        kr_this_stage_point_daily_curr.next_promo_stage_point,
        kr_this_stage_point_daily_curr.upddate
    FROM kr_this_stage_point_daily_curr
),
t2 as
(
    SELECT kr_this_stage_point_daily_prv.yyyymm,
        kr_this_stage_point_daily_prv.usrid,
        kr_this_stage_point_daily_prv.lstage,
        kr_this_stage_point_daily_prv.lstage_cd,
        kr_this_stage_point_daily_prv.tstage,
        kr_this_stage_point_daily_prv.tstage_cd,
        kr_this_stage_point_daily_prv.stage,
        kr_this_stage_point_daily_prv.stage_cd,
        kr_this_stage_point_daily_prv.thistotalprc,
        kr_this_stage_point_daily_prv.goalp,
        kr_this_stage_point_daily_prv.point_granted_sum,
        kr_this_stage_point_daily_prv.point_tobe_granted,
        kr_this_stage_point_daily_prv.point_confirmed,
        kr_this_stage_point_daily_prv.promo_stage,
        kr_this_stage_point_daily_prv.promo_stage_cd,
        kr_this_stage_point_daily_prv.next_promo_stage_amt,
        kr_this_stage_point_daily_prv.next_promo_stage_point,
        kr_this_stage_point_daily_prv.upddate
    FROM kr_this_stage_point_daily_prv
),
union_of as
(
    select * from t1 
    union all
    select * from t2
),
final as
(
    select yyyymm::VARCHAR(6) AS YYYYMM,
        usrid::NUMBER(38,0) AS USRID,
        lstage::VARCHAR(18) AS LSTAGE,
        lstage_cd::VARCHAR(2) AS LSTAGE_CD,
        tstage::VARCHAR(18) AS TSTAGE,
        tstage_cd::VARCHAR(2) AS TSTAGE_CD,
        stage::VARCHAR(18) AS STAGE,
        stage_cd::VARCHAR(2) AS STAGE_CD,
        thistotalprc::NUMBER(38,0) AS THISTOTALPRC,
        goalp::NUMBER(38,0) AS GOALP,
        point_granted_sum::NUMBER(38,0) AS POINT_GRANTED_SUM,
        point_tobe_granted::NUMBER(38,0) AS POINT_TOBE_GRANTED,
        point_confirmed::NUMBER(38,0) AS POINT_CONFIRMED,
        promo_stage::VARCHAR(18) AS PROMO_STAGE,
        promo_stage_cd::VARCHAR(2) AS PROMO_STAGE_CD,
        next_promo_stage_amt::NUMBER(38,0) AS NEXT_PROMO_STAGE_AMT,
        next_promo_stage_point::NUMBER(38,0) AS NEXT_PROMO_STAGE_POINT,
        upddate::VARCHAR(17) AS UPDDATE
    from union_of
)
select * from final