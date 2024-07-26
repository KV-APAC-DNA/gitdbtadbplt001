with kr_this_stage_point_monthly
as
(
    select * from jpdcledw_integration.kr_this_stage_point_monthly
),
final as
(
    SELECT yyyymm::VARCHAR(9) AS YYYYMM,
        kokyano::VARCHAR(30) AS KOKYANO,
        usrid::NUMBER(38,0) AS USRID,
        thistotalprc::NUMBER(38,18) AS THISTOTALPRC,
        stage::VARCHAR(18) AS STAGE,
        thispoint::NUMBER(38,18) AS THISPOINT,
        prevpoint::NUMBER(38,18) AS PREVPOINT,
        point::NUMBER(38,18) AS POINT,
        insertdate::VARCHAR(25) AS INSERTDATE
    FROM kr_this_stage_point_monthly
)
select * from final


