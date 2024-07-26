with kr_054_cal_v
as (
    select *
    from dev_dna_core.snapjpdcledw_integration.kr_054_cal_v
    ),
kr_054_plyotei_meisai
as (
    select *
    from dev_dna_core.snapjpdcledw_integration.kr_054_plyotei_meisai
    ),
transformed
as (
    select '103_失効予定P',
        mei.yotei_ym::varchar as yotei_ym,
        mei.yotei_yy::varchar as yotei_yy,
        mei.yotei_mm::varchar as yotei_mm,
        v.yymm::varchar as point_ym,
        substring(v.yymm, 1, 4)::varchar as point_yy,
        substring(v.yymm, 5, 2)::varchar as point_mm,
        sum(nvl(mei.ypoint, 0)) as yotei_sum
    from kr_054_cal_v v
    left join kr_054_plyotei_meisai mei on v.yymm = mei.point_ym
    group by '103_失効予定P'::varchar,
        yotei_ym::varchar,
        yotei_yy::varchar,
        yotei_mm::varchar,
        v.yymm::varchar,
        substring(v.yymm, 1, 4)::varchar,
        substring(v.yymm, 5, 2)::varchar
    ),
final
as (
    select '103_失効予定P'::varchar(20) as yotei_label,
        yotei_ym::varchar(6) as yotei_ym,
        yotei_yy::varchar(4) as yotei_yy,
        yotei_mm::varchar(2) as yotei_mm,
        point_ym::varchar(6) as point_ym,
        point_yy::varchar(4) as point_yy,
        point_mm::varchar(2) as point_mm,
        yotei_sum::number(18, 0) as ypoint
    from transformed
    )
select *
from final
