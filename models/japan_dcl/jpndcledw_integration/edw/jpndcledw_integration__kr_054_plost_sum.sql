with kr_054_plost_meisai
as (
    select *
    from dev_dna_core.snapjpdcledw_integration.kr_054_plost_meisai
    ),
kr_comm_point_para
as (
    select *
    from dev_dna_core.snapjpdcledw_integration.kr_comm_point_para
    ),
transformed
as (
    select '102_失効P',
        substring(lpoint_limit, 1, 6)::varchar as lost_ym,
        substring(lpoint_limit, 1, 4)::varchar as lost_yy,
        substring(lpoint_limit, 5, 2)::varchar as lost_mm,
        substring(lpoint_limit, 1, 4) - 1::varchar as lost_calc_yy,
        substring(lpoint_limit, 5, 2)::varchar as lost_calc_mm,
        sum(nvl(lpoint, 0)) * (- 1) as lpoint
    from kr_054_plost_meisai
    where kr_054_plost_meisai.lpoint_limit <= (
            select term_end
            from kr_comm_point_para
            )
    group by '102_失効P'::varchar,
        substring(lpoint_limit, 1, 6)::varchar,
        substring(lpoint_limit, 1, 4)::varchar,
        substring(lpoint_limit, 5, 2)::varchar,
        substring(lpoint_limit, 1, 4) - 1::varchar,
        substring(lpoint_limit, 5, 2)::varchar
    ),
final
as (
    select '102_失効P'::varchar(20) as lost_label,
        lost_ym::varchar(6) as lost_ym,
        lost_yy::varchar(4) as lost_yy,
        lost_mm::varchar(2) as lost_mm,
        lost_calc_yy::varchar(4) as lost_calc_yy,
        lost_calc_mm::varchar(2) as lost_calc_mm,
        lpoint::number(18, 0) as lpoint
    from transformed
    )
select *
from final
