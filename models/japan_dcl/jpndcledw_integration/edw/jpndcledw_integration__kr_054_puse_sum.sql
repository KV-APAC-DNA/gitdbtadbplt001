with kr_054_cal_v as (
    select * from {{ ref('jpndcledw_integration__kr_054_cal_v') }}
),

kr_054_puse_meisai as (
    select * from {{ ref('jpndcledw_integration__kr_054_puse_meisai') }}
),


result as (
select '101_利用P' as use_label,
    use_ym::varchar as use_ym,
    use_yy::varchar as use_yy,
    use_mm::varchar as use_mm,
    v.yymm::varchar as point_ym,
    substring(v.yymm, 1, 4)::varchar as point_yy,
    substring(v.yymm, 5, 2)::varchar as point_mm,
    sum(nvl(upoint, 0)) * (- 1) as upoint
from kr_054_cal_v v
left join kr_054_puse_meisai mei on v.yymm = mei.point_ym
group by '101_利用P',
    use_ym::varchar,
    use_yy::varchar,
    use_mm::varchar,
    v.yymm::varchar,
    substring(v.yymm, 1, 4)::varchar,
    substring(v.yymm, 5, 2)::varchar
),

final as (
    select
        use_label::varchar(20) as use_label,
        use_ym::varchar(6) as use_ym,
        use_yy::varchar(4) as use_yy,
        use_mm::varchar(2) as use_mm,
        point_ym::varchar(6) as point_ym,
        point_yy::varchar(4) as point_yy,
        point_mm::varchar(2) as point_mm,
        upoint::number(18,0) as upoint
    from result
)

select * from final