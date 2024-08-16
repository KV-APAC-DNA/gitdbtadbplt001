with kr_054_fsoutei_meisai as (
    select * from {{ ref('jpndcledw_integration__kr_054_fsoutei_meisai') }}
),

result as (
select '02_付与想定P' as fuyo_label,
    point_ym::varchar as point_ym,
    point_yy::varchar as point_yy,
    point_mm::varchar as point_mm,
    sum(nvl(fuyoyotei_point, 0)) as point_sum
from kr_054_fsoutei_meisai
group by '02_付与想定P',
    point_ym::varchar,
    point_yy::varchar,
    point_mm::varchar
),

final as (
    select 
        fuyo_label::varchar(20) as fuyo_label,
        point_ym::varchar(6) as point_ym,
        point_yy::varchar(4) as point_yy,
        point_mm::varchar(2) as point_mm,
        point_sum::number(18,0) as point
    from result
)

select * from final