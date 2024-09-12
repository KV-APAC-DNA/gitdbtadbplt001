{% if build_month_end_job_models()  %}
with kr_054_cal_v as (
    select * from {{ ref('jpndcledw_integration__kr_054_cal_v') }}
),

kr_054_pfuyo_meisai as (
    select * from {{ ref('jpndcledw_integration__kr_054_pfuyo_meisai') }}
),

result as (
select '01_付与P' as fuyo_label,
    v.yymm::varchar as point_ym,
    substring(v.yymm, 1, 4)::varchar as point_yy,
    substring(v.yymm, 5, 2)::varchar as point_mm,
    sum(nvl(mei.point, 0)) as point_sum
from kr_054_cal_v v
left join kr_054_pfuyo_meisai mei on v.yymm = mei.point_ym
group by '01_付与P',
    v.yymm::varchar,
    substring(v.yymm, 1, 4)::varchar,
    substring(v.yymm, 5, 2)::varchar
    ),

final as (
    select
        fuyo_label::varchar(30) as fuyo_label,
        point_ym::varchar(9) as point_ym,
        point_yy::varchar(6) as point_yy,
        point_mm::varchar(3) as point_mm,
        point_sum::number(38,0) as point
    from result
)

select * from final
{% else %}
    select * from {{this}}
{% endif %}