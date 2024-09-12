{% if build_month_end_job_models()  %}
with kr_054_allhist as (
    select * from {{ ref('jpndcledw_integration__kr_054_allhist') }}
),

kr_054_pfuyo_meisai as (
    select * from {{ ref('jpndcledw_integration__kr_054_pfuyo_meisai') }}
),

tbusrpram as (
    select * from {{ ref('jpndclitg_integration__tbusrpram') }}
),


result as (
select his.diecusrid as diecusrid,
    to_char(his.dsren, 'YYYYMM') as lost_ym,
    to_char(his.dsren, 'YYYY') as lost_yy,
    to_char(his.dsren, 'MM') as lost_mm,
    his.dipoint as lpoint,
    his.c_dipointissueid as lpoint_id,
    his.c_dspointlimitdate as lpoint_limit,
    his.dipointhistid as hist_id,
    his.dsren as dsren
from kr_054_allhist his
inner join (
    select distinct diecusrid,
        point_id
    from kr_054_pfuyo_meisai
    ) wk on his.diecusrid = wk.diecusrid
    and his.c_dipointissueid = wk.point_id
where 1 = 1
    and his.dielimflg = '0'
    and his.dipoint < 0
    and his.diregistdivcode = '10008'
    and his.divalidflg = '1'
    and to_char(his.dsren, 'YYYY') >= '2020'
    and his.diecusrid in (
        select usr.diusrid
        from tbusrpram usr
        where 1 = 1
            and usr.dielimflg = '0'
            and usr.dsbirthday not like '1600%'
            and usr.disecessionflg = '0'
            and usr.dsdat93 = '通常ユーザ'
        )
),

final as (
    select
        diecusrid::number(38,0) as diecusrid,
        lost_ym::varchar(9) as lost_ym,
        lost_yy::varchar(6) as lost_yy,
        lost_mm::varchar(3) as lost_mm,
        lpoint::number(38,0) as lpoint,
        lpoint_id::number(38,0) as lpoint_id,
        lpoint_limit::varchar(12) as lpoint_limit,
        hist_id::number(38,0) as hist_id,
        dsren::timestamp_ntz(9) as dsren
    from result
)

select * from final
{% else %}
    select * from {{this}}
{% endif %}