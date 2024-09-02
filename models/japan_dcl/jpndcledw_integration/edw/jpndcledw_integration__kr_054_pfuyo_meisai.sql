with kr_054_allhist as (
    select * from {{ ref('jpndcledw_integration__kr_054_allhist') }}
),

tbusrpram as (
    select * from {{ ref('jpndclitg_integration__tbusrpram') }}
),

result as (
select his.diecusrid as diecusrid,
    cast(cast(substring(c_dspointlimitdate, 1, 6) - 100 as varchar) as integer) as point_ym,
    cast(cast(substring(c_dspointlimitdate, 1, 4) - 1 as varchar) as integer) as point_yy,
    substring(c_dspointlimitdate, 5, 2) as point_mm,
    his.dipoint as point,
    his.c_dipointissueid as point_id,
    his.c_dspointlimitdate as pointlimitdate,
    his.dipointhistid as hist_id,
    his.dsren as dsren
from kr_054_allhist his
inner join tbusrpram usr on his.diecusrid = usr.diusrid
where 1 = 1
    and his.dipoint > 0
    and his.dielimflg = '0'
    and his.dipointcode = '1'
    and his.divalidflg = '1'
    and nvl2(his.dspointmemo, his.dspointmemo, '0') <> '移行ポイント'
    and his.diregistdivcode = '10104'
    and cast(substring(his.c_dspointlimitdate, 1, 4) - 1 as varchar) >= '2020'
    and usr.dsbirthday not like '1600%'
    and usr.dielimflg = '0'
    and usr.disecessionflg = '0'
    and usr.dsdat93 = '通常ユーザ'
    ),

final as (
    select
        diecusrid::number(38,0) as diecusrid,
        point_ym::varchar(9) as point_ym,
        point_yy::varchar(6) as point_yy,
        point_mm::varchar(3) as point_mm,
        point::number(38,0) as point,
        point_id::number(38,0) as point_id,
        pointlimitdate::varchar(12) as pointlimitdate,
        hist_id::number(38,0) as hist_id,
        dsren::timestamp_ntz(9) as dsren
    from result
)

select * from final


