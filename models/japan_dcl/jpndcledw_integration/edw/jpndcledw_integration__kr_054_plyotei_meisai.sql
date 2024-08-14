with kr_054_alladm
as (
    select *
    from {{ ref('jpndcledw_integration__kr_054_alladm') }}
    ),
kr_054_pfuyo_meisai
as (
    select *
    from {{ ref('jpndcledw_integration__kr_054_pfuyo_meisai') }}
    ),
tbusrpram
as (
    select *
    from {{ ref('jpndclitg_integration__tbusrpram') }}
    ),
transformed
as (
    select adm.diecusrid as diecusrid,
        substring(adm.c_dspointlimitdate, 1, 6) as yotei_ym,
        substring(adm.c_dspointlimitdate, 1, 4) as yotei_yy,
        substring(adm.c_dspointlimitdate, 5, 2) as yotei_mm,
        wk.point_ym as point_ym,
        wk.point_yy as point_yy,
        wk.point_mm as point_mm,
        adm.diremnantpoint as ypoint,
        adm.c_dipointissueid as ypoint_id
    from kr_054_alladm adm
    inner join kr_054_pfuyo_meisai wk on adm.diecusrid = wk.diecusrid
        and adm.c_dipointissueid = wk.point_id
    where 1 = 1
        and adm.dielimflg = '0'
        and adm.divalidflg = '1'
        and adm.dipointcode = '1'
        and adm.diremnantpoint <> 0
        and substring(adm.c_dspointlimitdate, 1, 4) >= '2020'
        and wk.diecusrid in (
            select usr.diusrid
            from tbusrpram usr
            where 1 = 1
                and usr.dielimflg = '0'
                and usr.dsbirthday not like '1600%'
                and usr.disecessionflg = '0'
                and usr.dsdat93 = '通常ユーザ'
            )
    ),
final
as (
    select diecusrid::number(38, 0) as diecusrid,
        yotei_ym::varchar(6) as yotei_ym,
        yotei_yy::varchar(4) as yotei_yy,
        yotei_mm::varchar(2) as yotei_mm,
        point_ym::varchar(6) as point_ym,
        point_yy::varchar(4) as point_yy,
        point_mm::varchar(2) as point_mm,
        ypoint::number(18, 0) as ypoint,
        ypoint_id::number(18, 0) as ypoint_id
    from transformed
    )
select *
from final
