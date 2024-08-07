with kr_054_pfuyo_meisai as (
    select * from {{ ref('jpndcledw_integration__kr_054_pfuyo_meisai') }}
),

kr_054_allhist as (
    select * from dev_dna_core.snapjpdcledw_integration.kr_054_allhist
),

tbecorder as (
    select * from dev_dna_core.snapjpdclitg_integration.tbecorder
),

kr_054_alladm as (
    select * from dev_dna_core.snapjpdcledw_integration.kr_054_alladm
),

kr_comm_point_para as (
    select * from dev_dna_core.snapjpdcledw_integration.kr_comm_point_para
),

tbusrpram as (
select * from dev_dna_core.snapjpdclitg_integration.tbusrpram
)
,

result as (
select his.diecusrid as diecusrid,
    to_char(his.dsren, 'YYYYMM') as use_ym,
    to_char(his.dsren, 'YYYY') as use_yy,
    to_char(his.dsren, 'MM') as use_mm,
    wk.point_ym as point_ym,
    wk.point_yy as point_yy,
    wk.point_mm as point_mm,
    his.dipoint as upoint,
    his.c_dipointissueid as upoint_id,
    his.dipointhistid as hist_id,
    his.dsren as dsren
from kr_054_pfuyo_meisai wk
inner join kr_054_allhist his on his.diecusrid = wk.diecusrid
    and his.c_dipointissueid = wk.point_id
left join tbecorder odr on odr.diorderid = his.diorderid
    and odr.dicancel = 0
    and odr.dielimflg = 0
left join kr_054_alladm adm on adm.c_dipointissueid = his.c_dipointissueid
    and adm.dielimflg = 0
where 1 = 1
    and his.dielimflg = '0'
    and his.diregistdivcode not in ('10008', '10104')
    and nvl2(his.dspointmemo, his.dspointmemo, '0') <> '移行ポイント'
    and his.divalidflg = '1'
    and wk.point_yy >= '2020'
    and to_char(his.dsren, 'YYYYMMDD') <= (
        select term_end
        from kr_comm_point_para
        )
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
        use_ym::varchar(9) as use_ym,
        use_yy::varchar(6) as use_yy,
        use_mm::varchar(3) as use_mm,
        point_ym::varchar(9) as point_ym,
        point_yy::varchar(6) as point_yy,
        point_mm::varchar(3) as point_mm,
        upoint::number(38,0) as upoint,
        upoint_id::number(38,0) as upoint_id,
        hist_id::number(38,0) as hist_id,
        dsren::timestamp_ntz(9) as dsren
    from result
)

select * from final