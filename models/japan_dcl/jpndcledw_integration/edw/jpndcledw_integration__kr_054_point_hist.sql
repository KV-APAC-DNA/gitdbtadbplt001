--
with
    kr_054_allhist as (
        select * from dev_dna_core.snapjpdcledw_integration.kr_054_allhist
    ),

    tbecorder as (select * from dev_dna_core.snapjpdclitg_integration.tbecorder),

    kr_054_alladm as (
        select * from dev_dna_core.snapjpdcledw_integration.kr_054_alladm
    ),

    kr_054_v_ptrgstdivmst as (
        select * from dev_dna_core.snapjpdcledw_integration.kr_054_v_ptrgstdivmst
    ),

    tbusrpram as (select * from dev_dna_core.snapjpdclitg_integration.tbusrpram),

    kr_054_pfuyo_meisai as (
        select * from dev_dna_core.snapjpdcledw_integration.kr_054_pfuyo_meisai
    ),

    kr_054_puse_meisai as (
        select * from dev_dna_core.snapjpdcledw_integration.kr_054_puse_meisai
    ),

    kr_054_plost_meisai as (
        select * from dev_dna_core.snapjpdcledw_integration.kr_054_plost_meisai
    ),

    raw_transformed as (
        select
            his.diecusrid as diecusrid,
            odr.diorderid as diorderid,
            case
                when his.dipointcode = '2'
                then to_char(nvl(odr.dsuriagedt, his.dsren), 'YYYYMMDD')
                else to_char(his.dsren, 'YYYYMMDD')
            end as p_date,
            his.diregistdivcode as diregistdivcode,
            nvl(odr.dirouteid, '99') as dirouteid,
            his.dipointcode as dipointcode,
            his.dipoint as dipoint,
            to_char(adm.dsprep, 'YYYYMM') as dsprep,
            mas.c_dsregistdivname as divname_mae,
            his.c_dipointissueid as point_id,
            his.dipointhistid as hist_id
        from kr_054_allhist his
        left join
            tbecorder odr
            on odr.diorderid = his.diorderid
            and odr.dicancel = 0
            and odr.dielimflg = 0
        left join
            kr_054_alladm adm
            on adm.c_dipointissueid = his.c_dipointissueid
            and adm.dielimflg = 0
        inner join
            kr_054_v_ptrgstdivmst mas on mas.diregistdivcode = adm.diregistdivcode
        inner join tbusrpram usr on his.diecusrid = usr.diusrid
        where
            1 = 1
            and usr.dielimflg = '0'
            and usr.disecessionflg = '0'
            and usr.dsdat93 = '通常ユーザ'
            and his.dielimflg = 0
            and his.dipointcode <> 0
            and his.divalidflg <> 0
    ),

    union_1 as (select * from raw_transformed where divname_mae != 'バースデー特典'),

    union_2 as (

        select
            his.diecusrid as diecusrid,
            odr.diorderid as diorderid,
            fuyo.point_ym || '01' as p_date,
            his.diregistdivcode as diregistdivcode,
            nvl(odr.dirouteid, '99') as dirouteid,
            his.dipointcode as dipointcode,
            his.dipoint as dipoint,
            fuyo.point_ym as dsprep,
            mas.c_dsregistdivname as divname_mae,
            his.c_dipointissueid as point_id,
            his.dipointhistid as hist_id
        from kr_054_allhist his
        left join
            tbecorder odr
            on odr.diorderid = his.diorderid
            and odr.dicancel = 0
            and odr.dielimflg = 0
        left join
            kr_054_alladm adm
            on adm.c_dipointissueid = his.c_dipointissueid
            and adm.dielimflg = 0
        inner join
            kr_054_v_ptrgstdivmst mas on mas.diregistdivcode = adm.diregistdivcode
        inner join
            kr_054_pfuyo_meisai fuyo
            on his.diecusrid = fuyo.diecusrid
            and his.dipointhistid = fuyo.hist_id

    ),

    union_3 as (
        select
            his.diecusrid as diecusrid,
            odr.diorderid as diorderid,
            to_char(puse.dsren, 'YYYYMMDD') as p_date,
            his.diregistdivcode as diregistdivcode,
            nvl(odr.dirouteid, '99') as dirouteid,
            his.dipointcode as dipointcode,
            his.dipoint as dipoint,
            to_char(puse.dsren, 'YYYYMM') as dsprep,
            mas.c_dsregistdivname as divname_mae,
            his.c_dipointissueid as point_id,
            his.dipointhistid as hist_id
        from kr_054_allhist his
        left join
            tbecorder odr
            on odr.diorderid = his.diorderid
            and odr.dicancel = 0
            and odr.dielimflg = 0
        left join
            kr_054_alladm adm
            on adm.c_dipointissueid = his.c_dipointissueid
            and adm.dielimflg = 0
        inner join
            kr_054_v_ptrgstdivmst mas on mas.diregistdivcode = adm.diregistdivcode
        inner join tbusrpram usr on his.diecusrid = usr.diusrid
        inner join
            kr_054_puse_meisai puse
            on his.diecusrid = puse.diecusrid
            and his.dipointhistid = puse.hist_id
    ),

    union_4 as (
        select
            his.diecusrid as diecusrid,
            odr.diorderid as diorderid,
            plost.lpoint_limit as p_date,
            his.diregistdivcode as diregistdivcode,
            nvl(odr.dirouteid, '99') as dirouteid,
            his.dipointcode as dipointcode,
            his.dipoint as dipoint,
            plost.lost_ym as dsprep,
            mas.c_dsregistdivname as divname_mae,
            his.c_dipointissueid as point_id,
            his.dipointhistid as hist_id
        from kr_054_allhist his
        left join
            tbecorder odr
            on odr.diorderid = his.diorderid
            and odr.dicancel = 0
            and odr.dielimflg = 0
        left join
            kr_054_alladm adm
            on adm.c_dipointissueid = his.c_dipointissueid
            and adm.dielimflg = 0
        inner join
            kr_054_v_ptrgstdivmst mas on mas.diregistdivcode = adm.diregistdivcode
        inner join tbusrpram usr on his.diecusrid = usr.diusrid
        inner join
            kr_054_plost_meisai plost
            on his.diecusrid = plost.diecusrid
            and his.dipointhistid = plost.hist_id
    ),

    transformed as (
        select *
        from union_1
        union all
        select *
        from union_2
        union all
        select *
        from union_3
        union all
        select *
        from union_4

    ),

    final as (select * from transformed)

select *
from final
