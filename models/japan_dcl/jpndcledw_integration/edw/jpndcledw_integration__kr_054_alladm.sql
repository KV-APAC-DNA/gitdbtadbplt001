with kr_054_pointadm
as (
    select *
    from {{ ref('jpndcledw_integration__kr_054_pointadm') }}
    ),
kr_054_cpadm
as (
    select *
    from {{ ref('jpndcledw_integration__kr_054_cpadm') }}
    ),
c1
as (
    select adm.c_dipointissueid,
        adm.diecusrid,
        adm.diordercode,
        adm.diorderid,
        adm.c_dikesaiid,
        adm.dspointkubun,
        adm.c_diissuepoint,
        adm.diremnantpoint,
        adm.c_dspointlimitdate,
        adm.diregistdivcode,
        adm.c_dipointchanelid,
        adm.c_dideptid,
        adm.dipointcode,
        adm.divalidflg,
        adm.dspointmemo,
        adm.dspointren,
        adm.c_dsvaliddate,
        adm.c_dstenpoorderno,
        adm.dsprep,
        adm.dsren,
        adm.dselim,
        adm.diprepusr,
        adm.direnusr,
        adm.dielimusr,
        adm.dielimflg
    from kr_054_pointadm adm
    ),
c2
as (
    select adm.c_dipointissueid,
        adm.diecusrid,
        adm.diordercode,
        adm.diorderid,
        adm.c_dikesaiid,
        adm.dspointkubun,
        adm.c_diissuepoint,
        adm.diremnantpoint,
        adm.c_dspointlimitdate,
        adm.diregistdivcode,
        adm.c_dipointchanelid,
        adm.c_dideptid,
        adm.dipointcode,
        adm.divalidflg,
        adm.dspointmemo,
        adm.dspointren,
        adm.c_dsvaliddate,
        adm.c_dstenpoorderno,
        adm.dsprep,
        adm.dsren,
        adm.dselim,
        adm.diprepusr,
        adm.direnusr,
        adm.dielimusr,
        adm.dielimflg
    from kr_054_cpadm adm
    ),
transformed
as (
    select *
    from c1
    
    union all
    
    select *
    from c2
    ),
final
as (
    select c_dipointissueid::number(38, 0) as c_dipointissueid,
        diecusrid::number(38, 0) as diecusrid,
        diordercode::varchar(18) as diordercode,
        diorderid::number(38, 0) as diorderid,
        c_dikesaiid::number(38, 0) as c_dikesaiid,
        dspointkubun::varchar(3) as dspointkubun,
        c_diissuepoint::number(38, 0) as c_diissuepoint,
        diremnantpoint::number(38, 0) as diremnantpoint,
        c_dspointlimitdate::varchar(12) as c_dspointlimitdate,
        diregistdivcode::varchar(7) as diregistdivcode,
        c_dipointchanelid::number(38, 0) as c_dipointchanelid,
        c_dideptid::number(38, 0) as c_dideptid,
        dipointcode::varchar(1) as dipointcode,
        divalidflg::varchar(1) as divalidflg,
        dspointmemo::varchar(1728) as dspointmemo,
        dspointren::timestamp_ntz(9) as dspointren,
        c_dsvaliddate::timestamp_ntz(9) as c_dsvaliddate,
        c_dstenpoorderno::varchar(19) as c_dstenpoorderno,
        dsprep::timestamp_ntz(9) as dsprep,
        dsren::timestamp_ntz(9) as dsren,
        dselim::timestamp_ntz(9) as dselim,
        diprepusr::number(38, 0) as diprepusr,
        direnusr::number(38, 0) as direnusr,
        dielimusr::number(38, 0) as dielimusr,
        dielimflg::varchar(1) as dielimflg
    from transformed
    )
select *
from final