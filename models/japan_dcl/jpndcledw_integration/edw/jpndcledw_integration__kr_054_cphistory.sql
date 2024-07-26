with tbecpointhistory
as (
    select *
    from dev_dna_core.snapjpdclitg_integration.tbecpointhistory
    ),
tbusrpram
as (
    select *
    from dev_dna_core.snapjpdclitg_integration.tbusrpram
    ),
c1
as (
    select hist.dipointhistid,
        hist.diecusrid,
        hist.diorderid,
        '10104' as diregistdivcode,
        hist.diidentid,
        hist.dsidentname,
        hist.dipointcode,
        hist.divalidflg,
        hist.dipoint,
        hist.dspointmemo,
        hist.dspointren,
        hist.c_dsvaliddate,
        hist.dsprep,
        hist.dsren,
        hist.dselim,
        hist.diprepusr,
        hist.direnusr,
        hist.dielimusr,
        hist.dielimflg,
        hist.c_dipointissueid,
        hist.diordercode,
        hist.c_dikesaiid,
        hist.diinquireid,
        hist.c_dspointlimitdate,
        hist.c_dipointchanelid,
        hist.c_dideptid,
        hist.c_dikokuchipoint,
        hist.c_dstenpoorderno,
        hist.c_dstenpoorderno2,
        hist.c_diregularreservationid
    from tbecpointhistory hist
    inner join tbusrpram usr on hist.diecusrid = usr.diusrid
    where 1 = 1
        and hist.diregistdivcode = '20001'
        and hist.dspointmemo like '%前年累計購入金額%'
        and usr.dsbirthday not like '1600%'
        and substring(usr.dsbirthday, 6, 2) = '01'
    ),
c2
as (
    select hist.dipointhistid,
        hist.diecusrid,
        hist.diorderid,
        '20002' as diregistdivcode
        --,'50001'        as diregistdivcode               
        ,
        hist.diidentid,
        hist.dsidentname,
        hist.dipointcode,
        hist.divalidflg,
        hist.dipoint,
        hist.dspointmemo,
        hist.dspointren,
        hist.c_dsvaliddate,
        hist.dsprep,
        hist.dsren,
        hist.dselim,
        hist.diprepusr,
        hist.direnusr,
        hist.dielimusr,
        hist.dielimflg,
        hist.c_dipointissueid,
        hist.diordercode,
        hist.c_dikesaiid,
        hist.diinquireid,
        hist.c_dspointlimitdate,
        hist.c_dipointchanelid,
        hist.c_dideptid,
        hist.c_dikokuchipoint,
        hist.c_dstenpoorderno,
        hist.c_dstenpoorderno2,
        hist.c_diregularreservationid
    from tbecpointhistory hist
    inner join tbusrpram usr on hist.diecusrid = usr.diusrid
    where 1 = 1
        and hist.diregistdivcode in ('20001', '20002')
        -- and hist.diregistdivcode            =  '20001'                           
        and hist.dspointmemo like '%基本ポイント%'
    ),
c3
as (
    select hist.dipointhistid,
        hist.diecusrid,
        hist.diorderid,
        '50005' as diregistdivcode,
        hist.diidentid,
        hist.dsidentname,
        hist.dipointcode,
        hist.divalidflg,
        hist.dipoint,
        hist.dspointmemo,
        hist.dspointren,
        hist.c_dsvaliddate,
        hist.dsprep,
        hist.dsren,
        hist.dselim,
        hist.diprepusr,
        hist.direnusr,
        hist.dielimusr,
        hist.dielimflg,
        hist.c_dipointissueid,
        hist.diordercode,
        hist.c_dikesaiid,
        hist.diinquireid,
        hist.c_dspointlimitdate,
        hist.c_dipointchanelid,
        hist.c_dideptid,
        hist.c_dikokuchipoint,
        hist.c_dstenpoorderno,
        hist.c_dstenpoorderno2,
        hist.c_diregularreservationid
    from tbecpointhistory hist
    inner join tbusrpram usr on hist.diecusrid = usr.diusrid
    where 1 = 1
        and hist.diregistdivcode in ('20001', '20003')
        --and hist.diregistdivcode            =  '20001'                          
        and hist.dspointmemo like '%ポイント５倍キャンペーン%'
    ),
c4
as (
    select hist.dipointhistid,
        hist.diecusrid,
        hist.diorderid,
        '20005' as diregistdivcode
        --,'50006'                 as diregistdivcode    
        ,
        hist.diidentid,
        hist.dsidentname,
        hist.dipointcode,
        hist.divalidflg,
        hist.dipoint,
        'ステージアップボーナスポイント' as dspointmemo,
        hist.dspointren,
        hist.c_dsvaliddate,
        hist.dsprep,
        hist.dsren,
        hist.dselim,
        hist.diprepusr,
        hist.direnusr,
        hist.dielimusr,
        hist.dielimflg,
        hist.c_dipointissueid,
        hist.diordercode,
        hist.c_dikesaiid,
        hist.diinquireid,
        hist.c_dspointlimitdate,
        hist.c_dipointchanelid,
        hist.c_dideptid,
        hist.c_dikokuchipoint,
        hist.c_dstenpoorderno,
        hist.c_dstenpoorderno2,
        hist.c_diregularreservationid
    from tbecpointhistory hist
    inner join tbusrpram usr on hist.diecusrid = usr.diusrid
    where 1 = 1
        and hist.diregistdivcode in ('20001', '20005')
        -- and hist.diregistdivcode            =  '20001'                           
        and (
            hist.dspointmemo like '%シーラボポイントアッププログラム%'
            and to_char(hist.dsprep, 'yyyymmdd') = '20210322'
            )
        or (
            hist.dspointmemo like '%ステージアップボーナスポイント%'
            and to_char(hist.dsprep, 'yyyymmdd') <> '20210322'
            )
    ),
c5
as (
    select hist.dipointhistid,
        hist.diecusrid,
        hist.diorderid,
        '20004' as diregistdivcode
        --,'50007'        as diregistdivcode             
        ,
        hist.diidentid,
        hist.dsidentname,
        hist.dipointcode,
        hist.divalidflg,
        hist.dipoint,
        hist.dspointmemo,
        hist.dspointren,
        hist.c_dsvaliddate,
        hist.dsprep,
        hist.dsren,
        hist.dselim,
        hist.diprepusr,
        hist.direnusr,
        hist.dielimusr,
        hist.dielimflg,
        hist.c_dipointissueid,
        hist.diordercode,
        hist.c_dikesaiid,
        hist.diinquireid,
        hist.c_dspointlimitdate,
        hist.c_dipointchanelid,
        hist.c_dideptid,
        hist.c_dikokuchipoint,
        hist.c_dstenpoorderno,
        hist.c_dstenpoorderno2,
        hist.c_diregularreservationid
    from tbecpointhistory hist
    inner join tbusrpram usr on hist.diecusrid = usr.diusrid
    where 1 = 1
        and hist.diregistdivcode in ('20001', '20004')
        --and hist.diregistdivcode            =  '20001'                        
        and (
            hist.dspointmemo like '%シーラボポイントアッププログラム%'
            and to_char(hist.dsprep, 'yyyymmdd') <> '20210322'
            )
    ),
c6
as (
    select hist.dipointhistid,
        hist.diecusrid,
        hist.diorderid,
        '50008' as diregistdivcode,
        hist.diidentid,
        hist.dsidentname,
        hist.dipointcode,
        hist.divalidflg,
        hist.dipoint,
        hist.dspointmemo,
        hist.dspointren,
        hist.c_dsvaliddate,
        hist.dsprep,
        hist.dsren,
        hist.dselim,
        hist.diprepusr,
        hist.direnusr,
        hist.dielimusr,
        hist.dielimflg,
        hist.c_dipointissueid,
        hist.diordercode,
        hist.c_dikesaiid,
        hist.diinquireid,
        hist.c_dspointlimitdate,
        hist.c_dipointchanelid,
        hist.c_dideptid,
        hist.c_dikokuchipoint,
        hist.c_dstenpoorderno,
        hist.c_dstenpoorderno2,
        hist.c_diregularreservationid
    from tbecpointhistory hist
    inner join tbusrpram usr on hist.diecusrid = usr.diusrid
    where 1 = 1
        and hist.diregistdivcode in ('20001', '20003')
        -- and hist.diregistdivcode            =  '20001'                           
        and hist.dspointmemo like '%ポイント１０倍キャンペーン%'
    ),
transformed
as (
    select *
    from c1
    
    union all
    
    select *
    from c2
    
    union all
    
    select *
    from c3
    
    union all
    
    select *
    from c4
    
    union all
    
    select *
    from c5
    
    union all
    
    select *
    from c6
    ),
final
as (
    select dipointhistid::number(38, 0) as dipointhistid,
        diecusrid::number(38, 0) as diecusrid,
        diorderid::number(38, 0) as diorderid,
        diregistdivcode::varchar(7) as diregistdivcode,
        diidentid::number(38, 0) as diidentid,
        dsidentname::varchar(384) as dsidentname,
        dipointcode::varchar(1) as dipointcode,
        divalidflg::varchar(1) as divalidflg,
        dipoint::number(38, 0) as dipoint,
        dspointmemo::varchar(1728) as dspointmemo,
        dspointren::timestamp_ntz(9) as dspointren,
        c_dsvaliddate::timestamp_ntz(9) as c_dsvaliddate,
        dsprep::timestamp_ntz(9) as dsprep,
        dsren::timestamp_ntz(9) as dsren,
        dselim::timestamp_ntz(9) as dselim,
        diprepusr::number(38, 0) as diprepusr,
        direnusr::number(38, 0) as direnusr,
        dielimusr::number(38, 0) as dielimusr,
        dielimflg::varchar(1) as dielimflg,
        c_dipointissueid::number(38, 0) as c_dipointissueid,
        diordercode::varchar(18) as diordercode,
        c_dikesaiid::number(38, 0) as c_dikesaiid,
        diinquireid::number(38, 0) as diinquireid,
        c_dspointlimitdate::varchar(12) as c_dspointlimitdate,
        c_dipointchanelid::number(38, 0) as c_dipointchanelid,
        c_dideptid::number(38, 0) as c_dideptid,
        c_dikokuchipoint::number(38, 0) as c_dikokuchipoint,
        c_dstenpoorderno::varchar(19) as c_dstenpoorderno,
        c_dstenpoorderno2::varchar(19) as c_dstenpoorderno2,
        c_diregularreservationid::number(38, 0) as c_diregularreservationid
    from transformed
    )
select *
from final
