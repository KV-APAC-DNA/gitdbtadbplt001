{% if build_month_end_job_models()  %}
with c_tbecpointadm
as (
    select *
    from {{ ref('jpndclitg_integration__c_tbecpointadm') }}
    ),
tbusrpram
as (
    select *
    from {{ ref('jpndclitg_integration__tbusrpram') }}
    ),
cte_1
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
        '10104' as diregistdivcode,
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
    from c_tbecpointadm adm
    inner join tbusrpram usr on adm.diecusrid = usr.diusrid
    where 1 = 1
        and adm.diregistdivcode = '20001'
        and adm.dspointmemo like '%前年累計購入金額%'
        and usr.dsbirthday not like '1600%'
        and substring(usr.dsbirthday, 6, 2) = '01'
    ),
cte_2
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
        '20002' as diregistdivcode
        --,'50001'        as diregistdivcode  
        ,
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
    from c_tbecpointadm adm
    inner join tbusrpram usr on adm.diecusrid = usr.diusrid
    where 1 = 1
        and adm.diregistdivcode in ('20001', '20002')
        -- and adm.diregistdivcode            =  '20001'   
        and adm.dspointmemo like '%基本ポイント%'
    ),
cte_3
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
        '50005' as diregistdivcode,
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
    from c_tbecpointadm adm
    inner join tbusrpram usr on adm.diecusrid = usr.diusrid
    where 1 = 1
        and adm.diregistdivcode in ('20001', '20003')
        --and adm.diregistdivcode            =  '20001'                            
        and adm.dspointmemo like '%ポイント５倍キャンペーン%'
    ),
cte_4
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
        '20005' as diregistdivcode
        -- ,'50006'                 as diregistdivcode   
        ,
        adm.c_dipointchanelid,
        adm.c_dideptid,
        adm.dipointcode,
        adm.divalidflg,
        'ステージアップボーナスポイント' as dspointmemo,
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
    from c_tbecpointadm adm
    inner join tbusrpram usr on adm.diecusrid = usr.diusrid
    where 1 = 1
        and adm.diregistdivcode in ('20001', '20005')
        -- and adm.diregistdivcode  =  '20001'                                     
        and (
            adm.dspointmemo like '%シーラボポイントアッププログラム%'
            and to_char(adm.dsprep, 'yyyymmdd') = '20210322'
            )
        or (
            adm.dspointmemo like '%ステージアップボーナスポイント%'
            and to_char(adm.dsprep, 'yyyymmdd') <> '20210322'
            )
    ),
cte_5
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
        '20004' as diregistdivcode
        --,'50007'        as diregistdivcode               
        ,
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
    from c_tbecpointadm adm
    inner join tbusrpram usr on adm.diecusrid = usr.diusrid
    where 1 = 1
        and adm.diregistdivcode in ('20001', '20004')
        -- and adm.diregistdivcode  =  '20001'                                       -- campain code
        and (
            adm.dspointmemo like '%シーラボポイントアッププログラム%' -- pointmemo desc.
            and to_char(adm.dsprep, 'yyyymmdd') <> '20210322'
            )
    ),
cte_6
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
        '50008' as diregistdivcode,
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
    from c_tbecpointadm adm
    inner join tbusrpram usr on adm.diecusrid = usr.diusrid
    where 1 = 1
        and adm.diregistdivcode in ('20001', '20003')
        -- and adm.diregistdivcode            =  '20001'                           
        and adm.dspointmemo like '%ポイント１０倍キャンペーン%'
    ),
transformed
as (
    select *
    from cte_1
    
    union all
    
    select *
    from cte_2
    
    union all
    
    select *
    from cte_3
    
    union all
    
    select *
    from cte_4
    
    union all
    
    select *
    from cte_5
    
    union all
    
    select *
    from cte_6
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
{% else %}
    select * from {{this}}
{% endif %}