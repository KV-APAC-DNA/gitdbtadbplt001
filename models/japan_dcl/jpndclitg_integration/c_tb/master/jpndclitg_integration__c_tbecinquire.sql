{{
    config
    (
        materialized='incremental',
        incremental_strategy= 'delete+insert',
        unique_key= ['diinquireid']
    )
}}

with source as
(
    select * from {{ source('jpndclsdl_raw', 'c_tbecinquire') }}
),

final as
(
    select 
        diinquireid::number(38,0)  as diinquireid,
        diorderid::number(38,0) as diorderid,
        c_dsinquireprep::timestamp_ntz(9) as c_dsinquireprep,
        direceptionist::number(38,0) as direceptionist,
        didifftotalprc::number(38,0) as didifftotalprc,
        c_dihenpinkakuteidt::timestamp_ntz(9) as c_dihenpinkakuteidt,
        diinspectionist::number(38,0) as diinspectionist,
        c_dshenpinsts::varchar(6)  as c_dshenpinsts,
        direnkeists::varchar(1)  as direnkeists,
        c_diallhenpinflg::varchar(1) as c_diallhenpinflg,
        c_dshenpinmaehenkyakuflg::varchar(1)  as c_dshenpinmaehenkyakuflg,
        c_dsgoseikyuteiseiflg::varchar(1)  as c_dsgoseikyuteiseiflg,
        c_dssagakushoriflg::varchar(1)  as c_dssagakushoriflg,
        disagakukbn::varchar(1) as disagakukbn,
        diecusrid::number(38,0) as diecusrid,
        dssei::varchar(30) as dssei,
        dsmei::varchar(30) as dsmei,
        c_diintroduceid::number(38,0) as c_diintroduceid,
        c_dihenkyakuyoteipoint::number(38,0) as c_dihenkyakuyoteipoint,
        c_dikigengirepoint::number(38,0) as c_dikigengirepoint,
        c_dihenkyakusumipoint::number(38,0) as c_dihenkyakusumipoint,
        c_dikakutokuyoteipoint::number(38,0) as c_dikakutokuyoteipoint,
        ditotalprc::number(38,0) as ditotalprc,
        diordertax::number(38,0)  as diordertax,
        ditaxrate::number(38,0)  as ditaxrate,
        c_didiscountprc::number(38,0) as c_didiscountprc,
        c_didiscountall::number(38,0) as c_didiscountall,
        dihaisoprc::number(38,0) as dihaisoprc,
        c_dicollectprc::number(38,0) as c_dicollectprc,
        c_ditoujitsuhaisoprc::number(38,0) as c_ditoujitsuhaisoprc,
        diusepoint::number(38,0) as diusepoint,
        c_dsshikyuhenkinflg::varchar(1)  as c_dsshikyuhenkinflg,
        c_diexchangepoint::number(38,0) as c_diexchangepoint,
        c_diteikidiscountprc::number(38,0) as c_diteikidiscountprc,
        c_dshenpinmemo::varchar(3000) as c_dshenpinmemo,
        c_dspointitemincludeflg::varchar(1) as c_dspointitemincludeflg,
        c_diranktargetprc::number(38,0) as c_diranktargetprc,
        c_diranktotalprc::number(38,0) as c_diranktotalprc,
        c_dipaymentprc::number(38,0) as c_dipaymentprc,
        c_dspointtargetprc::number(38,0) as c_dspointtargetprc,
        c_dinyukinprc::number(38,0) as c_dinyukinprc,
        c_dspotsalesno::varchar(25) as c_dspotsalesno,
        c_dstenposalesno::varchar(16) as c_dstenposalesno,
        c_dsrewriteno::varchar(12) as c_dsrewriteno,
        c_dstempocode::varchar(7) as c_dstempocode,
        c_dstemponame::varchar(192) as c_dstemponame,
        dsprep::timestamp_ntz(9)  as dsprep,
        dsren::timestamp_ntz(9)  as dsren,
        dselim::timestamp_ntz(9) as dselim,
        diprepusr::number(38,0) as diprepusr,
        direnusr::number(38,0) as direnusr,
        dielimusr::number(38,0) as dielimusr,
        dielimflg::varchar(1)  as dielimflg,
        c_diyoyakuhenpinflg::varchar(1)  as c_diyoyakuhenpinflg,
        c_diadjustprc::number(38,0) as c_diadjustprc,
        c_dihenkinprcinputdiff::number(38,0)  as c_dihenkinprcinputdiff,
        c_dihenkinprcinputdifftotal::number(38,0)  as c_dihenkinprcinputdifftotal,
        c_dsrecalcflg::varchar(1)  as c_dsrecalcflg,
        c_dshenpinprockbn::varchar(1) as c_dshenpinprockbn,
        c_diadjustpoint::number(38,0)  as c_diadjustpoint,
        c_dspostshipchangeflg::varchar(4)  as c_dspostshipchangeflg,
        null::varchar(10) as source_file_date,
        inserted_date::timestamp_ntz(9) as inserted_date,
        null::varchar(10) as inserted_by,
        updated_date::timestamp_ntz(9) as updated_date,
        null::varchar(100) as updated_by
    from source
)

select * from source