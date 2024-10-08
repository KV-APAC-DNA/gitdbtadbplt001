with tbecorder as (
    select * from {{ ref('jpndclitg_integration__tbecorder') }}
),

final as (
select diorderid as "diorderid",
    diordercode as "diordercode",
    diecusrid as "diecusrid",
    dssei as "dssei",
    dsmei as "dsmei",
    dsseikana as "dsseikana",
    dsmeikana as "dsmeikana",
    c_dimembtype as "c_dimembtype",
    ditodokeid as "ditodokeid",
    dstodokesei as "dstodokesei",
    dstodokemei as "dstodokemei",
    dstodokeseikana as "dstodokeseikana",
    dstodokemeikana as "dstodokemeikana",
    dstodokezip as "dstodokezip",
    c_dstodokeprefcd as "c_dstodokeprefcd",
    dstodokepref as "dstodokepref",
    dstodokecity as "dstodokecity",
    dstodokeaddr as "dstodokeaddr",
    dstodoketatemono as "dstodoketatemono",
    dstodokeaddrkana as "dstodokeaddrkana",
    dstodoketel as "dstodoketel",
    dstodokefax as "dstodokefax",
    dipromid as "dipromid",
    dirouteid as "dirouteid",
    dsorderdt as "dsorderdt",
    c_dskeiyaktodokedate as "c_dskeiyaktodokedate",
    ditotalprc as "ditotalprc",
    dihaisoprc as "dihaisoprc",
    ditaxrate as "ditaxrate",
    c_dsreserveflg as "c_dsreserveflg",
    diusepoint as "diusepoint",
    c_diintroducepointflg as "c_diintroducepointflg",
    c_diintroduceid as "c_diintroduceid",
    dipoint as "dipoint",
    dimonthlypoint as "dimonthlypoint",
    diavailablepoint as "diavailablepoint",
    dihoryu as "dihoryu",
    c_dshoryuriyumemo as "c_dshoryuriyumemo",
    dicancel as "dicancel",
    dishukkasts as "dishukkasts",
    dsuriagedt as "dsuriagedt",
    dsordermemo as "dsordermemo",
    c_dikakutokuyoteipoint as "c_dikakutokuyoteipoint",
    c_dsorderkbn as "c_dsorderkbn",
    c_dsdosokbn as "c_dsdosokbn",
    c_diallhenpinflg as "c_diallhenpinflg",
    c_dshaisoshikibetsukbn as "c_dshaisoshikibetsukbn",
    diordertax as "diordertax",
    c_didiscountprc as "c_didiscountprc",
    c_didiscountall as "c_didiscountall",
    diseikyuprc as "diseikyuprc",
    dinyukinprc as "dinyukinprc",
    diseikyuremain as "diseikyuremain",
    dihenkinzumiprc as "dihenkinzumiprc",
    dihaisokeitai as "dihaisokeitai",
    c_dstorikomiid as "c_dstorikomiid",
    c_dsorderimportdate as "c_dsorderimportdate",
    c_diuketsukeusrid as "c_diuketsukeusrid",
    c_dsuketsuketelcompanycd as "c_dsuketsuketelcompanycd",
    c_dsuketsukeusrname as "c_dsuketsukeusrname",
    c_diinputusrid as "c_diinputusrid",
    c_dsinputusrname as "c_dsinputusrname",
    c_dsinputtelcompanycd as "c_dsinputtelcompanycd",
    c_dilastupdusrid as "c_dilastupdusrid",
    c_dslastupdusrname as "c_dslastupdusrname",
    c_dslastupdtelcompanycd as "c_dslastupdtelcompanycd",
    c_dipendingstsid as "c_dipendingstsid",
    c_dspendingcorrdate as "c_dspendingcorrdate",
    c_dsdeliveryfreeflg as "c_dsdeliveryfreeflg",
    c_dscollectfreeflg as "c_dscollectfreeflg",
    c_dicollectprc as "c_dicollectprc",
    c_ditoujitsuhaisoprc as "c_ditoujitsuhaisoprc",
    c_diclassid as "c_diclassid",
    c_dsusrsts as "c_dsusrsts",
    c_dspointitemincludeflg as "c_dspointitemincludeflg",
    c_diranktargetprc as "c_diranktargetprc",
    c_diranktotalprc as "c_diranktotalprc",
    c_dipaymentprc as "c_dipaymentprc",
    c_diordernum as "c_diordernum",
    c_diordersamplenum as "c_diordersamplenum",
    c_dsorderreferdate as "c_dsorderreferdate",
    c_diregdiscticketprc as "c_diregdiscticketprc",
    c_dsregularautocreateflg as "c_dsregularautocreateflg",
    c_diexchangepoint as "c_diexchangepoint",
    c_dspointtargetprc as "c_dspointtargetprc",
    c_dsrewriteno as "c_dsrewriteno",
    c_dspotsalesno as "c_dspotsalesno",
    c_dstempocode as "c_dstempocode",
    c_dstemponame as "c_dstemponame",
    c_dstenposalesno as "c_dstenposalesno",
    c_dsorigpotsalesno as "c_dsorigpotsalesno",
    c_dsupdateflg as "c_dsupdateflg",
    c_dsfreekbn as "c_dsfreekbn",
    c_dsdeliverydemandflg as "c_dsdeliverydemandflg",
    c_dstoujitsuhaisofreeflg as "c_dstoujitsuhaisofreeflg",
    dibillincluded as "dibillincluded",
    dspackageflg as "dspackageflg",
    c_dssamplelogicd as "c_dssamplelogicd",
    dsprep as "dsprep",
    dsren as "dsren",
    dselim as "dselim",
    diprepusr as "diprepusr",
    direnusr as "direnusr",
    dielimusr as "dielimusr",
    dielimflg as "dielimflg",
    c_diyoyakuhenpinflg as "c_diyoyakuhenpinflg",
    c_dsordercompmailsendkbn as "c_dsordercompmailsendkbn",
    c_diadjustprc as "c_diadjustprc",
    c_dihenkinprcinputdiff as "c_dihenkinprcinputdiff",
    c_dihenkinprcinputdifftotal as "c_dihenkinprcinputdifftotal",
    c_diadjustpoint as "c_diadjustpoint",
    source_file_date as "source_file_date",
    inserted_date as "inserted_date",
    inserted_by as "inserted_by",
    updated_date as "updated_date",
    updated_by as "updated_by"
from tbecorder
)

select * from final