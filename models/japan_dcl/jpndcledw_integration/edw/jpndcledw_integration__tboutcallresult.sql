with tbOutcallResult_wk as (
    select * from dev_dna_core.jpdcledw_integration.tbOutcallResult_wk
),
tbOutcallTel as (
    select * from dev_dna_core.jpdcledw_integration.tbOutcallTel
),
KR_THIS_STAGE_POINT_MONTHLY as (
    select * from dev_dna_core.jpdcledw_integration.KR_THIS_STAGE_POINT_MONTHLY
),
tel5 as (
    select 
        maintel 
    from tbOutcallResult_wk
    group by 
        maintel
    having 
        count(diusrid)>4
),
latest_stage as (
    select
        stage,
        usrid
    from 
        KR_THIS_STAGE_POINT_MONTHLY
    where
        YYYYMM=
        (
            select
                max(YYYYMM)
            from KR_THIS_STAGE_POINT_MONTHLY
        )
),
transformed as (    
    SELECT 
       result_wk.diusrid,
       result_wk.dsname,
       result_wk.dsname2,
       result_wk.dskana,
       result_wk.dskana2,
       result_wk.dstel,
       result_wk.dsdat2,
       result_wk.dsdat3,
       result_wk.dsdat4,
       result_wk.dszip,
       result_wk.dspref,
       result_wk.address,
       result_wk.dspromname,
       result_wk.dsbirthday,
       result_wk.dsdat9,
       result_wk.c_dsdmname,
       nvl(latest_stage.stage,'レギュラー') stage,
       result_wk.age,
       result_wk.dsseibetsu,
       result_wk.dipoint,
       result_wk.c_diissuepoint,
       result_wk.c_disumusepoint,
       result_wk.c_dspointlimitdate,
       result_wk.lostpoint,
       result_wk.dsroutename,
       result_wk.c_dscardcompanyname1,
       result_wk.c_dscardcompanyname2,
       result_wk.c_dscardcompanyname3,
       result_wk.c_dscardcompanyname4,
       result_wk.c_dscardcompanyname5,
       result_wk.dsdat10,
       result_wk.dsdat11,
       result_wk.dsdat12,
       result_wk.maintel,
       result_wk.dsdat60,
       result_wk.dsdat62,
       result_wk.disecessionflg,
       result_wk.dielimflg,
       case 
           when tel5.mainTel is not null then '16'   
           when outcall_tel.diusrid is not null then '17'
           else '0'
       end   excflg
    FROM  tbOutcallResult_wk result_wk 
    LEFT JOIN tel5
        ON result_wk.mainTel = tel5.mainTel
    LEFT JOIN tbOutcallTel outcall_tel
        ON result_wk.diusrid = outcall_tel.diusrid AND  outcall_tel.pnt = '0' AND outcall_tel.excflg <> '0'
    LEFT JOIN latest_stage
        ON result_wk.diusrid = latest_stage.usrid
),
final as (
    select
        diusrid::number(38,0)    as diusrid,
        dsname::varchar(30)    as dsname,
        dsname2::varchar(30)    as dsname2,
        dskana::varchar(30)    as dskana,
        dskana2::varchar(30)    as dskana2,
        dstel::varchar(24)    as dstel,
        dsdat2::varchar(6000)    as dsdat2,
        dsdat3::varchar(6000)    as dsdat3,
        dsdat4::varchar(6000)    as dsdat4,
        dszip::varchar(15)    as dszip,
        dspref::varchar(15)    as dspref,
        address::varchar(192)    as address,
        dspromname::varchar(256)    as dspromname,
        dsbirthday::varchar(28)    as dsbirthday,
        dsdat9::varchar(6000)    as dsdat9,
        c_dsdmname::varchar(210)    as c_dsdmname,
        stage::varchar(6000)    as stage,
        age::number(38,0)    as age,
        dsseibetsu::varchar(1)    as dsseibetsu,
        dipoint::number(38,0)    as dipoint,
        c_diissuepoint::number(38,0)    as c_diissuepoint,
        c_disumusepoint::number(38,0)    as c_disumusepoint,
        c_dspointlimitdate::timestamp_ntz(9)    as c_dspointlimitdate,
        lostpoint::number(38,0)    as lostpoint,
        dsroutename::varchar(32)    as dsroutename,
        c_dscardcompanyname1::varchar(64)    as c_dscardcompanyname1,
        c_dscardcompanyname2::varchar(64)    as c_dscardcompanyname2,
        c_dscardcompanyname3::varchar(64)    as c_dscardcompanyname3,
        c_dscardcompanyname4::varchar(64)    as c_dscardcompanyname4,
        c_dscardcompanyname5::varchar(64)    as c_dscardcompanyname5,
        dsdat10::varchar(6000)    as dsdat10,
        dsdat11::varchar(6000)    as dsdat11,
        dsdat12::varchar(6000)    as dsdat12,
        maintel::varchar(6000)    as maintel,
        dsdat60::varchar(6000)    as dsdat60,
        dsdat62::varchar(6000)    as dsdat62,
        disecessionflg::varchar(1)    as disecessionflg,
        dielimflg::varchar(1)    as dielimflg,
        excflg::varchar(2)    as excflg
    from transformed
)
select * from final
