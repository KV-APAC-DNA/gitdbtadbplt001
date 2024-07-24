with ocl_in_userlist_v as (
    select * from dev_dna_core.jpdcledw_integration.OCL_in_userlist_v
),
c_tbmembunitrel as (
    select * from dev_dna_core.jpdclitg_integration.c_tbmembunitrel
),
tbusrpram as (
    select * from dev_dna_core.jpdclitg_integration.tbUsrPram
),

tbusrpram_withdates as (
    select 
        *,
        TO_CHAR(current_timestamp(),'YYYYMMDD')::integer as today,
        TO_CHAR(TO_DATE(DSBIRTHDAY,'YYYY-MM-DD HH24:MI:SS'),'YYYYMMDD')::integer as dob
    from tbUsrPram
),
user_attributes as (
    SELECT 
        Usr_attr.diusrid as diusrid,
        Usr_attr.dsname as dsname,
        Usr_attr.dsname2 as dsname2,
        Usr_attr.dskana as dskana,
        Usr_attr.dskana2 as dskana2,
        Usr_attr.dsTel as dstel,
        Usr_attr.dsdat2 as dsdat2,
        Usr_attr.dsdat3 as dsdat3,
        Usr_attr.dsdat4 as dsdat4,
        Usr_attr.dsZip as dszip,
        Usr_attr.dsPref as dspref,
        '' as  address,
        '' as  dspromname,
        Usr_attr.dsBirthDay as dsbirthday,
        Usr_attr.dsdat9 as dsdat9,
        '' as  c_dsdmname,
        '' as  stage,
        {{getnenrei("Usr_attr.today", "Usr_attr.dob") }} as age,
        Usr_attr.dsSeibetsu as  dsseibetsu,
        NVL(Usr_attr.dipoint, 0) as dipoint,
        null::integer as   c_diissuepoint,
        null::integer as   c_disumusepoint,
        null::date as   c_dspointlimitdate,
        null::integer as   lostpoint,
        '' as dsroutename,
        '' as c_dscardcompanyname1,
        '' as c_dscardcompanyname2,
        '' as c_dscardcompanyname3,
        '' as c_dscardcompanyname4,
        '' as c_dscardcompanyname5,
        Usr_attr.dsdat10 as dsdat10,
        Usr_attr.dsdat11 as dsdat11,
        Usr_attr.dsdat12 as dsdat12,
        CASE Usr_attr.dsdat4
            WHEN '固定' THEN dsTel
            WHEN '携帯' THEN dsdat2
            ELSE Usr_attr.dsdat3
        END maintel,
        Usr_attr.dsdat60 as dsdat60,
        Usr_attr.dsdat62 as dsdat62,
        Usr_attr.disecessionflg as disecessionflg,
        Usr_attr.dielimflg as dielimflg,
        '0' as excflg
    FROM  OCL_in_userlist_v Usr_list
    INNER JOIN tbusrpram_withdates  Usr_attr
        ON Usr_list.diUsrId = Usr_attr.diusrid
),
parent_user_attributes as (
    SELECT 
        usr_attr.diusrid as diusrid,
        usr_attr.dsname as dsname,
        usr_attr.dsname2 as dsname2,
        usr_attr.dskana as dskana,
        usr_attr.dskana2 as dskana2,
        usr_attr.dsTel as dstel,
        usr_attr.dsdat2 as dsdat2,
        usr_attr.dsdat3 as dsdat3,
        usr_attr.dsdat4 as dsdat4,
        Usr_attr.dsZip as dszip,
        usr_attr.dsPref as dspref,
        ''  as address,
        ''  as dspromname,
        Usr_attr.dsBirthDay as dsbirthday,
        Usr_attr.dsdat9 as dsdat9,
        ''  as c_dsdmname,
        ''  as stage,
        {{ getnenrei("Usr_attr.today", "Usr_attr.dob") }}  as age,
        Usr_attr.dsSeibetsu  as dsseibetsu,
        NVL(Usr_attr.dipoint, 0) as dipoint,
        null::integer as c_diissuepoint,
        null::integer as c_disumusepoint,
        null  as c_dspointlimitdate,
        null::integer as lostpoint,
        '' as dsroutename,
        ''  as c_dscardcompanyname1,
        ''  as c_dscardcompanyname2,
        ''  as c_dscardcompanyname3,
        ''  as c_dscardcompanyname4,
        ''  as c_dscardcompanyname5,
        usr_attr.dsdat10 as dsdat10,
        usr_attr.dsdat11 as dsdat11,
        usr_attr.dsdat12 as dsdat12,
        CASE Usr_attr.dsdat4
            WHEN '固定' THEN dsTel
            WHEN '携帯' THEN dsdat2
            ELSE Usr_attr.dsdat3
        END maintel,
        usr_attr.dsdat60 as dsdat60,
        usr_attr.dsdat62 as dsdat62,
        usr_attr.disecessionflg as disecessionflg,
        usr_attr.dielimflg as dielimflg,
        '0' as excflg
  FROM OCL_in_userlist_v Usr_list 
 INNER JOIN c_tbmembunitrel nayose
    ON Usr_list.diUsrId = nayose.c_diChildUsrid 
   AND nayose.dielimflg = '0'
 INNER JOIN tbusrpram_withdates usr_attr
    ON nayose.c_diParentUsrid = usr_attr.diusrid
),
transformed as (
    select * from user_attributes
    union all
    select * from parent_user_attributes
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
    