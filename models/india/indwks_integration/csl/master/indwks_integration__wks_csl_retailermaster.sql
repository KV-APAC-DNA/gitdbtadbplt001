{{
    config
    (
        pre_hook= '{{build_itg_retailermaster_temp()}}'
    )
}}

with sdl_csl_retailermaster as 
(
    select * from {{source('indsdl_raw', 'sdl_csl_retailermaster')}}
),
itg_retailermaster as
(
    select * from {{ source('inditg_integration', 'itg_retailermaster') }}
),
wks_tmp_date as 
(
    select getdate() as currtime
),
transformed as
(
  SELECT
    src.distcode,
    rtrid,
    src.rtrcode,
    src.rtrname src_rtrname,
    tgt.rtrname tgt_rtrname,
    csrtrcode,
    rtrcatlevelid,
    rtrcategorycode,
    classcode,
    keyaccount,
    regdate,
    relationstatus,
    parentcode,
    geolevel,
    geolevelvalue,
    STATUS,
    createdid,
    createddate,
    rtraddress1,
    rtraddress2,
    rtraddress3,
    rtrpincode,
    villageid,
    villagecode,
    villagename,
    mode,
    uploadflag,
    approvalremarks,
    syncid,
    druglno,
    rtrcrbills,
    rtrcrlimit,
    rtrcrdays,
    rtrdayoff,
    rtrtinno,
    rtrcstno,
    rtrlicno,
    rtrlicexpirydate,
    rtrdrugexpirydate,
    rtrpestlicno,
    rtrpestexpirydate,
    approved,
    rtrphoneno,
    rtrcontactperson,
    rtrtaxgroup,
    rtrtype,
    rtrtaxable,
    rtrshippadd1,
    rtrshippadd2,
    rtrshippadd3,
    rtrfoodlicno,
    rtrfoodexpirydate,
    rtrfoodgracedays,
    rtrdruggracedays,
    rtrcosmeticlicno,
    rtrcosmeticexpirydate,
    rtrcosmeticgracedays,
    CASE 
        WHEN tgt.crt_dttm IS NULL
        THEN tmpdt.currtime
        WHEN tgt.crt_dttm IS NOT NULL
        AND src.rtrname <> tgt.rtrname
        THEN tgt.crt_dttm
        ELSE tgt.crt_dttm
        END AS crt_dttm,
    CASE 
        WHEN tgt.crt_dttm IS NULL
        THEN NULL
        WHEN tgt.crt_dttm IS NOT NULL
        AND src.rtrname <> tgt.rtrname
        THEN tmpdt.currtime
        ELSE tmpdt.currtime
        END AS updt_dttm,
    CASE 
        WHEN tgt.crt_dttm IS NULL
        THEN 'I'
        WHEN tgt.crt_dttm IS NOT NULL
        AND src.rtrname <> tgt.rtrname
        THEN 'U2'
        ELSE 'U'
        END AS chng_flg,
    CASE 
        WHEN tgt.crt_dttm IS NULL
        THEN 'Y'
        WHEN tgt.crt_dttm IS NOT NULL
        AND src.rtrname <> tgt.rtrname
        THEN 'N'
        ELSE 'Y'
        END AS actv_flg,
    RtrLatitude,
    RtrLongitude,
    RtrUniquecode FROM (
    SELECT DISTINCT distcode,
        rtrid,
        rtrcode,
        rtrname,
        csrtrcode,
        rtrcatlevelid,
        rtrcategorycode,
        classcode,
        keyaccount,
        regdate,
        relationstatus,
        parentcode,
        geolevel,
        geolevelvalue,
        STATUS,
        createdid,
        createddate,
        rtraddress1,
        rtraddress2,
        rtraddress3,
        rtrpincode,
        villageid,
        villagecode,
        villagename,
        mode,
        uploadflag,
        approvalremarks,
        syncid,
        druglno,
        rtrcrbills,
        rtrcrlimit,
        rtrcrdays,
        rtrdayoff,
        rtrtinno,
        rtrcstno,
        rtrlicno,
        rtrlicexpirydate,
        rtrdrugexpirydate,
        rtrpestlicno,
        rtrpestexpirydate,
        approved,
        rtrphoneno,
        rtrcontactperson,
        rtrtaxgroup,
        rtrtype,
        rtrtaxable,
        rtrshippadd1,
        rtrshippadd2,
        rtrshippadd3,
        rtrfoodlicno,
        rtrfoodexpirydate,
        rtrfoodgracedays,
        rtrdruggracedays,
        rtrcosmeticlicno,
        rtrcosmeticexpirydate,
        rtrcosmeticgracedays,
        crt_dttm,
        RtrLatitude,
        RtrLongitude,
        RtrUniquecode
    --row_number() over ( partition by distcode, rtrcode order by createddate desc, rtrname) as rn
    FROM SDL_CSL_RETAILERMASTER a --) b
        --Where b.rn=1
    ) src LEFT OUTER JOIN (
    SELECT distcode,
        rtrcode,
        rtrname,
        crt_dttm,
        actv_flg
  FROM ITG_RETAILERMASTER
  ) tgt ON src.distcode = tgt.distcode
  AND src.rtrcode = tgt.rtrcode
  AND tgt.actv_flg = 'Y' CROSS JOIN WKS_TMP_DATE tmpdt
),
final as 
(
    select
        distcode::varchar(50) as distcode,
        rtrid::number(18,0) as rtrid,
        rtrcode::varchar(50) as rtrcode,
        src_rtrname::varchar(100) as src_rtrname,
        tgt_rtrname::varchar(100) as tgt_rtrname,
        csrtrcode::varchar(50) as csrtrcode,
        rtrcatlevelid::varchar(30) as rtrcatlevelid,
        rtrcategorycode::varchar(50) as rtrcategorycode,
        classcode::varchar(50) as classcode,
        keyaccount::varchar(50) as keyaccount,
        regdate::timestamp_ntz(9) as regdate,
        relationstatus::varchar(50) as relationstatus,
        parentcode::varchar(50) as parentcode,
        geolevel::varchar(50) as geolevel,
        geolevelvalue::varchar(100) as geolevelvalue,
        status::number(18,0) as status,
        createdid::number(18,0) as createdid,
        createddate::timestamp_ntz(9) as createddate,
        rtraddress1::varchar(100) as rtraddress1,
        rtraddress2::varchar(100) as rtraddress2,
        rtraddress3::varchar(100) as rtraddress3,
        rtrpincode::varchar(20) as rtrpincode,
        villageid::number(18,0) as villageid,
        villagecode::varchar(100) as villagecode,
        villagename::varchar(100) as villagename,
        mode::varchar(100) as mode,
        uploadflag::varchar(10) as uploadflag,
        approvalremarks::varchar(400) as approvalremarks,
        syncid::number(38,0) as syncid,
        druglno::varchar(100) as druglno,
        rtrcrbills::number(18,0) as rtrcrbills,
        rtrcrlimit::number(38,6) as rtrcrlimit,
        rtrcrdays::number(18,0) as rtrcrdays,
        rtrdayoff::number(18,0) as rtrdayoff,
        rtrtinno::varchar(100) as rtrtinno,
        rtrcstno::varchar(100) as rtrcstno,
        rtrlicno::varchar(100) as rtrlicno,
        rtrlicexpirydate::varchar(100) as rtrlicexpirydate,
        rtrdrugexpirydate::varchar(100) as rtrdrugexpirydate,
        rtrpestlicno::varchar(100) as rtrpestlicno,
        rtrpestexpirydate::varchar(100) as rtrpestexpirydate,
        approved::number(18,0) as approved,
        rtrphoneno::varchar(100) as rtrphoneno,
        rtrcontactperson::varchar(100) as rtrcontactperson,
        rtrtaxgroup::varchar(100) as rtrtaxgroup,
        rtrtype::varchar(20) as rtrtype,
        rtrtaxable::varchar(1) as rtrtaxable,
        rtrshippadd1::varchar(200) as rtrshippadd1,
        rtrshippadd2::varchar(200) as rtrshippadd2,
        rtrshippadd3::varchar(200) as rtrshippadd3,
        rtrfoodlicno::varchar(200) as rtrfoodlicno,
        rtrfoodexpirydate::timestamp_ntz(9) as rtrfoodexpirydate,
        rtrfoodgracedays::number(18,0) as rtrfoodgracedays,
        rtrdruggracedays::number(18,0) as rtrdruggracedays,
        rtrcosmeticlicno::varchar(200) as rtrcosmeticlicno,
        rtrcosmeticexpirydate::timestamp_ntz(9) as rtrcosmeticexpirydate,
        rtrcosmeticgracedays::number(18,0) as rtrcosmeticgracedays,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        chng_flg::varchar(2) as chng_flg,
        actv_flg::varchar(1) as actv_flg,
        rtrlatitude::varchar(40) as rtrlatitude,
        rtrlongitude::varchar(40) as rtrlongitude,
        rtruniquecode::varchar(100) as rtruniquecode
    from transformed
)
select * from final
