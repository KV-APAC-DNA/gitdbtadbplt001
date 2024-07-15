{{
    config
    (
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as
(
    select * from {{source('indsdl_raw', 'sdl_csl_retailermaster')}}
),
final as
(
    select 
        distcode::varchar(50) as distcode,
        rtrid::number(18,0) as rtrid,
        rtrcode::varchar(50) as rtrcode,
        rtrname::varchar(100) as rtrname,
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
        rtrlatitude::varchar(40) as rtrlatitude,
        rtrlongitude::varchar(40) as rtrlongitude,
        rtruniquecode::varchar(100) as rtruniquecode,
        run_id::number(14,0) as run_id,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        file_name::varchar(50) as file_name
    from source
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %})
select * from final
