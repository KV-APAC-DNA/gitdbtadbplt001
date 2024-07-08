{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = " {% if is_incremental() %}
        delete from {{this}} where case  when ( select count(*) from {{ source('indsdl_raw', 'sdl_in_retailer_route') }} ) > 0 then 1 else 0 end = 1;
        {% endif %}"
    )
}}

with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_in_retailer') }}
),
final as 
(
    select
        cmpcode::varchar(10) as cmpcode,
        distcode::varchar(50) as distcode,
        distrbrcode::varchar(30) as distrbrcode,
        rtrcode::varchar(50) as rtrcode,
        rtrtype::varchar(10) as rtrtype,
        rtrname::varchar(100) as rtrname,
        rtruniquecode::varchar(50) as rtruniquecode,
        channelcode::varchar(30) as channelcode,
        retlrgroupcode::varchar(30) as retlrgroupcode,
        classcode::varchar(20) as classcode,
        rtrphoneno::varchar(20) as rtrphoneno,
        rtrcontactperson::varchar(50) as rtrcontactperson,
        emailid::varchar(50) as emailid,
        to_date(regdate) as regdate,
        rtrlicno::varchar(100) as rtrlicno,
        to_date(rtrlicexpirydate) as rtrlicexpirydate,
        druglno::varchar(100) as druglno,
        to_date(rtrdrugexpirydate) as rtrdrugexpirydate,
        rtrcrbills::number(18,0) as rtrcrbills,
        rtrcrdays::number(18,0) as rtrcrdays,
        rtrcrlimit::number(38,6) as rtrcrlimit,
        relationstatus::varchar(10) as relationstatus,
        parentcode::varchar(50) as parentcode,
        status::varchar(10) as status,
        rtrlatitude::varchar(20) as rtrlatitude,
        rtrlongitude::varchar(20) as rtrlongitude,
        csrtrcode::varchar(50) as csrtrcode,
        keyaccount::varchar(10) as keyaccount,
        rtrfoodlicno::varchar(200) as rtrfoodlicno,
        pannumber::varchar(15) as pannumber,
        retailertype::varchar(10) as retailertype,
        composite::varchar(10) as composite,
        relatedparty::varchar(10) as relatedparty,
        statename::varchar(50) as statename,
        lastmoddate::timestamp_ntz(9) as lastmoddate,
        createddt::timestamp_ntz(9) as createddt,
        filename::varchar(100) as filename,
        run_id::varchar(50) as run_id,
        crt_dttm::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final