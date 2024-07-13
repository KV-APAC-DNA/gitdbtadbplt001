{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as(
    select * from {{ source('indsdl_raw', 'sdl_in_retailer') }} 
),
final as(
    select 
        cmpcode as cmpcode,
        distcode as distcode,
        distrbrcode as distrbrcode,
        rtrcode as rtrcode,
        rtrtype as rtrtype,
        rtrname as rtrname,
        rtruniquecode as rtruniquecode,
        channelcode as channelcode,
        retlrgroupcode as retlrgroupcode,
        classcode as classcode,
        rtrphoneno as rtrphoneno,
        rtrcontactperson as rtrcontactperson,
        emailid as emailid,
        regdate as regdate,
        rtrlicno as rtrlicno,
        rtrlicexpirydate as rtrlicexpirydate,
        druglno as druglno,
        rtrdrugexpirydate as rtrdrugexpirydate,
        rtrcrbills as rtrcrbills,
        rtrcrdays as rtrcrdays,
        rtrcrlimit as rtrcrlimit,
        relationstatus as relationstatus,
        parentcode as parentcode,
        status as status,
        rtrlatitude as rtrlatitude,
        rtrlongitude as rtrlongitude,
        csrtrcode as csrtrcode,
        keyaccount as keyaccount,
        rtrfoodlicno as rtrfoodlicno,
        pannumber as pannumber,
        retailertype as retailertype,
        composite as composite,
        relatedparty as relatedparty,
        statename as statename,
        lastmoddate as lastmoddate,
        createddt as createddt,
        filename as filename,
        run_id as run_id,
        crt_dttm as crt_dttm
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
)

select * from final