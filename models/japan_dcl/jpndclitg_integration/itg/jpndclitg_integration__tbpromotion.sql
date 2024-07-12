{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                delete from {{this}} where dipromid in (select dipromid from {{ source('jpdclsdl_raw', 'tbpromotion') }});
                    {% endif %}"
    )
}}

with source as(
    select * from {{ source('jpdclsdl_raw', 'tbpromotion') }}
),
final as(
    select 
        dipromid::number(38,0) as dipromid,
        dipromcateid::number(38,0) as dipromcateid,
        dspromname::varchar(384) as dspromname,
        dspromcode::varchar(7) as dspromcode,
        dipromregistflg::varchar(1) as dipromregistflg,
        dipromenqflg::varchar(1) as dipromenqflg,
        dipromorderflg::varchar(1) as dipromorderflg,
        dipromdivsts::varchar(1) as dipromdivsts,
        dsvalidfrom::varchar(28) as dsvalidfrom,
        dsvalidto::varchar(28) as dsvalidto,
        dspcurl::varchar(1536) as dspcurl,
        dsmburl::varchar(1536) as dsmburl,
        diinvalidsts::varchar(1) as diinvalidsts,
        dspcinvalidurl::varchar(1536) as dspcinvalidurl,
        dsmbinvalidurl::varchar(1536) as dsmbinvalidurl,
        c_diaffiliatekubun::number(38,0) as c_diaffiliatekubun,
        c_dsorderendtag::varchar(6000) as c_dsorderendtag,
        c_dskoukokuhi::varchar(384) as c_dskoukokuhi,
        c_dsdistributenum::varchar(384) as c_dsdistributenum,
        c_dipublishkubun::varchar(3) as c_dipublishkubun,
        c_dipromcompanyid::number(38,0) as c_dipromcompanyid,
        c_didisppriority::number(38,0) as c_didisppriority,
        c_dicampaignid::number(38,0) as c_dicampaignid,
        c_diredirectflg::varchar(1) as c_diredirectflg,
        dsunredirectsts::varchar(1) as dsunredirectsts,
        dsunredirecturlpc::varchar(1536) as dsunredirecturlpc,
        dsunredirecturlmobile::varchar(1536) as dsunredirecturlmobile,
        dsprep::varchar(28) as dsprep,
        dsren::varchar(28) as dsren,
        diprepusr::number(38,0) as diprepusr,
        direnusr::number(38,0) as direnusr,
        dielimflg::varchar(1) as dielimflg,
        c_csid::varchar(192) as c_csid,
        c_dipcviewflg::varchar(1) as c_dipcviewflg,
        c_dideptid::number(38,0) as c_dideptid,
        NULL::varchar(10) as source_file_date,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        NULL::varchar(10) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        NULL::varchar(100) as updated_by
    from source
)
select * from final