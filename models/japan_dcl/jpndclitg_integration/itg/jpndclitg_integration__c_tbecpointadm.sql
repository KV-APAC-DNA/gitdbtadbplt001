{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                delete from {{this}} where c_dipointissueid in (select c_dipointissueid from {{ source('jpdclsdl_raw', 'c_tbecpointadm') }});
                    {% endif %}"
    )
}}

with source as(
    select * from {{ source('jpdclsdl_raw', 'c_tbecpointadm') }}
),
final as(
    select 
        c_dipointissueid::number(38,0) as c_dipointissueid,
        diecusrid::number(38,0) as diecusrid,
        diordercode::varchar(18) as diordercode,
        diorderid::number(38,0) as diorderid,
        c_dikesaiid::number(38,0) as c_dikesaiid,
        dspointkubun::varchar(3) as dspointkubun,
        c_diissuepoint::number(38,0) as c_diissuepoint,
        diremnantpoint::number(38,0) as diremnantpoint,
        c_dspointlimitdate::varchar(12) as c_dspointlimitdate,
        diregistdivcode::varchar(7) as diregistdivcode,
        c_dipointchanelid::number(38,0) as c_dipointchanelid,
        c_dideptid::number(38,0) as c_dideptid,
        dipointcode::varchar(1) as dipointcode,
        divalidflg::varchar(1) as divalidflg,
        dspointmemo::varchar(1728) as dspointmemo,
        current_timestamp()::timestamp_ntz(9) as dspointren,
        c_dsvaliddate::timestamp_ntz(9) as c_dsvaliddate,
        c_dstenpoorderno::varchar(19) as c_dstenpoorderno,
        current_timestamp()::timestamp_ntz(9) as dsprep,
        current_timestamp()::timestamp_ntz(9) as dsren,
        dselim::timestamp_ntz(9) as dselim,
        diprepusr::number(38,0) as diprepusr,
        direnusr::number(38,0) as direnusr,
        dielimusr::number(38,0) as dielimusr,
        dielimflg::varchar(1) as dielimflg,
        NULL::varchar(10) as source_file_date,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        NULL::varchar(10) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        NULL::varchar(100) as updated_by
    from source
)
select * from final