{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                delete from {{this}}  where dirouteid in (select dirouteid from {{ source('jpdclsdl_raw', 'tbecsalesroutemst') }} ) ;
                    {% endif %}"
    )
}}

with source as(
    select * from {{ source('jpdclsdl_raw', 'tbecsalesroutemst') }}
),
final as(
    select 
        dirouteid::number(18,0) as dirouteid,
        dsroutename::varchar(48) as dsroutename,
        dsmemo::varchar(192) as dsmemo,
        didisableflg::varchar(1) as didisableflg,
        dsprep::timestamp_ntz(9) as dsprep,
        dsren::timestamp_ntz(9) as dsren,
        dselim::timestamp_ntz(9) as dselim,
        diprepusr::number(38,0) as diprepusr,
        direnusr::number(38,0) as direnusr,
        dielimusr::number(38,0) as dielimusr,
        dielimflg::varchar(1) as dielimflg,
        c_dskikanruikeipritekiyoflg::varchar(1) as c_dskikanruikeipritekiyoflg,
        c_dsservicetargetflg::varchar(1) as c_dsservicetargetflg,
        c_dsscreenselectflg::varchar(1) as c_dsscreenselectflg,
        c_distockchanelid::number(38,0) as c_distockchanelid,
        c_dipointchanelid::number(38,0) as c_dipointchanelid,
        c_dsroutenameryaku::varchar(24) as c_dsroutenameryaku,
        c_dsstoreflg::varchar(1) as c_dsstoreflg,
        disortid::number(38,0) as disortid,
        NULL::varchar(10) as source_file_date,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        NULL::varchar(10) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        NULL::varchar(100) as updated_by
    from source
)
select * from final