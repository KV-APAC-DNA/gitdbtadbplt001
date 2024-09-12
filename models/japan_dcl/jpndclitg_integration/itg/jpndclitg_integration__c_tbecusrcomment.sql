{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                delete from {{this}} where c_diusrcommentid in (select c_diusrcommentid from {{ source('jpdclsdl_raw', 'c_tbecusrcomment') }});
                    {% endif %}"
    )
}}

with source as(
    select * from {{ source('jpdclsdl_raw', 'c_tbecusrcomment') }}
),
final as(
    select 
        c_diusrcommentid::number(38,0) as c_diusrcommentid,
        diecusrid::number(38,0) as diecusrid,
        c_dsusrcommentdate::timestamp_ntz(9) as c_dsusrcommentdate,
        c_dsusrcommentclasskbn::varchar(30) as c_dsusrcommentclasskbn,
        c_dsusrcomment::varchar(3000) as c_dsusrcomment,
        dsprep::timestamp_ntz(9) as dsprep,
        dsren::timestamp_ntz(9) as dsren,
        dselim::TIMESTAMP_NTZ(9) as dselim,
        diprepusr::number(38,0) as diprepusr,
        direnusr::number(38,0) as direnusr,
        dielimusr::number(38,0) as dielimusr,
        dielimflg::varchar(30) as dielimflg,
        source_file_date::varchar(10) as source_file_date,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        inserted_by::varchar(10) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        updated_by::varchar(100) as updated_by
    from source
)
select * from final