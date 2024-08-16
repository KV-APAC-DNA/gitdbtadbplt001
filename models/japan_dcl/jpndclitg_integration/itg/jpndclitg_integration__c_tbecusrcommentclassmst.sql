{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
            delete from {{this}} where c_dsusrcommentclasskbn in (select c_dsusrcommentclasskbn from {{ source('jpdclsdl_raw', 'c_tbecusrcommentclassmst') }});
                    {% endif %}"
    )
}}

with source as(
    select * from {{ source('jpdclsdl_raw', 'c_tbecusrcommentclassmst') }}
),
final as(
    select 
        c_dsusrcommentclasskbn::varchar(3) as c_dsusrcommentclasskbn,
        c_dsusrcommentclassname::varchar(96) as c_dsusrcommentclassname,
        disortid::number(38,0) as disortid,
        current_timestamp()::timestamp_ntz(9) as dsprep,
        current_timestamp()::timestamp_ntz(9) as dsren,
        dselim::timestamp_ntz(9) as dselim,
        diprepusr::number(38,0) as diprepusr,
        direnusr::number(38,0) as direnusr,
        dielimusr::number(38,0) as dielimusr,
        dielimflg::varchar(4) as dielimflg,
        NULL::varchar(10) as source_file_date,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        NULL::varchar(10) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        NULL::varchar(100) as updated_by
    from source
)
select * from final