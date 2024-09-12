{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                delete from {{this}} where c_dstelcompanycd in (select c_dstelcompanycd from {{ source('jpdclsdl_raw', 'c_tbtelcompanymst') }});
                    {% endif %}"
    )
}}

with source as(
    select * from {{ source('jpdclsdl_raw', 'c_tbtelcompanymst') }}
),
final as(
    select 
        c_dstelcompanycd::varchar(4) as c_dstelcompanycd,
        c_dstelcompayname::varchar(96) as c_dstelcompayname,
        didisporder::number(38,0) as didisporder,
        dsprep::timestamp_ntz(9) as dsprep,
        dsren::timestamp_ntz(9) as dsren,
        dselim::varchar(28) as dselim,
        diprepusr::number(38,0) as diprepusr,
        direnusr::number(38,0) as direnusr,
        dielimusr::number(38,0) as dielimusr,
        dielimflg::varchar(4) as dielimflg,
        source_file_date::varchar(10) as source_file_date,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        inserted_by::varchar(10) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        updated_by::varchar(100) as updated_by
    from source
)
select * from final