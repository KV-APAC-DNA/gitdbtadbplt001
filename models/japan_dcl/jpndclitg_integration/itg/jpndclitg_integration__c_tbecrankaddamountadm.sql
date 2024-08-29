{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                delete from {{this}} where c_diaddadmid in (select c_diaddadmid from {{ source('jpdclsdl_raw', 'c_tbecrankaddamountadm') }}) ;
                    {% endif %}"
    )
}}

with source as(
    select * from {{ source('jpdclsdl_raw', 'c_tbecrankaddamountadm') }}
),
final as(
    select 
        c_diaddadmid::number(38,0) as c_diaddadmid,
        diecusrid::number(38,0) as diecusrid,
        dsorderdt::timestamp_ntz(9) as dsorderdt,
        c_dsrankaddprc::number(38,0) as c_dsrankaddprc,
        dsprep::timestamp_ntz(9) as dsprep,
        dsren::timestamp_ntz(9) as dsren,
        diprepusr::number(38,0) as diprepusr,
        direnusr::number(38,0) as direnusr,
        dielimflg::varchar(1) as dielimflg,
        source_file_date::varchar(10) as source_file_date,
        inserted_date as inserted_date,
        inserted_by::varchar(10) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        updated_by::varchar(100) as updated_by
    from source
)
select * from final