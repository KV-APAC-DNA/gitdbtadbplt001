{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                delete from {{this}} where disokoid in (select disokoid from {{ source('jpndclsdl_raw', 'tbecsoko') }});
                    {% endif %}"
    )
}}

with source as(
    select * from {{ source('jpndclsdl_raw', 'tbecsoko') }}
),
final as(
    select 
        disokoid::number(38,0) as disokoid,
        dssokoname::varchar(48) as dssokoname,
        dssokonameryaku::varchar(24) as dssokonameryaku,
        current_timestamp()::timestamp_ntz(9) as dsprep,
        current_timestamp()::timestamp_ntz(9) as dsren,
        dselim::timestamp_ntz(9) as dselim,
        diprepusr::number(38,0) as diprepusr,
        direnusr::number(38,0) as direnusr,
        dielimusr::number(38,0) as dielimusr,
        dielimflg::varchar(1) as dielimflg,
        c_dsshipmenttargetflg::varchar(1) as c_dsshipmenttargetflg,
        NULL::varchar(10) as source_file_date,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        NULL::varchar(10) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        NULL::varchar(100) as updated_by
    from source
)
select * from final