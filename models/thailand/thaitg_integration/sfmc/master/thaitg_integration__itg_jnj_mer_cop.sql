{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['file_name']
    )
}}
with source as(
    select * from {{ source('thasdl_raw', 'sdl_jnj_mer_cop') }}
),
final as(
    select
        cop_date::varchar(255) as cop_date,
        emp_address_pc::varchar(255) as emp_address_pc,
        pc_name::varchar(255) as pc_name,
        survey_name::varchar(255) as survey_name,
        emp_address_supervisor::varchar(255) as emp_address_supervisor,
        supervisor_name::varchar(255) as supervisor_name,
        cop_priority::varchar(255) as cop_priority,
        start_date::varchar(255) as start_date,
        end_date::varchar(255) as end_date,
        area::varchar(255) as area,
        channel::varchar(255) as channel,
        account::varchar(255) as account,
        store_id::varchar(255) as store_id,
        store_name::varchar(255) as store_name,
        question::varchar(500) as question,
        answer::varchar(255) as answer,
        run_id::number(14,0)as run_id,
        file_name::varchar(255) as file_name,
        yearmo::varchar(255) as yearmo,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        category::varchar(255) as category,
        brand::varchar(255) as brand
  from source
)
select * from final