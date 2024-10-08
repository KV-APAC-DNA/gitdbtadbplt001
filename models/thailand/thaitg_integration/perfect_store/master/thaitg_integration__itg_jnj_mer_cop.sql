{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=["file_name"],
        pre_hook= "delete from {{this}} 
        where file_name in (select distinct file_name 
        from {{ source('thasdl_raw', 'sdl_jnj_mer_cop') }}
        where file_name not in (
                    select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_jnj_mer_cop__null_test') }}
                    union all
                    select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_jnj_mer_cop__test_date_format_odd_eve') }}
                    )
        )"
    )
}}

with source as(
    select *, dense_rank() over(partition by null order by file_name desc) as rnk
    from {{ source('thasdl_raw', 'sdl_jnj_mer_cop') }}
    where file_name not in (
                    select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_jnj_mer_cop__null_test') }}
                    union all
                    select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_jnj_mer_cop__test_date_format_odd_eve') }}
                    ) qualify rnk =1
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