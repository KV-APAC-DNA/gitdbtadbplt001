{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['objective_key'],
        pre_hook= "{%if is_incremental()%}
        delete from {{this}} where nvl(objective_key, '#') in (select distinct nvl(objective_key, '#') from {{ source('pcfsdl_raw', 'sdl_perenso_call_objectives') }} 
        where file_name not in (
        select distinct file_name from {{source('pcfwks_integration','TRATBL_sdl_perenso_call_objectives__null_test')}}
        ));{%endif%}"
    )
}}
with source as 
(
    select *, dense_rank() over (partition by objective_key order by file_name desc) rnk
    from {{ source('pcfsdl_raw', 'sdl_perenso_call_objectives') }}
    where file_name not in (
        select distinct file_name from {{source('pcfwks_integration','TRATBL_sdl_perenso_call_objectives__null_test')}}
        ) qualify rnk = 1
),
final as 
(
    select 
    objective_key::varchar(50) as objective_key,
	to_date(try_to_timestamp_ntz(call_created_dt,'dd/mm/yyyy hh12:mi:ss am')) as call_created_dt,
	replace(trim(description),chr(10),'')::varchar(255) as description,
	acct_key::varchar(50) as acct_key,
	assigned_user_key::varchar(50) as assigned_user_key,
	created_user_key::varchar(50) as created_user_key,
	to_date(try_to_timestamp_ntz(due_dt,'dd/mm/yyyy hh12:mi:ss am')) as due_dt,
	to_date(try_to_timestamp_ntz(completed_dt,'dd/mm/yyyy hh12:mi:ss am')) as completed_dt,
	status::varchar(50) as status,
	run_id::number(14,0) as run_id,
	current_timestamp()::timestamp_ntz(9) as create_dt,
	current_timestamp()::timestamp_ntz(9) as update_dt,
    file_name::varchar(255) as file_name
    from source
)
select * from final