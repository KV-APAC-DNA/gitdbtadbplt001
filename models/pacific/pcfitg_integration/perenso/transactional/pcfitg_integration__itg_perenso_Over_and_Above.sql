{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['over_and_above_key','acct_key','prod_grp_key'],
        pre_hook= "delete from {{this}} where (over_and_above_key,acct_key,prod_grp_key) in (select distinct over_and_above_key,acct_key,prod_grp_key from {{ source('pcfsdl_raw', 'sdl_perenso_over_and_above') }});"
    )
}}
with source as 
(
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_over_and_above') }}
),
final as 
(
    select 
    over_and_above_key::number(10,0) as over_and_above_key,
	acct_key::number(10,0) as acct_key,
	todo_option_key::number(10,0) as todo_option_key,
	prod_grp_key::number(10,0) as prod_grp_key,
	activated::varchar(10) as activated,
	notes::varchar(255) as notes,
	run_id::number(14,0) as run_id,
	current_timestamp()::timestamp_ntz(9) as create_dt,
	current_timestamp()::timestamp_ntz(9) as update_dt 
    from source
)
select * from final