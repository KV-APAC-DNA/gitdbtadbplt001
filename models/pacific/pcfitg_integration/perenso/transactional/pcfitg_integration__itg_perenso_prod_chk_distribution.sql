{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['start_date'],
        pre_hook= "delete from {{this}} where (start_date) in (select distinct to_date(to_varchar(to_date(start_date,'dd/mm/yyyy'),'yyyy-mm-dd'),'yyyy-mm-dd') as start_date from {{ source('pcfsdl_raw', 'sdl_perenso_prod_chk_distribution') }});"
    )
}}
with source as 
(
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_prod_chk_distribution') }}
),
final as 
(
    select 
    acct_key::number(10,0) as acct_key,
	prod_key::number(10,0) as prod_key,
	to_date(to_varchar(to_date(start_date,'dd/mm/yyyy'),'yyyy-mm-dd'),'yyyy-mm-dd') as start_date,
	to_date(to_varchar(to_date(end_date,'dd/mm/yyyy'),'yyyy-mm-dd'),'yyyy-mm-dd') as end_date,
	in_distribution::varchar(5) as in_distribution,
	run_id::number(14,0) as run_id,
	current_timestamp()::timestamp_ntz(9) as create_dt,
	current_timestamp()::timestamp_ntz(9) as update_dt 
    from source
)
select * from final