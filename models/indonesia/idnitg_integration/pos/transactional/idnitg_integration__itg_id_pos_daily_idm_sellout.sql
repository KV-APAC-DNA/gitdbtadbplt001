{{
    config
    (
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key= ["yearmonth"]
    )
}}
with source as 
(
    select * from {{ source('idnsdl_raw', 'sdl_id_pos_daily_idm_sellout') }}
),


final as
(
    select
    account::varchar(250) as account,
	kode_branch::varchar(250) as kode_branch,
	branch_name::varchar(250) as branch_name,
	tgl::varchar(250) as tgl,
	plu::varchar(250) as plu,
	descp::varchar(250) as descp,
	type::varchar(10) as type,
	value::number(18,2) as value,
	pos_cust::varchar(50) as pos_cust,
	yearmonth::varchar(10) as yearmonth,
	run_id::number(14,0) as run_id,
	filename::varchar(100) as filename,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) as crtd_dttm 
    from source 
)
select * from final