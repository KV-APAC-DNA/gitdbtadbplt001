{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['cust_code','slsper_id','branch_code','ise_id','visit_date']
    )
}}

with source as 
(
    select * from {{ source('vnmsdl_raw','sdl_vn_interface_customer_visited') }}
),

final as
(
    select
    cust_code::varchar(255) as cust_code,
	slsper_id::varchar(255) as slsper_id,
	branch_code::varchar(255) as branch_code,
	ise_id::varchar(255) as ise_id,
	created_date::varchar(255) as created_date,
	visit_date::varchar(255) as visit_date,
	filename::varchar(255) as filename,
	current_timestamp()::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final