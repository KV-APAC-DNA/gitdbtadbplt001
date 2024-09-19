with source as
(
    select * from {{ source('ntasdl_raw', 'sdl_tw_strategic_cust_hier') }}
),
final as 
(
    select 
    strategy_customer_hierachy_code::varchar(10) as strategy_customer_hierachy_code,
	strategy_customer_hierachy_name::varchar(255) as strategy_customer_hierachy_name,
	cust_cd::varchar(10) as cust_cd,
	cust_nm::varchar(255) as cust_nm,
	filename::varchar(100) as filename,
	run_id::varchar(20) as run_id,
	load_date::timestamp_ntz(9) as load_date
    from source
)
select * from final