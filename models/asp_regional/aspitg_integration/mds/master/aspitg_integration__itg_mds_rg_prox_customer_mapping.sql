with source as
(
    select * from {{ source('aspsdl_raw', 'sdl_mds_rg_prox_customer_mapping') }}
),
final as
(
    SELECT 	code
	,prox_cust_code
	,prox_cust_name
	,local_cust_code
	,local_cust_name
	,market 
    ,category
    ,convert_timezone('UTC',current_timestamp()) as updt_dt  FROM
    source
)
select code::varchar(500) as code,
    prox_cust_code::varchar(200) as prox_cust_code,
    prox_cust_name::varchar(200) as prox_cust_name,
    local_cust_code::varchar(200) as local_cust_code,
    local_cust_name::varchar(200) as local_cust_name,
    market::varchar(200) as market,
    category::varchar(200) as category,
    updt_dt::timestamp_ntz(9) as updt_dt
 from final
