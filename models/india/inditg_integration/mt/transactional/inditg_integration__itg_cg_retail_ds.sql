{{
    config(
        materialized= "incremental",
        incremental_strategy= "merge",
        unique_key= ["month","year","customer_code","product_code"],
        merge_exclude_columns= ["crt_dttm"]
    )
}}
with 
wks_cg_retail_ds as 
(
 select * from {{ ref('indwks_integration__wks_cg_retail_ds') }}
),
final as 
(
    select 
	month::number(18,0) as month,
	year::number(18,0) as year,
	customer_code::varchar(50) as customer_code,
	product_code::varchar(50) as product_code,
	retail_quantity::number(18,3) as retail_quantity,
	retail_value::number(18,3) as retail_value,
	wt_ret_qty::number(38,11) as wt_ret_qty,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm,
    file_name::varchar(255) as file_name
    from wks_cg_retail_ds
)
select * from final