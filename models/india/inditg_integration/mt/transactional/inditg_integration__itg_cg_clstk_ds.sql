{{
    config(
        materialized= "incremental",
        incremental_strategy= "merge",
        unique_key= ["month","year","customer_code","product_code","cls_stock_qty","cl_stck_value"],
        merge_exclude_columns= ["crt_dttm"]
    )
}}
with 
wks_cg_clstk_ds as 
(
 select * from {{ ref('indwks_integration__wks_cg_clstk_ds') }}
),
final as 
(
    select 
    month::number(18,0) as month,
	year::number(18,0) as year,
	customer_code::varchar(50) as customer_code,
	product_code::varchar(50) as product_code,
	cls_stock_qty::number(18,3) as cls_stock_qty,
	cl_stck_value::number(18,3) as cl_stck_value,
	wt_stock_qty::number(38,11) as wt_stock_qty,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm
    
    from wks_cg_clstk_ds
)
select * from final