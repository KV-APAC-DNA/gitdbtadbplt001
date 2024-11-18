with source_1 as
(
    select * from {{ source('thasdl_raw', 'sdl_ecom_ams_hb') }}
),
source_2 as
(
	select * from {{ source('thasdl_raw', 'sdl_ecom_ams_jb') }}
),
source_3 as
(
	select * from {{ source('thasdl_raw', 'sdl_ecom_ams_ltr') }}
),
final as
(
select
	order_time::varchar(50) as order_time,
	order_id::varchar(100) as order_id,
	order_completed_time::varchar(50) as order_completed_time,
	verified_status::varchar(50) as verified_status,
	conversion_completed_time::varchar(50) as conversion_completed_time,
	brand_commission::number(20,4) as brand_commission,
	item_id::varchar(50) as item_id,
	item_name::varchar(500) as item_name,
	model_id::varchar(50) as model_id,
	price::number(20,4) as price,
	promotion_id::varchar(100) as promotion_id,
	qty::number(38,0) as qty,
	purchase_value::number(20,4) as purchase_value,
	l1_global_category::varchar(100) as l1_global_category,
	l2_global_category::varchar(255) as l2_global_category,
	l3_global_category::varchar(255) as l3_global_category,
	attri_commi_id::varchar(100) as attri_commi_id,
	channel::varchar(100) as channel,
	replace(commi_rate,'%','')::number(38,0) as commi_rate,
	expense::varchar(100) as expense,
	deduction_status::varchar(100) as deduction_status,
	deduction_method::varchar(100) as deduction_method,
	deduction_time::varchar(100) as deduction_time,
	shop_name::varchar(50) as shop_name,
	filename::varchar(255) as filename,
	crtd_dttm :: timestamp_ntz(9) as crtd_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm
from source_1
UNION ALL
select
	order_time::varchar(50) as order_time,
	order_id::varchar(100) as order_id,
	order_completed_time::varchar(50) as order_completed_time,
	verified_status::varchar(50) as verified_status,
	conversion_completed_time::varchar(50) as conversion_completed_time,
	brand_commission::number(20,4) as brand_commission,
	item_id::varchar(50) as item_id,
	item_name::varchar(500) as item_name,
	model_id::varchar(50) as model_id,
	price::number(20,4) as price,
	promotion_id::varchar(100) as promotion_id,
	qty::number(38,0) as qty,
	purchase_value::number(20,4) as purchase_value,
	l1_global_category::varchar(100) as l1_global_category,
	l2_global_category::varchar(255) as l2_global_category,
	l3_global_category::varchar(255) as l3_global_category,
	attri_commi_id::varchar(100) as attri_commi_id,
	channel::varchar(100) as channel,
	replace(commi_rate,'%','')::number(38,0) as commi_rate,
	expense::varchar(100) as expense,
	deduction_status::varchar(100) as deduction_status,
	deduction_method::varchar(100) as deduction_method,
	deduction_time::varchar(100) as deduction_time,
	shop_name::varchar(50) as shop_name,
	filename::varchar(255) as filename,
	crtd_dttm :: timestamp_ntz(9) as crtd_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm
from source_2
UNION ALL
select
	order_time::varchar(50) as order_time,
	order_id::varchar(100) as order_id,
	order_completed_time::varchar(50) as order_completed_time,
	verified_status::varchar(50) as verified_status,
	conversion_completed_time::varchar(50) as conversion_completed_time,
	brand_commission::number(20,4) as brand_commission,
	item_id::varchar(50) as item_id,
	item_name::varchar(500) as item_name,
	model_id::varchar(50) as model_id,
	price::number(20,4) as price,
	promotion_id::varchar(100) as promotion_id,
	qty::number(38,0) as qty,
	purchase_value::number(20,4) as purchase_value,
	l1_global_category::varchar(100) as l1_global_category,
	l2_global_category::varchar(255) as l2_global_category,
	l3_global_category::varchar(255) as l3_global_category,
	attri_commi_id::varchar(100) as attri_commi_id,
	channel::varchar(100) as channel,
	replace(commi_rate,'%','')::number(38,0) as commi_rate,
	expense::varchar(100) as expense,
	deduction_status::varchar(100) as deduction_status,
	deduction_method::varchar(100) as deduction_method,
	deduction_time::varchar(100) as deduction_time,
	shop_name::varchar(50) as shop_name,
	filename::varchar(255) as filename,
	crtd_dttm :: timestamp_ntz(9) as crtd_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm
from source_3
)
select * from final