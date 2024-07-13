with sdl_pop6_kr_display_plans as (
    select * from {{ source('ntasdl_raw','sdl_pop6_kr_display_plans') }}
),
sdl_pop6_tw_display_plans as (
    select * from {{ source('ntasdl_raw','sdl_pop6_tw_display_plans') }}
),
sdl_pop6_hk_display_plans as (
    select * from {{ source('ntasdl_raw','sdl_pop6_hk_display_plans') }}
),
sdl_pop6_jp_display_plans as (
    select * from {{ source('jpnsdl_raw','sdl_pop6_jp_display_plans') }}
),
sdl_pop6_sg_display_plans as (
    select * from {{ source('sgpsdl_raw','sdl_pop6_sg_display_plans') }}
),
sdl_pop6_th_display_plans as (
    select * from {{ source('thasdl_raw','sdl_pop6_th_display_plans') }}
),
transformed as (
SELECT  
	'KR' as cntry_cd,
	substring(file_name, 1, 8) as src_file_date,
	display_plan_id,
	status,
	allocation_method,
	pop_code_or_pop_list_code,
	team,
	display_code,
	display_name,
	required_number_of_displays,
	start_date,
	end_date,
	product_attribute_id,
	product_attribute,
	product_attribute_value_id,
	product_attribute_value,
	comments,
	file_name,
	null as run_id,
	crtd_dttm,
    convert_timezone('Asia/Singapore',current_timestamp())::timestamp_ntz(9) as updt_dttm
FROM sdl_pop6_kr_display_plans
union
SELECT  
	'TW',
	substring(file_name, 1, 8),
	display_plan_id,
	status,
	allocation_method,
	pop_code_or_pop_list_code,
	team,	
	display_code,
	display_name,
	required_number_of_displays,
	start_date,
	end_date,
	product_attribute_id,
	product_attribute,
	product_attribute_value_id,
	product_attribute_value,
	comments,
	file_name,
	null as run_id,
	crtd_dttm,
    convert_timezone('Asia/Singapore',current_timestamp())::timestamp_ntz(9)
FROM sdl_pop6_tw_display_plans
union
SELECT  
	'HK',
	substring(file_name, 1, 8),
	display_plan_id,
	status,
	allocation_method,
	pop_code_or_pop_list_code,
	team,
	display_code,
	display_name,
	required_number_of_displays,
	start_date,
	end_date,
	product_attribute_id,
	product_attribute,
	product_attribute_value_id,
	product_attribute_value,
	comments,
	file_name,
	null as run_id,
	crtd_dttm,
    convert_timezone('Asia/Singapore',current_timestamp())::timestamp_ntz(9)
FROM sdl_pop6_hk_display_plans
union
SELECT  
	'JP',
	substring(file_name, 1, 8),
	display_plan_id,
	status,
	allocation_method,
	pop_code_or_pop_list_code,
	team,
	display_code,
	display_name,
	required_number_of_displays,
	start_date,
	end_date,
	product_attribute_id,
	product_attribute,
	product_attribute_value_id,
	product_attribute_value,
	comments,
	file_name,
	null as run_id,
	crtd_dttm,
    convert_timezone('Asia/Singapore',current_timestamp())::timestamp_ntz(9)
FROM sdl_pop6_jp_display_plans
union
SELECT  
	'SG',
	substring(file_name, 1, 8),
	display_plan_id,
	status,
	allocation_method,
	pop_code_or_pop_list_code,
	team,
	display_code,
	display_name,
	required_number_of_displays,
	start_date,
	end_date,
	product_attribute_id,
	product_attribute,
	product_attribute_value_id,
	product_attribute_value,
	comments,
	file_name,
	null as run_id,
	crtd_dttm,
    convert_timezone('Asia/Singapore',current_timestamp())::timestamp_ntz(9)
FROM sdl_pop6_sg_display_plans
/*Changes for TH POP6 - Jan 2024*/
union
SELECT 'TH',
       substring(file_name,1,8), 
       display_plan_id,
       status,
       allocation_method,
       pop_code_or_pop_list_code,
       team,
       display_code,
       display_name,
       required_number_of_displays,
       start_date,
       end_date,
       product_attribute_id,
       product_attribute,
       product_attribute_value_id,
       product_attribute_value,
       comments,
       file_name,
       	null as run_id,
       	--run_id,
       crtd_dttm,
    convert_timezone('Asia/Singapore',current_timestamp())::timestamp_ntz(9)
FROM sdl_pop6_th_display_plans
),
final as (
select
cntry_cd::varchar(10) as cntry_cd,
src_file_date::varchar(10) as src_file_date,
display_plan_id::varchar(255) as display_plan_id,
status::number(18,0) as status,
allocation_method::varchar(255) as allocation_method,
pop_code_or_pop_list_code::varchar(50) as pop_code_or_pop_list_code,
team::varchar(50) as team,
display_code::varchar(255) as display_code,
display_name::varchar(255) as display_name,
required_number_of_displays::number(18,0) as required_number_of_displays,
start_date::date as start_date,
end_date::date as end_date,
product_attribute_id::varchar(255) as product_attribute_id,
product_attribute::varchar(200) as product_attribute,
product_attribute_value_id::varchar(255) as product_attribute_value_id,
product_attribute_value::varchar(255) as product_attribute_value,
comments::varchar(255) as comments,
file_name::varchar(100) as file_name,
run_id::number(14,0) as run_id,
crtd_dttm::timestamp_ntz(9) as crtd_dttm,
updt_dttm::timestamp_ntz(9) as updt_dttm
from transformed
)
select * from final 