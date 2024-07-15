{{
    config(
        materialized="incremental",
        incremental_strategy="append"
)}}

with sdl_pop6_kr_display_plans as (
    select * from  {{ref('aspwks_integration__wks_pop6_kr_display_plans')}}
),
sdl_pop6_tw_display_plans as (
    select * from {{ref('aspwks_integration__wks_pop6_tw_display_plans')}}
),
sdl_pop6_hk_display_plans as (
    select * from {{ref('aspwks_integration__wks_pop6_hk_display_plans')}}
),
sdl_pop6_jp_display_plans as (
    select * from {{ref('aspwks_integration__wks_pop6_jp_display_plans')}}
),
sdl_pop6_sg_display_plans as (
    select * from {{ref('aspwks_integration__wks_pop6_sg_display_plans')}}
),
sdl_pop6_th_display_plans as (
    select * from {{ref('aspwks_integration__wks_pop6_th_display_plans')}}
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
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='KR')
    {% endif %}
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
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='TW')
    {% endif %}
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
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='HK')
    {% endif %}
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
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='JP')
    {% endif %}
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
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='SG')
    {% endif %}
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
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='TH')
    {% endif %}
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
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where transformed.crtd_dttm > (select max(crtd_dttm) from {{ this }})
 {% endif %}
)
select * from final