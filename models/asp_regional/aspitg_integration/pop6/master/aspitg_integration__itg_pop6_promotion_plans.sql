{{
    config
    (
        materialized = 'incremental',
        incremental_model = 'append'
    )
}}

with sdl_pop6_kr_promotion_plans as 
(
	select * from {{ ref('aspwks_integration__wks_pop6_kr_promotion_plans') }}
),

sdl_pop6_tw_promotion_plans as
(
	select * from {{ ref('aspwks_integration__wks_pop6_tw_promotion_plans') }}
),

sdl_pop6_hk_promotion_plans as
(
	select * from {{ ref('aspwks_integration__wks_pop6_hk_promotion_plans') }}
),

sdl_pop6_jp_promotion_plans as
(
	select * from {{ ref('aspwks_integration__wks_pop6_jp_promotion_plans') }}
),

sdl_pop6_sg_promotion_plans as
(
	select * from {{ ref('aspwks_integration__wks_pop6_sg_promotion_plans') }}
),

sdl_pop6_th_promotion_plans as
(
	select * from {{ ref('aspwks_integration__wks_pop6_th_promotion_plans') }}
),
kr AS
(
	SELECT 
		'KR'as cntry_cd,
		substring(file_name, 1, 8) as src_file_date,
		status,
        promotion_plan_id,
        allocation_method,
        pop_code_or_pop_list_code,
        team,
        promotion_code,
        promotion_name,
        promotion_mechanics,
        start_date,
        end_date,
        product_attribute_id,
        product_attribute,
        product_attribute_value_id,
        product_attribute_value,
        promotion_price,
        price_field,
        photo_field,
        reason_field,
        file_name,
        run_id,
        crtd_dttm,
        current_timestamp()::timestamp_ntz(9)
	FROM sdl_pop6_kr_promotion_plans
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='KR')
    {% endif %}
),

tw AS
(
	SELECT 
		'TW'as cntry_cd,
		substring(file_name, 1, 8) as src_file_date,
		status,
        promotion_plan_id,
        allocation_method,
        pop_code_or_pop_list_code,
        team,
        promotion_code,
        promotion_name,
        promotion_mechanics,
        start_date,
        end_date,
        product_attribute_id,
        product_attribute,
        product_attribute_value_id,
        product_attribute_value,
        promotion_price,
        price_field,
        photo_field,
        reason_field,
        file_name,
        run_id,
        crtd_dttm,
        current_timestamp()::timestamp_ntz(9)
	FROM sdl_pop6_tw_promotion_plans
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='TW')
    {% endif %}
),

hk AS
(
	SELECT 
		'HK' as cntry_cd,
		substring(file_name, 1, 8) as src_file_date,
		status,
        promotion_plan_id,
        allocation_method,
        pop_code_or_pop_list_code,
        team,
        promotion_code,
        promotion_name,
        promotion_mechanics,
        start_date,
        end_date,
        product_attribute_id,
        product_attribute,
        product_attribute_value_id,
        product_attribute_value,
        promotion_price,
        price_field,
        photo_field,
        reason_field,
        file_name,
        run_id,
        crtd_dttm,
        current_timestamp()::timestamp_ntz(9)
	FROM sdl_pop6_hk_promotion_plans
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='HK')
    {% endif %}
),

jp AS
(
	SELECT 
		'JP' as cntry_cd,
		substring(file_name, 1, 8) as src_file_date,
		status,
        promotion_plan_id,
        allocation_method,
        pop_code_or_pop_list_code,
        team,
        promotion_code,
        promotion_name,
        promotion_mechanics,
        start_date,
        end_date,
        product_attribute_id,
        product_attribute,
        product_attribute_value_id,
        product_attribute_value,
        promotion_price,
        price_field,
        photo_field,
        reason_field,
        file_name,
        run_id,
        crtd_dttm,
        current_timestamp()::timestamp_ntz(9)
	FROM sdl_pop6_jp_promotion_plans
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='JP')
    {% endif %}
),


sg AS
(
	SELECT 
		'SG' as cntry_cd,
		substring(file_name, 1, 8) as src_file_date,
		status,
        promotion_plan_id,
        allocation_method,
        pop_code_or_pop_list_code,
        team,
        promotion_code,
        promotion_name,
        promotion_mechanics,
        start_date,
        end_date,
        product_attribute_id,
        product_attribute,
        product_attribute_value_id,
        product_attribute_value,
        promotion_price,
        price_field,
        photo_field,
        reason_field,
        file_name,
        run_id,
        crtd_dttm,
        current_timestamp()::timestamp_ntz(9)
	FROM sdl_pop6_sg_promotion_plans
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='SG')
    {% endif %}
),


th as 
(
	SELECT 
        'TH' as cntry_cd,
		substring(file_name, 1, 8) as src_file_date,
		status,
        promotion_plan_id,
        allocation_method,
        pop_code_or_pop_list_code,
        team,
        promotion_code,
        promotion_name,
        promotion_mechanics,
        start_date,
        end_date,
        product_attribute_id,
        product_attribute,
        product_attribute_value_id,
        product_attribute_value,
        promotion_price,
        price_field,
        photo_field,
        reason_field,
        file_name,
        run_id,
        crtd_dttm,
        current_timestamp()::timestamp_ntz(9)
	FROM sdl_pop6_th_promotion_plans
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='TH')
    {% endif %}
),


transformed AS
(
	select * from kr
	union
	select * from tw
	union
	select * from hk
	union
	select * from jp
	union
	select * from sg
	union
	select * from th
),

final AS
(
	select 
		cntry_cd::varchar(10) as cntry_cd,
		src_file_date::varchar(10) as src_file_date,
		status::number(18,0) as status,
		promotion_plan_id::varchar(255) as promotion_plan_id,
		allocation_method::varchar(255) as allocation_method,
		pop_code_or_pop_list_code::varchar(50) as pop_code_or_pop_list_code,
		team::varchar(50) as team,
		promotion_code::varchar(255) as promotion_code,
		promotion_name::varchar(255) as promotion_name,
		promotion_mechanics::varchar(255) as promotion_mechanics,
		start_date::date as start_date,
		end_date::date as end_date,
		product_attribute_id::varchar(255) as product_attribute_id,
		product_attribute::varchar(200) as product_attribute,
		product_attribute_value_id::varchar(255) as product_attribute_value_id,
		product_attribute_value::varchar(65535) as product_attribute_value,
		promotion_price::number(18,2) as promotion_price,
		price_field::varchar(255) as price_field,
		photo_field::varchar(255) as photo_field,
		reason_field::varchar(255) as reason_field,
		file_name::varchar(100) as file_name,
		run_id::number(14,0) as run_id,
		current_timestamp()::timestamp_ntz(9) as crtd_dttm,
		current_timestamp()::timestamp_ntz(9) as updt_dttm
	from transformed
)

select * from final