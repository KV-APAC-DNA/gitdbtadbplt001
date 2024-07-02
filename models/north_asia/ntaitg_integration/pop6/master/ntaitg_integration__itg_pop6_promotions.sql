{{
    config
    (
        materialized = 'incremental',
        incremental_model = 'append'
    )
}}

with sdl_pop6_kr_promotions as 
(
	select * from {{ source('ntasdl_raw', 'sdl_pop6_kr_promotions') }}
),

sdl_pop6_tw_promotions as
(
	select * from {{ source('ntasdl_raw', 'sdl_pop6_tw_promotions') }}
),

sdl_pop6_hk_promotions as
(
	select * from {{ source('ntasdl_raw', 'sdl_pop6_hk_promotions') }}
),

sdl_pop6_jp_promotions as
(
	select * from {{ source('jpnsdl_raw', 'sdl_pop6_jp_promotions') }}
),

sdl_pop6_sg_promotions as
(
	select * from {{ source('sgpsdl_raw', 'sdl_pop6_sg_promotions') }}
),

sdl_pop6_th_promotions as
(
	select * from {{ source('thasdl_raw', 'sdl_pop6_th_promotions') }}
),


kr AS
(
	SELECT 
		'KR'as cntry_cd,
		substring(file_name, 1, 8) as src_file_date,
		visit_id,
        promotion_plan_id,
        promotion_code,
        promotion_name,
        promotion_mechanics,
        promotion_type,
        start_date,
        end_date,
        product_attribute_id,
        product_attribute,
        product_attribute_value_id,
        product_attribute_value,
        promotion_price,
        promotion_compliance,
        actual_price,
        non_compliance_reason,
        photo,
        file_name,
        run_id,
        crtd_dttm,
        current_timestamp()::timestamp_ntz(9)
	FROM sdl_pop6_kr_promotions
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='KR')
    {% endif %}
),

tw AS
(
	SELECT 
		'TW'as cntry_cd,
		substring(file_name, 1, 8) as src_file_date,
		visit_id,
        promotion_plan_id,
        promotion_code,
        promotion_name,
        promotion_mechanics,
        promotion_type,
        start_date,
        end_date,
        product_attribute_id,
        product_attribute,
        product_attribute_value_id,
        product_attribute_value,
        promotion_price,
        promotion_compliance,
        actual_price,
        non_compliance_reason,
        photo,
        file_name,
        run_id,
        crtd_dttm,
        current_timestamp()::timestamp_ntz(9)
	FROM sdl_pop6_tw_promotions
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='TW')
    {% endif %}
),

hk AS
(
	SELECT 
		'HK' as cntry_cd,
		substring(file_name, 1, 8) as src_file_date,
		visit_id,
        promotion_plan_id,
        promotion_code,
        promotion_name,
        promotion_mechanics,
        promotion_type,
        start_date,
        end_date,
        product_attribute_id,
        product_attribute,
        product_attribute_value_id,
        product_attribute_value,
        promotion_price,
        promotion_compliance,
        actual_price,
        non_compliance_reason,
        photo,
        file_name,
        run_id,
        crtd_dttm,
        current_timestamp()::timestamp_ntz(9)
	FROM sdl_pop6_hk_promotions
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='HK')
    {% endif %}
),

jp AS
(
	SELECT 
		'JP' as cntry_cd,
		substring(file_name, 1, 8) as src_file_date,
		visit_id,
        promotion_plan_id,
        promotion_code,
        promotion_name,
        promotion_mechanics,
        promotion_type,
        start_date,
        end_date,
        product_attribute_id,
        product_attribute,
        product_attribute_value_id,
        product_attribute_value,
        promotion_price,
        promotion_compliance,
        actual_price,
        non_compliance_reason,
        photo,
        file_name,
        run_id,
        crtd_dttm,
        current_timestamp()::timestamp_ntz(9)
	FROM sdl_pop6_jp_promotions
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='JP')
    {% endif %}
),


sg AS
(
	SELECT 
		'SG' as cntry_cd,
		substring(file_name, 1, 8) as src_file_date,
		visit_id,
        promotion_plan_id,
        promotion_code,
        promotion_name,
        promotion_mechanics,
        promotion_type,
        start_date,
        end_date,
        product_attribute_id,
        product_attribute,
        product_attribute_value_id,
        product_attribute_value,
        promotion_price,
        promotion_compliance,
        actual_price,
        non_compliance_reason,
        photo,
        file_name,
        run_id,
        crtd_dttm,
        current_timestamp()::timestamp_ntz(9)
	FROM sdl_pop6_sg_promotions
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='SG')
    {% endif %}
),


th as 
(
	SELECT 
        'TH' as cntry_cd,
		substring(file_name, 1, 8) as src_file_date,
		visit_id,
        promotion_plan_id,
        promotion_code,
        promotion_name,
        promotion_mechanics,
        promotion_type,
        start_date,
        end_date,
        product_attribute_id,
        product_attribute,
        product_attribute_value_id,
        product_attribute_value,
        promotion_price,
        promotion_compliance,
        actual_price,
        non_compliance_reason,
        photo,
        file_name,
        run_id,
        crtd_dttm,
        current_timestamp()::timestamp_ntz(9)
	FROM sdl_pop6_th_promotions
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
		visit_id::varchar(255) as visit_id,
		promotion_plan_id::varchar(255) as promotion_plan_id,
		promotion_code::varchar(255) as promotion_code,
		promotion_name::varchar(255) as promotion_name,
		promotion_mechanics::varchar(255) as promotion_mechanics,
		promotion_type::varchar(255) as promotion_type,
		start_date::date as start_date,
		end_date::date as end_date,
		product_attribute_id::varchar(255) as product_attribute_id,
		product_attribute::varchar(200) as product_attribute,
		product_attribute_value_id::varchar(255) as product_attribute_value_id,
		product_attribute_value::varchar(65535) as product_attribute_value,
		promotion_price::number(18,2) as promotion_price,
		promotion_compliance::varchar(10) as promotion_compliance,
		actual_price::number(18,2) as actual_price,
		non_compliance_reason::varchar(255) as non_compliance_reason,
		photo::varchar(65535) as photo,
		file_name::varchar(100) as file_name,
		run_id::number(14,0) as run_id,
		current_timestamp()::timestamp_ntz(9) as crtd_dttm,
		current_timestamp()::timestamp_ntz(9) as updt_dttm
	from transformed
)

select * from final