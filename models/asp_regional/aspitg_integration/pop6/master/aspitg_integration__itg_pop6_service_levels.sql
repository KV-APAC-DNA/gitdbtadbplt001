{{
    config
    (
        materialized = 'incremental',
        incremental_model = 'append'
    )
}}

with sdl_pop6_kr_service_levels as 
(
	select * from {{ ref('aspwks_integration__wks_pop6_kr_service_levels') }}
),

sdl_pop6_tw_service_levels as
(
	select * from {{ ref('aspwks_integration__wks_pop6_tw_service_levels') }}
),

sdl_pop6_hk_service_levels as
(
	select * from {{ ref('aspwks_integration__wks_pop6_hk_service_levels') }}
),

sdl_pop6_jp_service_levels as
(
	select * from {{ ref('aspwks_integration__wks_pop6_jp_service_levels') }}
),

sdl_pop6_sg_service_levels as
(
	select * from {{ ref('aspwks_integration__wks_pop6_sg_service_levels') }}
),

sdl_pop6_th_service_levels as
(
	select * from {{ ref('aspwks_integration__wks_pop6_th_service_levels') }}
),


kr AS
(
	SELECT 
		'KR'as cntry_cd,
		substring(file_name, 1, 8) as src_file_date,
		NULL::varchar(200) AS customer,
		customer_grade,
		NULL as channel,
		retail_environment_ps,
		team,
		visit_frequency,
		estimated_visit_duration,
		service_level_date,
		file_name,
		run_id,
        crtd_dttm,
	    current_timestamp()::timestamp_ntz(9),
		NULL as sales_grp_nm
	FROM sdl_pop6_kr_service_levels
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='KR')
    {% endif %}
),

tw AS
(
	SELECT 
		'TW'as cntry_cd,
		substring(file_name, 1, 8) as src_file_date,
		customer,
		customer_grade,
		NULL as channel,
		NULL as retail_environment_ps,
		team,
		visit_frequency,
		estimated_visit_duration,
		service_level_date,
		file_name,
		run_id,
        crtd_dttm,
	    current_timestamp()::timestamp_ntz(9),
		NULL as sales_grp_nm
	FROM sdl_pop6_tw_service_levels
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='TW')
    {% endif %}
),

hk AS
(
	SELECT 
		'HK' as cntry_cd,
		substring(file_name, 1, 8) as src_file_date,
		customer,
		customer_grade,
		NULL as channel,
		NULL as retail_environment_ps,
		team,
		visit_frequency,
		estimated_visit_duration,
		service_level_date,
		file_name,
		run_id,
        crtd_dttm,
	    current_timestamp()::timestamp_ntz(9),
		NULL as sales_grp_nm
	FROM sdl_pop6_hk_service_levels
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='HK')
    {% endif %}
),

jp AS
(
	SELECT 
		'JP' as cntry_cd,
		substring(file_name, 1, 8) as src_file_date,
		NULL AS customer,
		customer_grade,
		NULL as channel,
		NULL as retail_environment_ps,
		team,
		visit_frequency,
		estimated_visit_duration,
		service_level_date AS DATE,
		file_name,
		run_id,
        crtd_dttm,
	    current_timestamp()::timestamp_ntz(9),
		sales_grp_nm
	FROM sdl_pop6_jp_service_levels
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='JP')
    {% endif %}
),


sg AS
(
	SELECT 
		'SG' as cntry_cd,
		substring(file_name, 1, 8) as src_file_date,
		NULL AS customer,
		customer_grade,
		NULL as channel,
		NULL as retail_environment_ps,
		team,
		visit_frequency,
		estimated_visit_duration,
		service_level_date AS DATE,
		file_name,
		run_id,
        crtd_dttm,
	    current_timestamp()::timestamp_ntz(9),
		sales_grp_nm
	FROM sdl_pop6_sg_service_levels
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='SG')
    {% endif %}
),


th as 
(
	SELECT 
        'TH' as cntry_cd,
		substring(file_name, 1, 8) as src_file_date,
		NULL AS customer,
		customer_grade,
		NULL as channel,
		NULL as retail_environment_ps,
		team,
		visit_frequency,
		estimated_visit_duration,
		service_level_date AS DATE,
		file_name,
		run_id,
        crtd_dttm,
	    current_timestamp()::timestamp_ntz(9),
		sales_grp_nm
	FROM sdl_pop6_th_service_levels
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
		customer::varchar(200) as customer,
		customer_grade::varchar(200) as customer_grade,
		channel::varchar(200) as channel,
		retail_environment_ps::varchar(200) as retail_environment_ps,
		team::varchar(200) as team,
		visit_frequency::varchar(10) as visit_frequency,
		estimated_visit_duration::varchar(150) as estimated_visit_duration,
		service_level_date::date as service_level_date,
		file_name::varchar(100) as file_name,
		run_id::number(14,0) as run_id,
		current_timestamp()::timestamp_ntz(9) as crtd_dttm,
		current_timestamp()::timestamp_ntz(9) as updt_dttm,
		sales_grp_nm::varchar(255) as sales_grp_nm
	from transformed
)

select * from final