{{
    config
    (
        materialized = 'incremental',
        incremental_model = 'append',
        pre_hook = "
            {% if is_incremental() %}
            delete from {{this}} itg where itg.file_name in (select sdl.file_name 
			from {{ source('thasdl_raw', 'sdl_pop6_th_tasks') }} sdl
            where file_name not in (
                    select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_pop6_th_tasks__null_test') }}
                    union all
                    select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_pop6_th_tasks__duplicate_test') }}
            ) ) 
            {% endif %}
        "
    )
}}

with sdl_pop6_kr_tasks as 
(
	select * from {{ ref('aspwks_integration__wks_pop6_kr_tasks') }}
),

sdl_pop6_tw_tasks as
(
	select * from {{ ref('aspwks_integration__wks_pop6_tw_tasks') }}
),

sdl_pop6_hk_tasks as
(
	select * from {{ ref('aspwks_integration__wks_pop6_hk_tasks') }}
),

sdl_pop6_jp_tasks as
(
	select * from {{ ref('aspwks_integration__wks_pop6_jp_tasks') }}
),

sdl_pop6_sg_tasks as
(
	select * from {{ ref('aspwks_integration__wks_pop6_sg_tasks') }}
),

sdl_pop6_th_tasks as
(
	select * from {{ source('thasdl_raw', 'sdl_pop6_th_tasks') }}
    where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_pop6_th_tasks__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_pop6_th_tasks__duplicate_test') }}
    )
),


kr AS
(
	SELECT 
		'KR'as cntry_cd,
		substring(file_name, 1, 8) as src_file_date,
		visit_id,
        task_group,
        task_id,
        task_name,
        field_id,
        field_code,
        field_label,
        field_type,
        dependent_on_field_id,
        response,
        file_name,
        run_id,
        crtd_dttm,
        current_timestamp()::timestamp_ntz(9)
	FROM sdl_pop6_kr_tasks
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='KR')
    {% endif %}
),

tw AS
(
	SELECT 
		'TW'as  cntry_cd,
		substring(file_name, 1, 8) as src_file_date,
		visit_id,
        task_group,
        task_id,
        task_name,
        field_id,
        field_code,
        field_label,
        field_type,
        dependent_on_field_id,
        response,
        file_name,
        run_id,
        crtd_dttm,
        current_timestamp()::timestamp_ntz(9)
	FROM sdl_pop6_tw_tasks
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='TW')
    {% endif %}
),

hk AS
(
	SELECT 
		'HK' as  cntry_cd,
		substring(file_name, 1, 8) as src_file_date,
		visit_id,
        task_group,
        task_id,
        task_name,
        field_id,
        field_code,
        field_label,
        field_type,
        dependent_on_field_id,
        response,
        file_name,
        run_id,
        crtd_dttm,
        current_timestamp()::timestamp_ntz(9)
	FROM sdl_pop6_hk_tasks
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='HK')
    {% endif %}
),

jp AS
(
	SELECT 
		'JP' as  cntry_cd,
		substring(file_name, 1, 8) as src_file_date,
		visit_id,
        task_group,
        task_id,
        task_name,
        field_id,
        field_code,
        field_label,
        field_type,
        dependent_on_field_id,
        response,
        file_name,
        run_id,
        crtd_dttm,
        current_timestamp()::timestamp_ntz(9)
	FROM sdl_pop6_jp_tasks
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='JP')
    {% endif %}
),


sg AS
(
	SELECT 
		'SG' as  cntry_cd,
		substring(file_name, 1, 8) as src_file_date,
		visit_id,
        task_group,
        task_id,
        task_name,
        field_id,
        field_code,
        field_label,
        field_type,
        dependent_on_field_id,
        response,
        file_name,
        run_id,
        crtd_dttm,
        current_timestamp()::timestamp_ntz(9)
	FROM sdl_pop6_sg_tasks
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='SG')
    {% endif %}
),


th as 
(
	SELECT 
        'TH' as  cntry_cd,
		substring(file_name, 1, 8) as src_file_date,
		visit_id,
        task_group,
        task_id,
        task_name,
        field_id,
        field_code,
        field_label,
        field_type,
        dependent_on_field_id,
        response,
        file_name,
        run_id,
        crtd_dttm,
        current_timestamp()::timestamp_ntz(9)
	FROM sdl_pop6_th_tasks
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
		task_group::varchar(50) as task_group,
		task_id::number(18,0) as task_id,
		task_name::varchar(50) as task_name,
		field_id::varchar(255) as field_id,
		field_code::varchar(255) as field_code,
		field_label::varchar(255) as field_label,
		field_type::varchar(50) as field_type,
		dependent_on_field_id::varchar(255) as dependent_on_field_id,
		response::varchar(65535) as response,
		file_name::varchar(100) as file_name,
		run_id::number(14,0) as run_id,
		current_timestamp()::timestamp_ntz(9) as crtd_dttm,
		current_timestamp()::timestamp_ntz(9) as updt_dttm
	from transformed
)

select * from final