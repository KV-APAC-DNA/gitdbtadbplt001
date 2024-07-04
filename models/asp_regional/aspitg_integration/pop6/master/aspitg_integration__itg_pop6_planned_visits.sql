{{
    config
    (
        materialized = 'incremental',
        incremental_model = 'append'
    )
}}

with sdl_pop6_kr_planned_visits as 
(
	select * from {{ source('ntasdl_raw', 'sdl_pop6_kr_planned_visits') }}
),

sdl_pop6_tw_planned_visits as
(
	select * from {{ source('ntasdl_raw', 'sdl_pop6_tw_planned_visits') }}
),

sdl_pop6_hk_planned_visits as
(
	select * from {{ source('ntasdl_raw', 'sdl_pop6_hk_planned_visits') }}
),

sdl_pop6_jp_planned_visits as
(
	select * from {{ source('jpnsdl_raw', 'sdl_pop6_jp_planned_visits') }}
),

sdl_pop6_sg_planned_visits as
(
	select * from {{ source('sgpsdl_raw', 'sdl_pop6_sg_planned_visits') }}
),

sdl_pop6_th_planned_visits as
(
	select * from {{ source('thasdl_raw', 'sdl_pop6_th_planned_visits') }}
),


kr AS
(
	SELECT 
		'KR'as cntry_cd,
		substring(file_name, 1, 8) as src_file_date,
		planned_visit_date,
        popdb_id,
        pop_code,
        pop_name,
        address,
        username,
        user_full_name,
        file_name,
        run_id,
        crtd_dttm,
        current_timestamp()::timestamp_ntz(9)
	FROM sdl_pop6_kr_planned_visits
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='KR')
    {% endif %}
),

tw AS
(
	SELECT 
		'TW'as cntry_cd,
		substring(file_name, 1, 8) as src_file_date,
		planned_visit_date,
        popdb_id,
        pop_code,
        pop_name,
        address,
        username,
        user_full_name,
        file_name,
        run_id,
        crtd_dttm,
        current_timestamp()::timestamp_ntz(9)
	FROM sdl_pop6_tw_planned_visits
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='TW')
    {% endif %}
),

hk AS
(
	SELECT 
		'HK' as cntry_cd,
		substring(file_name, 1, 8) as src_file_date,
		planned_visit_date,
        popdb_id,
        pop_code,
        pop_name,
        address,
        username,
        user_full_name,
        file_name,
        run_id,
        crtd_dttm,
        current_timestamp()::timestamp_ntz(9)
	FROM sdl_pop6_hk_planned_visits
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='HK')
    {% endif %}
),

jp AS
(
	SELECT 
		'JP' as cntry_cd,
		substring(file_name, 1, 8) as src_file_date,
		planned_visit_date,
        popdb_id,
        pop_code,
        pop_name,
        address,
        username,
        user_full_name,
        file_name,
        run_id,
        crtd_dttm,
        current_timestamp()::timestamp_ntz(9)
	FROM sdl_pop6_jp_planned_visits
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='JP')
    {% endif %}
),


sg AS
(
	SELECT 
		'SG' as cntry_cd,
		substring(file_name, 1, 8) as src_file_date,
		planned_visit_date,
        popdb_id,
        pop_code,
        pop_name,
        address,
        username,
        user_full_name,
        file_name,
        run_id,
        crtd_dttm,
        current_timestamp()::timestamp_ntz(9)
	FROM sdl_pop6_sg_planned_visits
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='SG')
    {% endif %}
),


th as 
(
	SELECT 
        'TH' as cntry_cd,
		substring(file_name, 1, 8) as src_file_date,
		planned_visit_date,
        popdb_id,
        pop_code,
        pop_name,
        address,
        username,
        user_full_name,
        file_name,
        run_id,
        crtd_dttm,
        current_timestamp()::timestamp_ntz(9)
	FROM sdl_pop6_th_planned_visits
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
        planned_visit_date::date as planned_visit_date,
        popdb_id::varchar(255) as popdb_id,
        pop_code::varchar(50) as pop_code,
        pop_name::varchar(100) as pop_name,
        address::varchar(150) as address,
        username::varchar(50) as username,
        user_full_name::varchar(50) as user_full_name,
		file_name::varchar(100) as file_name,
		run_id::number(14,0) as run_id,
		current_timestamp()::timestamp_ntz(9) as crtd_dttm,
		current_timestamp()::timestamp_ntz(9) as updt_dttm
	from transformed
)

select * from final