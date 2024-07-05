{{
    config
    (
        materialized = 'incremental',
        incremental_model = 'append',
        pre_hook = "{% if is_incremental() %}
                    DELETE
                    FROM {{this}}
                    WHERE ( visit_date, pop_code, country, merchandiser_userid, nvl(audit_form_name, ''), nvl(section_name, '') ) IN 
                    ( SELECT visit_date, pop_code, country, merchandiser_userid, nvl(audit_form_name, ''), nvl(section_name, '') FROM {{ source('ntasdl_raw', 'sdl_pop6_hk_exclusion') }} WHERE upper(operation_type) = 'D'  
                    UNION ALL SELECT visit_date, pop_code, country, merchandiser_userid, nvl(audit_form_name, ''), nvl(section_name, '') FROM {{ source('ntasdl_raw', 'sdl_pop6_kr_exclusion') }} WHERE upper(operation_type) = 'D'  
                    UNION ALL SELECT visit_date, pop_code, country, merchandiser_userid, nvl(audit_form_name, ''), nvl(section_name, '') FROM {{ source('ntasdl_raw', 'sdl_pop6_tw_exclusion') }} WHERE upper(operation_type) = 'D'  
                    UNION ALL SELECT visit_date, pop_code, country, merchandiser_userid, nvl(audit_form_name, ''), nvl(section_name, '') FROM {{ source('jpnsdl_raw', 'sdl_pop6_jp_exclusion') }} WHERE upper(operation_type) = 'D' );
                    {% endif %}"
    )
}}

with sdl_pop6_kr_exclusion as 
(
	select * from --{{ source('ntasdl_raw', 'sdl_pop6_kr_exclusion') }}
    {{ref('aspwks_integration__wks_pop6_kr_exclusion')}}
),

sdl_pop6_tw_exclusion as
(
	select * from --{{ source('ntasdl_raw', 'sdl_pop6_tw_exclusion') }}
    {{ref('aspwks_integration__wks_pop6_tw_exclusion')}}
),

sdl_pop6_hk_exclusion as
(
	select * from --{{ source('ntasdl_raw', 'sdl_pop6_hk_exclusion') }}
    {{ref('aspwks_integration__wks_pop6_hk_exclusion')}}
),

sdl_pop6_jp_exclusion as
(
	select * from --{{ source('jpnsdl_raw', 'sdl_pop6_jp_exclusion') }}
    {{ref('aspwks_integration__wks_pop6_jp_exclusion')}}
),


kr AS
(
	SELECT 
		visit_date,
        pop_code,
        country,
        merchandiser_userid,
        audit_form_name,
        section_name,
        operation_type,
        file_name,
        run_id,
        crtd_dttm,
        current_timestamp()::timestamp_ntz(9)
	FROM sdl_pop6_kr_exclusion
    WHERE upper(operation_type) = 'I'
),

tw AS
(
	SELECT 
		visit_date,
        pop_code,
        country,
        merchandiser_userid,
        audit_form_name,
        section_name,
        operation_type,
        file_name,
        run_id,
        crtd_dttm,
        current_timestamp()::timestamp_ntz(9)
	FROM sdl_pop6_tw_exclusion
    WHERE upper(operation_type) = 'I'
),

hk AS
(
	SELECT 
		visit_date,
        pop_code,
        country,
        merchandiser_userid,
        audit_form_name,
        section_name,
        operation_type,
        file_name,
        run_id,
        crtd_dttm,
        current_timestamp()::timestamp_ntz(9)
	FROM sdl_pop6_hk_exclusion
    WHERE upper(operation_type) = 'I'
),

jp AS
(
	SELECT 
		visit_date,
        pop_code,
        country,
        merchandiser_userid,
        audit_form_name,
        section_name,
        operation_type,
        file_name,
        run_id,
        crtd_dttm,
        current_timestamp()::timestamp_ntz(9)
	FROM sdl_pop6_jp_exclusion
    WHERE upper(operation_type) = 'I'
),





transformed AS
(
	select * from kr
	union all
	select * from tw
	union all
	select * from hk
	union all
	select * from jp
),

final AS
(
	select 
		visit_date::date as visit_date,
        pop_code::varchar(40) as pop_code,
        country::varchar(10) as country,
        merchandiser_userid::varchar(100) as merchandiser_userid,
        audit_form_name::varchar(500) as audit_form_name,
        section_name::varchar(500) as section_name,
        operation_type::varchar(10) as operation_type,
        file_name::varchar(40) as file_name,
        run_id::varchar(40) as run_id,
		current_timestamp()::timestamp_ntz(9) as crtd_dttm,
		current_timestamp()::timestamp_ntz(9) as uptd_dttm
	from transformed
)

select * from final