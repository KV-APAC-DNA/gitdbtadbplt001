{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= [
            "
            {% if is_incremental() %}
            delete from {{this}} itg where itg.file_name in (select sdl.file_name 
			from {{ source('thasdl_raw', 'sdl_jnj_consumerreach_cvs') }} sdl
            where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_jnj_consumerreach_cvs__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_jnj_consumerreach_cvs__test_date_format_odd_eve') }}
            ) 
            ) 
            {% endif %}
            "
            ,
            "delete from {{this}} where file_name in (select distinct file_name 
            from {{ source('thasdl_raw', 'sdl_jnj_consumerreach_cvs') }}
            where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_jnj_consumerreach_cvs__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_jnj_consumerreach_cvs__test_date_format_odd_eve') }}
            )
        );"]
    )
}}

with source as(
    select * from {{ source('thasdl_raw', 'sdl_jnj_consumerreach_cvs') }}
    where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_jnj_consumerreach_cvs__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_jnj_consumerreach_cvs__test_date_format_odd_eve') }}
            )
),
final as(
    select
        id::varchar(255) as id,
        cdate::varchar(255) as cdate,
        retail::varchar(255) as retail,
        retailname::varchar(255) as retailname,
        retailbranch::varchar(255) as retailbranch,
        retailprovince::varchar(255) as retailprovince,
        jjskubarcode::varchar(255) as jjskubarcode,
        trim(jjskuname)::varchar(255) as jjskuname,
        jjcore::varchar(255) as jjcore,
        distribution::varchar(255) as distribution,
        status::varchar(255) as status,
        run_id::number(14,0) as run_id,
        file_name::varchar(255) as file_name,
        yearmo::varchar(255) as yearmo,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
  from source
)
select * from final