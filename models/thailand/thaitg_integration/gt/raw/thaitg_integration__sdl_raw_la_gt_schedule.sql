{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with source as(
    select * from {{ source('thasdl_raw', 'sdl_la_gt_schedule') }}
    where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_schedule__duplicate_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_schedule__null_test') }}
            union all 
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_schedule__schedule_date_format_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_schedule__employee_id_format_test') }}
            )
),
final as(
    select * from source
    {% if is_incremental() %}
        where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
)
select * from final