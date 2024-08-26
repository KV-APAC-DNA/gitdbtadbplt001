{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with source as(
    select * from {{ source('thasdl_raw', 'sdl_la_gt_visit') }}
    where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_visit__duplicate_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_visit__null_test') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_visit__date_plan_format_test') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_visit__date_visi_format_test') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_visit__visit_end_format_test') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_visit__time_visi_format_test') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_visit__visit_time_format_test') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_visit__id_sale_format_test') }}
            )
),
final as(
    select * from source
    {% if is_incremental() %}
        where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
)
select * from final