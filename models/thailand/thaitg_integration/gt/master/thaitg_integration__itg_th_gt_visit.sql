{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['file_name']
    )
}}

with source as(
    select * from {{ source('thasdl_raw', 'sdl_th_gt_visit') }}
),
final as(
    select
        customer_name::varchar(100) as customer_name,
        try_to_date(date_plan) as date_plan,
        time_plan::varchar(50) as time_plan,
        try_to_date(date_visi) as date_visi,
        time_visi::varchar(50) as time_visit_in,
        object::varchar(100) as object,
        try_to_date(visit_end) as visit_end,
        visit_time::varchar(50) as time_visit_out,
        regioncode::varchar(50) as regioncode,
        areacode::varchar(50) as areacode,
        branchcode::varchar(50) as branchcode,
        saleunit::varchar(50) as saleunit,
        time_survey_in::varchar(50) as time_survey_in,
        time_survey_out::varchar(50) as time_survey_out,
        count_survey::varchar(50) as count_survey,
        filename::varchar(100) as filename,
        run_id::varchar(50) as run_id,
        current_timestamp()::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final