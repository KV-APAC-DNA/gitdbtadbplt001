{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["employee_id","route_id","schedule_date"]
    )
}}

with source as (
    select 
        *,
        dense_rank() over(partition by employee_id,route_id,try_to_date(schedule_date,'yyyymmdd') order by filename desc) as rnk
        from {{ source('thasdl_raw', 'sdl_la_gt_schedule') }}
        where filename not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_schedule__duplicate_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_schedule__null_test') }}
            union all 
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_schedule__schedule_date_format_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_schedule__employee_id_format_test') }}
            ) qualify rnk=1
),
final as (
    select
        employee_id::varchar(50) as employee_id,
        route_id::varchar(50) as route_id,
        try_to_date(schedule_date,'yyyymmdd') as schedule_date,
        approved::varchar(5) as approved,
        saleunit::varchar(20) as saleunit,
        filename::varchar(50) as filename,
        run_id::varchar(14) as run_id,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
    where rnk=1
)
select * from final