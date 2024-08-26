{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= "delete from {{this}} where exists( select 1 
        from ( select upper(trim(saleunit)) as saleunit, min(schedule_date)::date as schedule_date 
        from {{ source('thasdl_raw','sdl_th_gt_schedule') }}
        where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_schedule__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_schedule__duplicate_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_schedule__test_format') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_schedule__test_date_format_odd_eve_leap') }}
            ) 
        group by upper(trim(saleunit)) ) as sdl where upper(trim({{this}}.saleunit)) = upper(trim(sdl.saleunit)) and {{this}}.schedule_date::date >= sdl.schedule_date::date )"
    )
}}


with source as(
    select 
    *,
    dense_rank() over(partition by upper(trim(saleunit)),try_to_date(schedule_date) order by filename desc) as rnk 
    from {{ source('thasdl_raw','sdl_th_gt_schedule') }}
    where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_schedule__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_schedule__duplicate_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_schedule__test_format') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_schedule__test_date_format_odd_eve_leap') }}
            ) qualify rnk=1
),
final as(
    select
        cntry_cd::varchar(5) as cntry_cd,
        crncy_cd::varchar(5) as crncy_cd,
        employeeid::varchar(50) as employeeid,
        routeid::varchar(50) as routeid,
        try_to_date(schedule_date) as schedule_date,
        approved::varchar(10) as approved,
        saleunit::varchar(50) as saleunit,
        filename::varchar(100) as file_name,
        run_id::varchar(50) as run_id,
        current_timestamp()::timestamp_ntz(9) as crt_dttm
    from source
    where rnk=1
)
select * from final