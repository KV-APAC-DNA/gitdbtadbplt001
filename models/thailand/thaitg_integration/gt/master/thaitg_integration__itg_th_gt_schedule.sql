{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= "delete from {{this}} where exists( select 1 from ( select upper(trim(saleunit)) as saleunit, min(schedule_date)::date as schedule_date from {{ source('thasdl_raw','sdl_th_gt_schedule') }} group by upper(trim(saleunit)) ) as sdl where upper(trim({{this}}.saleunit)) = upper(trim(sdl.saleunit)) and {{this}}.schedule_date::date >= sdl.schedule_date::date )"
    )
}}


with source as(
    select 
    *,
    dense_rank() over(partition by upper(trim(saleunit)),try_to_date(schedule_date) order by filename desc) as rnk 
    from {{ source('thasdl_raw','sdl_th_gt_schedule') }}
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
        filename::varchar(100) as filename,
        run_id::varchar(50) as run_id,
        current_timestamp()::timestamp_ntz(9) as crt_dttm
    from source
    where rnk=1
)
select * from final