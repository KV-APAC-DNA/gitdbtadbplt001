{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['saleunit','id_sale', 'id_customer', 'date_plan', 'time_plan','time_visit_in', 'visit_end','time_visit_out'],
        pre_hook= "delete from {{this}} 
        where (upper(trim(saleunit)), upper(trim(id_sale)), upper(trim(id_customer)), date_plan, coalesce(trim(time_plan), 'NA'), COALESCE(TRIM(time_visit_in), 'NA'), COALESCE(visit_end, '9999-12-31'), COALESCE(TRIM(time_visit_out), 'NA')) IN ( SELECT DISTINCT UPPER(TRIM(saleunit)), UPPER(TRIM(id_sale)), UPPER(TRIM(id_customer)), date_plan, COALESCE(TRIM(time_plan), 'NA'), COALESCE(TRIM(time_visi), 'NA'), COALESCE(visit_end, '9999-12-31'), COALESCE(TRIM(visit_time), 'NA') 
        FROM {{ source('thasdl_raw','sdl_th_gt_visit') }}
        where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_visit__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_visit__duplicate_test') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_visit__test_format') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_visit__test_date_format_odd_eve_leap') }}
            )
        )"
    )
}}

with source as(
    select *,
    dense_rank() over( partition by
        upper(trim(saleunit)), 
        upper(trim(id_sale)), 
        upper(trim(id_customer)), 
        date_plan, 
        coalesce(trim(time_plan), 'NA'), 
        COALESCE(TRIM(time_visi), 'NA'), 
        COALESCE(visit_end, '9999-12-31'), 
        COALESCE(TRIM(visit_time), 'NA')
        order by filename desc
        ) as rnk
    from {{ source('thasdl_raw','sdl_th_gt_visit') }}
    where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_visit__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_visit__duplicate_test') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_visit__test_format') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_visit__test_date_format_odd_eve_leap') }}
    ) qualify rnk=1
),
final as(
    select
        cntry_cd::varchar(5) as cntry_cd,
        crncy_cd::varchar(5) as crncy_cd,
        id_sale::varchar(50) as id_sale,
        sale_name::varchar(150) as sale_name,
        id_customer::varchar(50) as id_customer,
        customer_name::varchar(100) as customer_name,
        date_plan::date as date_plan,
        time_plan::varchar(50) as time_plan,
        date_visi::date as date_visi,
        time_visi::varchar(50) as time_visit_in,
        left(object,100)::varchar(100) as object,
        visit_end::date as visit_end,
        visit_time::varchar(50) as time_visit_out,
        regioncode::varchar(50) as regioncode,
        areacode::varchar(50) as areacode,
        branchcode::varchar(50) as branchcode,
        saleunit::varchar(50) as saleunit,
        time_survey_in::varchar(50) as time_survey_in,
        time_survey_out::varchar(50) as time_survey_out,
        count_survey::varchar(50) as count_survey,
        filename::varchar(100) as file_name,
        run_id::varchar(50) as run_id,
        current_timestamp()::timestamp_ntz(9) as crt_dttm
    from source
    where rnk=1
)
select * from final