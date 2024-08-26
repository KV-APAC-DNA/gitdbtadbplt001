{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with source as(
    select * from {{ source('thasdl_raw', 'sdl_th_gt_visit') }}
    where filename not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_visit__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_visit__duplicate_test') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_visit__test_format') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_visit__test_date_format_odd_eve_leap') }}
    )
),
final as(
    select   
        cntry_cd as cntry_cd,
        crncy_cd as crncy_cd,
        id_sale as id_sale,
        sale_name as sale_name,
        id_customer as id_customer,
        customer_name as customer_name,
        date_plan as date_plan,
        time_plan as time_plan,
        date_visi as date_visi,
        time_visi as time_visi,
        object as object,
        visit_end as visit_end,
        visit_time as visit_time,
        regioncode as regioncode,
        areacode as areacode,
        branchcode as branchcode,
        saleunit as saleunit,
        time_survey_in as time_survey_in,
        time_survey_out as time_survey_out,
        count_survey as count_survey,
        filename as file_name,
        run_id as run_id,
        crt_dttm as crt_dttm
   from source
     {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crt_dttm > (select max(crt_dttm) from {{ this }}) 
 {% endif %}
)
select * from final