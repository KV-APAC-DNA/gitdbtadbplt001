{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with source as(
    select * from {{ source('thasdl_raw', 'sdl_th_gt_msl_distribution') }}
    where filename not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_msl_distribution__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_msl_distribution__duplicate_test') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_msl_distribution__test_format') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_msl_distribution__test_date_format_odd_eve_leap') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_msl_distribution__test_format_flag') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_msl_distribution__test_format_null_flag') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_msl_distribution__test_multiple_column') }}
    )
),
final as(
    select   
        cntry_cd as cntry_cd,
        crncy_cd as crncy_cd,
        distributor_id as distributor_id,
        distributor_name as distributor_name,
        re_code as re_code,
        re_name as re_name,
        store_id as store_id,
        store_name as store_name,
        sales_rep_id as sales_rep_id,
        sales_rep_name as sales_rep_name,
        category_code as category_code,
        category as category,
        brand_code as brand_code,
        brand as brand,
        barcode as barcode,
        product_description as product_description,
        survey_date as survey_date,
        no_distribution as no_distribution,
        osa as osa,
        oos as oos,
        oos_reason as oos_reason,
        filename as filename,
        run_id as run_id,
        crt_dttm as crt_dttm
   from source
     {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crt_dttm > (select max(crt_dttm) from {{ this }}) 
 {% endif %}
)
select * from final