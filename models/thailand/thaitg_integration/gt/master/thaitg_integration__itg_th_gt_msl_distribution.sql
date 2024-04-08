{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= "delete from {{this}} where substring(filename,6,8)::integer <= (select distinct substring(filename,6,8)::integer from {{ source('thasdl_raw','sdl_th_gt_msl_distribution') }}) and substring(filename,6,6) = (select distinct substring(filename,6,6) from {{ source('thasdl_raw','sdl_th_gt_msl_distribution') }} )"
    )
}}


with source as(
    select * from {{ source('thasdl_raw','sdl_th_gt_msl_distribution') }}
),
final as(
    select
        cntry_cd:: varchar(5) as cntry_cd,
        crncy_cd::varchar(5) as crncy_cd,
        distributor_id::varchar(10) as distributor_id,
        distributor_name::varchar(150) as distributor_name,
        re_code::varchar(20) as re_code,
        re_name::varchar(100) as re_name,
        store_id::varchar(50) as store_id,
        store_name::varchar(150) as store_name,
        sales_rep_id::varchar(50) as sales_rep_id,
        sales_rep_name::varchar(150) as sales_rep_name,
        category_code::varchar(50) as category_code,
        category::varchar(100) as category,
        brand_code::varchar(50) as brand_code,
        brand::varchar(100) as brand,
        barcode::varchar(50) as barcode,
        product_description::varchar(150) as product_description,
        try_to_date(survey_date) as survey_date,
        no_distribution::varchar(10) as no_distribution,
        osa::varchar(10) as osa,
        oos::varchar(10) as oos,
        oos_reason::varchar(255) as oos_reason,
        filename::varchar(100) as filename,
        run_id::varchar(100) as run_id,
        current_timestamp()::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final