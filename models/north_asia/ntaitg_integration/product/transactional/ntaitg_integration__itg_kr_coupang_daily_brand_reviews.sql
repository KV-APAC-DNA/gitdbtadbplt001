
{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= "{% if is_incremental() %}
        delete from {{this}} where trim(review_date)||trim(brand)||trim(coupang_id)||trim(coupang_product_name)||trim(review_score_star) in 
        (select distinct trim(review_date)||trim(brand)||trim(coupang_sku_id)||trim(coupang_sku_name)||trim(review_score_star) 
        from 
        {{ source('ntasdl_raw', 'sdl_kr_coupang_daily_brand_reviews') }} where file_name not in
     (select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_coupang_daily_brand_reviews__null_test') }}
      union all
      select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_coupang_daily_brand_reviews__test_date_format_odd_eve_leap') }}
     ));
        {% endif %}"
)
}}
with sdl_kr_coupang_daily_brand_reviews as (
    select *,dense_rank()over(partition by trim(review_date),trim(brand),trim(coupang_sku_id),trim(coupang_sku_name),trim(review_score_star) order by file_name desc) rnk
    from {{ source('ntasdl_raw', 'sdl_kr_coupang_daily_brand_reviews') }} where file_name not in
     (select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_coupang_daily_brand_reviews__null_test') }}
      union all
      select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_coupang_daily_brand_reviews__test_date_format_odd_eve_leap') }}
     ) qualify rnk = 1

),
final as 
(
    SELECT 	
        review_date::varchar(10) as review_date,
        brand::varchar(30) as brand,
        coupang_sku_id::varchar(15) as coupang_id,
        coupang_sku_name::varchar(200) as coupang_product_name,
        review_score_star::varchar(1) as review_score_star,
        review_contents::varchar(65535) as review_contents,
        run_id::number(14,0) as run_id,
        file_name::varchar(255) as file_name,
        yearmo::varchar(20) as yearmo,
        data_granularity::varchar(10) as data_granularity,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    FROM sdl_kr_coupang_daily_brand_reviews
)
select * from final