{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= " {% if is_incremental() %}
        delete from {{this}} where trim(category_depth1)||trim(category_depth2)||trim(category_depth3)||trim(product_name)||trim(ranking)||trim(search_keyword)||trim(yearmo)||trim(data_granularity) in (select distinct trim(category_depth1)||trim(category_depth2)||trim(category_depth3)||trim(product_name)||trim(ranking)||trim(search_keyword)||trim(yearmo)||trim(data_granularity) from {{ source('ntasdl_raw', 'sdl_kr_coupang_search_keyword_by_product') }});
        {% endif %}"
)
}}
with sdl_kr_coupang_search_keyword_by_category as (
select * from {{ source('ntasdl_raw', 'sdl_kr_coupang_search_keyword_by_product') }}
),
final as (
SELECT 
category_depth1::varchar(255) as category_depth1,
category_depth2::varchar(255) as category_depth2,
category_depth3::varchar(255) as category_depth3,
product_name::varchar(2000) as product_name,
ranking::varchar(255) as ranking,
search_keyword::varchar(255) as search_keyword,
click_rate::number(18,5) as click_rate,
cart_transition_rate::number(18,5) as cart_transition_rate,
purchase_conversion_rate::number(18,5) as purchase_conversion_rate,
run_id::number(14,0) as run_id,
file_name::varchar(255) as file_name,
yearmo::varchar(255) as yearmo,
data_granularity::varchar(255) as data_granularity,
current_timestamp()::timestamp_ntz(9) as updt_dttm
FROM sdl_kr_coupang_search_keyword_by_product)
select * from final