{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= "{% if is_incremental() %}
        delete from {{this}} where substring(file_name,46,11) in (select distinct substring(file_name,46,11) from {{ source('ntasdl_raw', 'sdl_kr_dads_coupang_search_keyword') }});
        {% endif %}"
)
}}
with sdl_kr_dads_coupang_search_keyword as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_dads_coupang_search_keyword') }}
),
final as (
    SELECT 
        by_search_term_ranking::varchar(250) as search_keyword_criteria,
        product_ranking_criteria::varchar(250) as product_keyword_criteria,
        category_1::varchar(250) as category1,
        category_2::varchar(250) as category2,
        category_3::varchar(250) as category3,
        ranking::varchar(250) as ranking,
        query::varchar(250) as search_keyword,
        product_standings::varchar(250) as product_ranking,
        goods::varchar(500) as product_name,
        my_products::varchar(250) as my_product,
        file_name::varchar(255) as file_name,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        file_date::varchar(10) as file_date
    from sdl_kr_dads_coupang_search_keyword
)
select * from final