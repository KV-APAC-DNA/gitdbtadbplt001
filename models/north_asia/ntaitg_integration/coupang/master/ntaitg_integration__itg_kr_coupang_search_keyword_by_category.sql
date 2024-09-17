c{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= "{% if is_incremental() %}
        delete from {{this}}  where trim(category_depth1)||trim(category_depth2)
        ||trim(category_depth3)||trim(all_brand)||trim(coupang_sku_id)||trim(coupang_sku_name)
        ||trim(ranking)||trim(jnj_product_flag)||trim(yearmo)||trim(category_depth2)||trim(category_depth3)
        ||trim(ranking)||trim(search_keyword)||trim(product_ranking)||trim(product_name)||trim(jnj_product_flag)||trim(yearmo)||trim(data_granularity) 
        in (select distinct trim(by_search_keyword)
        ||trim(by_product_ranking)||trim(category_depth1)
        ||trim(category_depth2)||trim(category_depth3)
        ||trim(ranking)||trim(search_keyword)||trim(product_ranking)
        ||trim(product_name)||trim(jnj_product_flag)||trim(yearmo)
        ||trim(data_granularity) from 
        {{ source('ntasdl_raw', 'sdl_kr_coupang_search_keyword_by_category') }});
        {% endif %}"
)
}}
with sdl_kr_coupang_search_keyword_by_category as (
select *, dense_rank()over(partition by trim(category_depth1),trim(category_depth2),trim(category_depth3),trim(all_brand),
        ,trim(coupang_sku_id),trim(coupang_sku_name)
        ,trim(ranking),trim(jnj_product_flag),trim(yearmo),trim(category_depth2),trim(category_depth3)
        ,trim(ranking),trim(search_keyword),trim(product_ranking),trim(product_name),trim(jnj_product_flag),trim(yearmo),trim(data_granularity)order by file_name desc) 
from {{ source('ntasdl_raw', 'sdl_kr_coupang_search_keyword_by_category') }}
where file_name not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_coupang_search_keyword_by_category__null_test') }}
    )
qualify rnk =1 
),
final as (
SELECT 
by_search_keyword::varchar(255) as by_search_keyword,
by_product_ranking::varchar(255) as by_product_ranking,
category_depth1::varchar(255) as category_depth1,
category_depth2::varchar(255) as category_depth2,
category_depth3::varchar(255) as category_depth3,
ranking::varchar(255) as ranking,
search_keyword::varchar(2000) as search_keyword,
product_ranking::varchar(255) as product_ranking,
product_name::varchar(2000) as product_name,
jnj_product_flag::varchar(255) as jnj_product_flag,
run_id::number(14,0) as run_id,
file_name::varchar(255) as file_name,
yearmo::varchar(255) as yearmo,
data_granularity::varchar(255) as data_granularity,
current_timestamp()::timestamp_ntz(9) as updt_dttm
FROM sdl_kr_coupang_search_keyword_by_category)
select * from final