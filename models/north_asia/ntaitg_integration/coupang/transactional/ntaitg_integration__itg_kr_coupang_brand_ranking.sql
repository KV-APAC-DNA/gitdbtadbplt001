{{
    config(
        materialized="incremental",
        incremental_strategy = "append",
        pre_hook="{% if is_incremental() %}
        delete from {{this}} where trim(category_depth1)||trim(category_depth2)||trim(category_depth3)||trim(ranking)||trim(brand)||trim(yearmo)||trim(data_granularity) in (select distinct trim(category_depth1)||trim(category_depth2)||trim(category_depth3)||trim(ranking)||trim(brand)||trim(yearmo)||trim(data_granularity) from {{ source('ntasdl_raw', 'sdl_kr_coupang_brand_ranking') }});
        {% endif %}"
    )
}}
with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_coupang_brand_ranking') }}
),
final as (
    SELECT 
        category_depth1::varchar(30) as category_depth1,
        category_depth2::varchar(30) as category_depth2,
        category_depth3::varchar(50) as category_depth3,
        ranking::varchar(4) as ranking,
        brand::varchar(30) as brand,
        jnj_brand::varchar(20) as jnj_brand,
        rank_change::varchar(5) as rank_change,
        run_id::number(14,0) as run_id,
        file_name::varchar(255) as file_name,
        yearmo::varchar(20) as yearmo,
        data_granularity::varchar(10) as data_granularity,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final