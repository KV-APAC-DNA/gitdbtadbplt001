{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["date"]
    )
}}

with source as (
    select * ,dense_rank() over(partition by date order by file_name desc) as rnk 
    from {{ source('thasdl_raw', 'sdl_th_mt_price_data') }}
    where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_mt_price_data__duplicate_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_mt_price_data__null_test') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_mt_price_data__format_test') }}
    ) qualify rnk =1
),
final as (
    select
        company::varchar(50) as company,
        date::timestamp_ntz(9) as date,
        upper(brand)::varchar(50) as brand,
        manufacturer::varchar(100) as manufacturer,
        case when source in ('Watsons','Boots') then trim(SPLIT_PART(product_name,'|',2))::varchar(400) else product_name::varchar(400) end as product_name,
        sku_id::varchar(200) as sku_id,
        list_price::number(20,5) as list_price,
        price::number(20,5) as price,
        category_jnj::varchar(100) as category_jnj,
        sub_category_jnj::varchar(100) as sub_category_jnj,
        category::varchar(200) as category,
        sub_category::varchar(400) as sub_category,
        url::varchar(400) as url,
        review_score::number(20,5) as review_score,
        review_qty::number(20,5) as review_qty,
        discount_depth::number(20,5) as discount_depth,
        source::varchar(20) as source,
        file_name::varchar(255) as file_name,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final