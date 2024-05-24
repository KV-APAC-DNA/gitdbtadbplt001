{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                    delete from {{this}}
					where trim(product_ranking_date)||trim(category_depth1)||trim(category_depth2)||trim(category_depth3)||trim(coupang_sku_id)||trim(coupang_sku_name)||trim(ranking)||trim(data_granularity)
					in
					(select distinct trim(product_ranking_date)||trim(category_depth1)||trim(category_depth2)||trim(category_depth3)||trim(coupang_sku_id)||trim(coupang_sku_name)||trim(ranking)||trim(data_granularity)
					from {{ source('ntasdl_raw', 'sdl_kr_coupang_product_ranking_monthly') }});
                    {% endif %}
                    "
    )
}}
with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_coupang_product_ranking_monthly') }}
),
final as
(	select
		product_ranking_date::varchar(255) as product_ranking_date,
		category_depth1::varchar(255) as category_depth1,
		category_depth2::varchar(255) as category_depth2,
		category_depth3::varchar(255) as category_depth3,
		coupang_sku_id::varchar(255) as coupang_sku_id,
		coupang_sku_name::varchar(20000) as coupang_sku_name,
		ranking::varchar(255) as ranking,
		run_id::number(14,0) as run_id,
		file_name::varchar(255) as file_name,
		yearmo::varchar(255) as yearmo,
		data_granularity::varchar(255) as data_granularity,
		current_timestamp()::timestamp_ntz(9) as updt_dttm
	from source
)
select * from final