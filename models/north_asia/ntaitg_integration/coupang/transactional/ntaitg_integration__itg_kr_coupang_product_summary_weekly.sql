{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = " {% if is_incremental() %}
                    delete from {{this}} where trim(category_depth1)||trim(category_depth2)||trim(category_depth3)||trim(all_brand)||trim(coupang_sku_id)||trim(coupang_sku_name)||trim(ranking)||trim(jnj_product_flag)||trim(yearmo)
					in
					(select distinct trim(category_depth1)||trim(category_depth2)||trim(category_depth3)||trim(all_brand)||trim(coupang_sku_id)||trim(coupang_sku_name)||trim(ranking)||trim(jnj_product_flag)||trim(yearmo)
					from {{ source('ntasdl_raw', 'sdl_kr_coupang_product_summary_weekly') }});
                    {% endif %}
                    "
    )
}}
with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_coupang_product_summary_weekly') }}
),
final as
(
	select
		category_depth1::varchar(255) as category_depth1,
		category_depth2::varchar(255) as category_depth2,
		category_depth3::varchar(255) as category_depth3,
		all_brand::varchar(255) as all_brand,
		coupang_sku_id::varchar(255) as coupang_sku_id,
		coupang_sku_name::varchar(20000) as coupang_sku_name,
		ranking::varchar(255) as ranking,
		jnj_product_flag::varchar(255) as jnj_product_flag,
		run_id::number(14,0) as run_id,
		file_name::varchar(255) as file_name,
		yearmo::varchar(255) as yearmo,
		current_timestamp()::timestamp_ntz(9) as updt_dttm
	from source
)
select * from final