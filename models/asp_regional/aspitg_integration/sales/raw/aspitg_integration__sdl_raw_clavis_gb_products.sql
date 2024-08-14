{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}

with source as (
    select * from {{ source('aspsdl_raw', 'sdl_clavis_gb_products')}}
),
final as
 (
    select
    client_id::varchar(10) as client_id,
	report_date::date as report_date,
	region::varchar(2) as region,
	product_id::varchar(150) as product_id,
	api_type::varchar(30) as api_type,
	online_store::varchar(50) as online_store,
	brand::varchar(100) as brand,
	category::varchar(50) as category,
	manufacturer::varchar(50) as manufacturer,
	is_competitor::varchar(50) as is_competitor,
	dimension1::varchar(100) as dimension1,
	dimension2::varchar(100) as dimension2,
	dimension3::varchar(100) as dimension3,
	dimension4::varchar(100) as dimension4,
	dimension5::varchar(100) as dimension5,
	dimension6::varchar(100) as dimension6,
	dimension7::varchar(100) as dimension7,
	dimension8::varchar(100) as dimension8,
	dimension9::varchar(100) as dimension9,
	dimension10::varchar(100) as dimension10,
	rpc::varchar(50) as rpc,
	upc::varchar(20) as upc,
	mpc::varchar(500) as mpc,
	product_desc::varchar(1000) as product_desc,
	url::varchar(2000) as url,
	product_image_url::varchar(2000) as product_image_url,
	avail_status::varchar(20) as avail_status,
	avail_comp::number(38,0) as avail_comp,
	search_score_comp::number(38,0) as search_score_comp,
	overall_rating::number(15,2) as overall_rating,
	rating_comp::number(38,0) as rating_comp,
	review_count::number(38,0) as review_count,
	review_comp::number(38,0) as review_comp,
	content_comp::number(38,0) as content_comp,
	milestone1_comp::number(38,0) as milestone1_comp,
	milestone2_comp::number(38,0) as milestone2_comp,
	milestone3_comp::number(38,0) as milestone3_comp,
	milestone4_comp::number(38,0) as milestone4_comp,
	milestone5_comp::number(38,0) as milestone5_comp,
	milestone6_comp::number(38,0) as milestone6_comp,
	milestone7_comp::number(38,0) as milestone7_comp,
	milestone8_comp::number(38,0) as milestone8_comp,
	milestone9_comp::number(38,0) as milestone9_comp,
	milestone10_comp::number(38,0) as milestone10_comp,
	imaging_comp::number(38,0) as imaging_comp,
	imaging_status::varchar(20) as imaging_status,
	menu_comp::number(38,0) as menu_comp,
	menu_position::number(15,2) as menu_position,
	price_comp::number(15,2) as price_comp,
	price_currency_symbol::varchar(10) as price_currency_symbol,
	observed_price::number(15,2) as observed_price,
	prev_observed_price::number(15,2) as prev_observed_price,
	diff_from_prev_observed_price::number(15,2) as diff_from_prev_observed_price,
	diff_from_prev_observed_price_percent::number(15,2) as diff_from_prev_observed_price_percent,
	msrp::number(15,2) as msrp,
	min_price::number(15,2) as min_price,
	max_price::number(15,2) as max_price,
	diff_from_msrp::number(15,2) as diff_from_msrp,
	diff_from_msrp_percent::number(15,2) as diff_from_msrp_percent,
	diff_from_min_max::number(15,2) as diff_from_min_max,
	diff_from_min_max_percent::number(15,2) as diff_from_min_max_percent,
	is_on_promotion::number(38,0) as is_on_promotion,
	promo_text::varchar(2000) as promo_text,
	promo_type::varchar(255) as promo_type,
	promo_discounted_price::number(15,2) as promo_discounted_price,
	promo_discount_amount::number(15,2) as promo_discount_amount,
	promo_discount_percent::number(15,2) as promo_discount_percent,
	promo_non_discounted_price::number(15,2) as promo_non_discounted_price,
	traffic_page_views::number(38,0) as traffic_page_views,
	sales_currency_symbol::varchar(10) as sales_currency_symbol,
	ext_page_no::number(38,0) as ext_page_no,
	etl_run_id::varchar(30) as etl_run_id,
	etl_load_dttm::timestamp_ntz(9) as etl_load_dttm,
	delete_ind::varchar(1) as delete_ind,
	run_id::number(18,0) as run_id
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.etl_load_dttm > (select max(etl_load_dttm) from {{ this }}) 
    {% endif %}
     
 )
select * from final