{{
    config
    (   materialized="incremental",
        incremental_strategy= "append",
        pre_hook = " {% if is_incremental() %}
        DELETE FROM {{this}} 
        WHERE CAST(SUBSTRING(cast(report_date as string), 1, 4) || SUBSTRING(cast(report_date as string), 6, 2) AS INTEGER) IN (
        SELECT DISTINCT CAST(SUBSTRING(cast(report_date as string), 1, 4) || SUBSTRING(cast(report_date as string), 6, 2) AS INTEGER)
        FROM {{source('aspsdl_raw', 'sdl_clavis_gb_search_terms_results')}});
        {% endif %}"
    )
}}
with sdl_clavis_gb_search_terms_results as
(
    select * from {{source('aspsdl_raw', 'sdl_clavis_gb_search_terms_results')}}
),
final as
(
    SELECT 
    client_id::varchar(10) as client_id,
	id::varchar(1000) as id,
	type::varchar(30) as type,
	report_date::date as report_date,
	region::varchar(2) as region,
	search_term::varchar(255) as search_term,
	online_store::varchar(255) as online_store,
	brand::varchar(500) as brand,
	category::varchar(500) as category,
	manufacturer::varchar(500) as manufacturer,
	dimension1::varchar(200) as dimension1,
	dimension2::varchar(200) as dimension2,
	dimension3::varchar(200) as dimension3,
	dimension4::varchar(200) as dimension4,
	dimension5::varchar(200) as dimension5,
	dimension6::varchar(200) as dimension6,
	dimension7::varchar(200) as dimension7,
	dimension8::varchar(200) as dimension8,
	is_competitor::varchar(10) as is_competitor,
	is_priority_search_term::varchar(10) as is_priority_search_term,
	results_per_page::number(38,0) as results_per_page,
	trusted_rpc::varchar(50) as trusted_rpc,
	trusted_upc::varchar(50) as trusted_upc,
	trusted_mpc::varchar(50) as trusted_mpc,
	trusted_product_description::varchar(2000) as trusted_product_description,
	harvested_url::varchar(10000) as harvested_url,
	harvested_product_image_url::varchar(10000) as harvested_product_image_url,
	harvested_product_description::varchar(2000) as harvested_product_description,
	search_results_rank::number(38,0) as search_results_rank,
	search_results_is_paid::varchar(20) as search_results_is_paid,
	search_results_basic_score::number(20,2) as search_results_basic_score,
	search_results_weighted_score::number(20,2) as search_results_weighted_score,
	search_results_priority_score::number(20,2) as search_results_priority_score,
	search_results_segment::varchar(255) as search_results_segment,
	search_results_segment_value::varchar(1000) as search_results_segment_value,
	search_results_max_rank::number(38,0) as search_results_max_rank,
	ext_page_no::number(38,0) as ext_page_no,
	etl_run_id::varchar(30) as etl_run_id,
	etl_load_dttm::timestamp_ntz(9) as etl_load_dttm,
	delete_ind::varchar(1) as delete_ind,
	run_id::number(18,0) as run_id
    from sdl_clavis_gb_search_terms_results
)
select * from final