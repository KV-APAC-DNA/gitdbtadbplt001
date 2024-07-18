with itg_kr_coupang_search_keyword_by_product as (
select * from {{ ref('ntaitg_integration__itg_kr_coupang_search_keyword_by_product') }}
),
itg_kr_coupang_search_keyword_by_category as (
select * from {{ ref('ntaitg_integration__itg_kr_coupang_search_keyword_by_category') }}
),
final as (
SELECT 
  'KR' :: character varying AS ctry_cd, 
  'SOUTH KOREA' :: character varying AS ctry_nm, 
  derived_table1.data_source, 
  to_date(derived_table1.reference_date,'YYYYMMDD'):: date AS reference_date, 
  derived_table1.data_granularity, 
  derived_table1.category_depth1, 
  derived_table1.category_depth2, 
  derived_table1.category_depth3, 
  derived_table1.ranking, 
  derived_table1.search_keyword, 
  derived_table1.product_name, 
  derived_table1.by_search_keyword, 
  derived_table1.by_product_ranking, 
  derived_table1.product_ranking, 
  derived_table1.jnj_product_flag, 
  derived_table1.click_rate, 
  derived_table1.cart_transition_rate, 
  derived_table1.purchase_conversion_rate 
FROM 
  (
    SELECT 
      'search_keyword_by_category' :: character varying AS data_source, 
      itg_kr_coupang_search_keyword_by_category.yearmo AS reference_date, 
      itg_kr_coupang_search_keyword_by_category.data_granularity, 
      itg_kr_coupang_search_keyword_by_category.category_depth1, 
      itg_kr_coupang_search_keyword_by_category.category_depth2, 
      itg_kr_coupang_search_keyword_by_category.category_depth3, 
      itg_kr_coupang_search_keyword_by_category.ranking, 
      itg_kr_coupang_search_keyword_by_category.search_keyword, 
      itg_kr_coupang_search_keyword_by_category.product_name, 
      itg_kr_coupang_search_keyword_by_category.by_search_keyword, 
      itg_kr_coupang_search_keyword_by_category.by_product_ranking, 
      itg_kr_coupang_search_keyword_by_category.product_ranking, 
      itg_kr_coupang_search_keyword_by_category.jnj_product_flag, 
      (
        (0):: numeric
      ):: numeric(18, 0) AS click_rate, 
      (
        (0):: numeric
      ):: numeric(18, 0) AS cart_transition_rate, 
      (
        (0):: numeric
      ):: numeric(18, 0) AS purchase_conversion_rate 
    FROM 
      itg_kr_coupang_search_keyword_by_category 
    UNION ALL 
    SELECT 
      'search_keyword_by_product' :: character varying AS data_source, 
      itg_kr_coupang_search_keyword_by_product.yearmo AS reference_date, 
      itg_kr_coupang_search_keyword_by_product.data_granularity, 
      itg_kr_coupang_search_keyword_by_product.category_depth1, 
      itg_kr_coupang_search_keyword_by_product.category_depth2, 
      itg_kr_coupang_search_keyword_by_product.category_depth3, 
      itg_kr_coupang_search_keyword_by_product.ranking, 
      itg_kr_coupang_search_keyword_by_product.search_keyword, 
      itg_kr_coupang_search_keyword_by_product.product_name, 
      '' :: character varying AS by_search_keyword, 
      '' :: character varying AS by_product_ranking, 
      '' :: character varying AS product_ranking, 
      '' :: character varying AS jnj_product_flag, 
      itg_kr_coupang_search_keyword_by_product.click_rate, 
      itg_kr_coupang_search_keyword_by_product.cart_transition_rate, 
      itg_kr_coupang_search_keyword_by_product.purchase_conversion_rate 
    FROM 
      itg_kr_coupang_search_keyword_by_product
  ) derived_table1
)
select * from final
