with itg_kr_coupang_customer_brand_trend as (
select * from {{ ref('ntaitg_integration__itg_kr_coupang_customer_brand_trend') }}
),
final as (
SELECT 
  'KR' :: character varying AS ctry_cd, 
  'SOUTH KOREA' :: character varying AS ctry_nm, 
  'brand_trend' :: character varying AS data_source, 
  to_date(
    (
      (derived_table1.reference_date):: text || ('01' :: character varying):: text
    )
  ,'YYYYMMDD') AS reference_date, 
  derived_table1.data_granularity, 
  derived_table1.category_depth1, 
  derived_table1.all_brand, 
  derived_table1.coupang_id, 
  derived_table1.new_user_count, 
  derived_table1.curr_user_count, 
  derived_table1.tot_user_count, 
  derived_table1.new_user_sales_amt, 
  derived_table1.curr_user_sales_amt, 
  derived_table1.new_user_avg_product_sales_price, 
  derived_table1.curr_user_avg_product_sales_price, 
  derived_table1.tot_user_avg_product_sales_price 
FROM 
  (
    SELECT 
      bt.date_yyyymm AS reference_date, 
      bt.yearmo, 
      bt.data_granularity, 
      bt.category_depth1, 
      bt.brand AS all_brand, 
      bt.date_yyyymm AS brand_trend_date, 
      bt.coupang_id, 
      bt.new_user_count, 
      bt.curr_user_count, 
      bt.tot_user_count, 
      bt.new_user_sales_amt, 
      bt.curr_user_sales_amt, 
      bt.new_user_avg_product_sales_price, 
      bt.curr_user_avg_product_sales_price, 
      bt.tot_user_avg_product_sales_price 
    FROM 
     itg_kr_coupang_customer_brand_trend bt
  ) derived_table1
)
select * from final 
