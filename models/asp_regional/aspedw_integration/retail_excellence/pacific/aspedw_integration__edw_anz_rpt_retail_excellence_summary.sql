with edw_anz_rpt_retail_excellence_summary_sellout as (
    select * from {{ ref('aspedw_integration__edw_anz_rpt_retail_excellence_summary_sellout') }}
),
edw_anz_rpt_retail_excellence_summary_pos as (
    select * from {{ ref('aspedw_integration__edw_anz_rpt_retail_excellence_summary_pos') }}
),
--Logical CTE

edw_anz_rpt_retail_excellence_summary as (

SELECT * FROM edw_anz_rpt_retail_excellence_summary_sellout UNION
SELECT * FROM edw_anz_rpt_retail_excellence_summary_pos 
),
final as 
(
select
    fisc_yr::VARCHAR(11) AS fisc_yr,
fisc_per::numeric(18,0) AS fisc_per,
"cluster"::VARCHAR(100) AS "cluster",
market::VARCHAR(50) AS market,
data_src::VARCHAR(14) AS data_src,
flag_agg_dim_key::VARCHAR(50) AS flag_agg_dim_key,
distributor_code::VARCHAR(500) AS distributor_code,
distributor_name::VARCHAR(500) AS distributor_name,
sell_out_channel::VARCHAR(255) AS sell_out_channel,
region::VARCHAR(255) AS region,
zone_name::VARCHAR(510) AS zone_name,
city::VARCHAR(255) AS city,
retail_environment::VARCHAR(500) AS retail_environment,
prod_hier_l1::VARCHAR(100) AS prod_hier_l1,
prod_hier_l2::VARCHAR(50) AS prod_hier_l2,
prod_hier_l3::VARCHAR(255) AS prod_hier_l3,
prod_hier_l4::VARCHAR(255) AS prod_hier_l4,
prod_hier_l5::VARCHAR(255) AS prod_hier_l5,
prod_hier_l6::VARCHAR(255) AS prod_hier_l6,
prod_hier_l7::VARCHAR(50) AS prod_hier_l7,
prod_hier_l8::VARCHAR(50) AS prod_hier_l8,
prod_hier_l9::VARCHAR(50) AS prod_hier_l9,
global_product_franchise::VARCHAR(30) AS global_product_franchise,
global_product_brand::VARCHAR(30) AS global_product_brand,
global_product_sub_brand::VARCHAR(100) AS global_product_sub_brand,
global_product_segment::VARCHAR(200) AS global_product_segment,
global_product_subsegment::VARCHAR(100) AS global_product_subsegment,
global_product_category::VARCHAR(200) AS global_product_category,
global_product_subcategory::VARCHAR(200) AS global_product_subcategory,
lm_sales_flag::VARCHAR(1) AS lm_sales_flag,
p3m_sales_flag::VARCHAR(1) AS p3m_sales_flag,
p6m_sales_flag::VARCHAR(1) AS p6m_sales_flag,
p12m_sales_flag::VARCHAR(1) AS p12m_sales_flag,
mdp_flag::VARCHAR(1) AS mdp_flag,
target_complaince::numeric(38,6) AS target_complaince,
sales_value::NUMERIC(38,6) AS sales_value,
sales_qty::NUMERIC(38,6) AS sales_qty,
avg_sales_qty::NUMERIC(38,6) AS avg_sales_qty,
lm_sales::NUMERIC(38,6) AS lm_sales,
lm_sales_qty::NUMERIC(38,6) AS lm_sales_qty,
lm_avg_sales_qty::NUMERIC(38,6) AS lm_avg_sales_qty,
p3m_sales::NUMERIC(38,6) AS p3m_sales,
p3m_qty::NUMERIC(38,6) AS p3m_qty,
p3m_avg_qty::NUMERIC(38,6) AS p3m_avg_qty,
p6m_sales::NUMERIC(38,6) AS p6m_sales,
p6m_qty::NUMERIC(38,6) AS p6m_qty,
p6m_avg_qty::NUMERIC(38,6) AS p6m_avg_qty,
p12m_sales::NUMERIC(38,6) AS p12m_sales,
p12m_qty::NUMERIC(38,6) AS p12m_qty,
p12m_avg_qty::NUMERIC(38,6) AS p12m_avg_qty,
f3m_sales::NUMERIC(38,6) AS f3m_sales,
f3m_qty::NUMERIC(38,6) AS f3m_qty,
f3m_avg_qty::NUMERIC(38,6) AS f3m_avg_qty,
size_of_price_lm::NUMERIC(38,14) AS size_of_price_lm,
size_of_price_p3m::NUMERIC(38,14) AS size_of_price_p3m,
size_of_price_p6m::NUMERIC(38,14) AS size_of_price_p6m,
size_of_price_p12m::NUMERIC(38,14) AS size_of_price_p12m,
lm_sales_flag_count::numeric(38,0) AS lm_sales_flag_count,
p3m_sales_flag_count::numeric(38,0) AS p3m_sales_flag_count,
p6m_sales_flag_count::numeric(38,0) AS p6m_sales_flag_count,
p12m_sales_flag_count::numeric(38,0) AS p12m_sales_flag_count,
mdp_flag_count::numeric(38,0) AS mdp_flag_count,
list_price::NUMERIC(20,4) AS list_price,
sales_value_list_price::NUMERIC(38,6) AS sales_value_list_price,
lm_sales_lp::NUMERIC(38,6) AS lm_sales_lp,
p3m_sales_lp::NUMERIC(38,6) AS p3m_sales_lp,
p6m_sales_lp::NUMERIC(38,6) AS p6m_sales_lp,
p12m_sales_lp::NUMERIC(38,6) AS p12m_sales_lp,
size_of_price_lm_lp::NUMERIC(38,14) AS size_of_price_lm_lp,
size_of_price_p3m_lp::NUMERIC(38,14) AS size_of_price_p3m_lp,
size_of_price_p6m_lp::NUMERIC(38,14) AS size_of_price_p6m_lp,
size_of_price_p12m_lp::NUMERIC(38,14) AS size_of_price_p12m_lp,
crt_dttm::TIMESTAMP WITHOUT TIME ZONE AS crt_dttm,
cm_actual_stores::NUMERIC(38,6) AS cm_actual_stores,
cm_universe_stores::NUMERIC(38,6) AS cm_universe_stores,
cm_numeric_distribution::NUMERIC(38,6) AS cm_numeric_distribution,
lm_actual_stores::NUMERIC(38,6) AS lm_actual_stores,
lm_universe_stores::NUMERIC(38,6) AS lm_universe_stores,
lm_numeric_distribution::NUMERIC(38,6) AS lm_numeric_distribution,
l3m_actual_stores::NUMERIC(38,6) AS l3m_actual_stores,
l3m_universe_stores::NUMERIC(38,6) AS l3m_universe_stores,
l3m_numeric_distribution::NUMERIC(38,6) AS l3m_numeric_distribution,
l6m_actual_stores::NUMERIC(38,6) AS l6m_actual_stores,
l6m_universe_stores::NUMERIC(38,6) AS l6m_universe_stores,
l6m_numeric_distribution::NUMERIC(38,6) AS l6m_numeric_distribution,
l12m_actual_stores::NUMERIC(38,6) AS l12m_actual_stores,
l12m_universe_stores::NUMERIC(38,6) AS l12m_universe_stores,
l12m_numeric_distribution::NUMERIC(38,6) AS l12m_numeric_distribution
 from edw_anz_rpt_retail_excellence_summary
)
--Final select
select * from final 