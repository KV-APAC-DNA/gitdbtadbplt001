--Import CTE
with v_edw_sg_rpt_retail_excellence as (
    select * from {{ ref('sgpedw_integration__edw_sg_rpt_retail_excellence') }}
),
--Logical CTE

--Final CTE
final as (
select fisc_yr,
       cast(fisc_per as numeric(18,0) ) as fisc_per,		--// integer
       "cluster",
       market,
       md5(nvl(sell_out_channel,'soc')||nvl(retail_environment,'re')||nvl(region,'reg')||nvl(zone_name,'zn')||
		   nvl(city,'cty')||nvl(prod_hier_l1,'ph1')||nvl(prod_hier_l2,'ph2')||
		   nvl(prod_hier_l3,'ph3')||nvl(prod_hier_l4,'ph4')||nvl(prod_hier_l5,'ph5')||
		   nvl(global_product_franchise,'gpf')||nvl(global_product_brand,'gpb')||
		   nvl(global_product_sub_brand,'gpsb')||nvl(global_product_segment,'gps')||
		   nvl(global_product_subsegment,'gpss')||nvl(global_product_category,'gpc')||
		   nvl(global_product_subcategory,'gpsc')) as flag_agg_dim_key,
       data_src,
       distributor_code,
       distributor_name,
       sell_out_channel,
       region,
       zone_name,
       city,
       retail_environment,
       prod_hier_l1,
       prod_hier_l2,
       prod_hier_l3,
       prod_hier_l4,
       prod_hier_l5,
       null as prod_hier_l6,
       null as prod_hier_l7,
       null as prod_hier_l8,
       null as prod_hier_l9,
       global_product_franchise,
       global_product_brand,
       global_product_sub_brand,
       global_product_segment,
       global_product_subsegment,
       global_product_category,
       global_product_subcategory,
       store_code,
       product_code,
       sum(sales_value)as sales_value,
       sum(sales_qty)as sales_qty,
       avg(sales_qty) as avg_sales_qty,		--// avg
       sum(lm_sales)as lm_sales,
       sum(lm_sales_qty)as lm_sales_qty,
       avg(lm_sales_qty)as lm_avg_sales_qty,		--// avg
       sum(p3m_sales)as p3m_sales,
       sum(p3m_qty)as p3m_qty,
       avg(p3m_qty)as p3m_avg_qty ,		--// avg
       sum(p6m_sales)as p6m_sales,
       sum(p6m_qty)as p6m_qty,
       avg(p6m_qty)as p6m_avg_qty,		--// avg
       sum(p12m_sales)as p12m_sales,
       sum(p12m_qty)as p12m_qty,
       avg(p12m_qty)as p12m_avg_qty,		--// avg
       sum(f3m_sales)as f3m_sales,
       sum(f3m_qty)as f3m_qty,
       avg(f3m_qty)as f3m_avg_qty,		--// avg
	    max(list_price) as list_price,
       case when sum (case when lm_sales_flag = 'Y' then 1 else 0 end ) >0 then 1 else 0 end as lm_sales_flag,
       case when sum(case when p3m_sales_flag = 'Y' then 1 else 0 end) >0 then 1 else 0 end as  p3m_sales_flag,
       case when sum(case when p6m_sales_flag = 'Y' then 1 else 0 end) >0 then 1 else 0 end as  p6m_sales_flag,
       case when sum(case when p12m_sales_flag= 'Y' then 1 else 0 end ) >0 then 1 else 0 end as p12m_sales_flag,
       case when mdp_flag ='Y' then 1 else 0 end as mdp_flag,
       max(size_of_price_lm)  as   size_of_price_lm,
       max(size_of_price_p3m) as   size_of_price_p3m,
       max(size_of_price_p6m) as   size_of_price_p6m,
       max(size_of_price_p12m) as  size_of_price_p12m,
        sum(sales_value_list_price)as sales_value_list_price,
       sum(lm_sales_lp) as lm_sales_lp,
       sum(p3m_sales_lp) as p3m_sales_lp,
       sum(p6m_sales_lp) as p6m_sales_lp,
       sum(p12m_sales_lp) as p12m_sales_lp,
       max(size_of_price_lm_lp)  as   size_of_price_lm_lp,
       max(size_of_price_p3m_lp) as   size_of_price_p3m_lp,
       max(size_of_price_p6m_lp) as   size_of_price_p6m_lp,
       max(size_of_price_p12m_lp) as  size_of_price_p12m_lp,
       target_complaince
 from v_edw_sg_rpt_retail_excellence flags	--//  from os_edw.edw_sg_rpt_retail_excellence flags
 group by flags.fisc_yr,		--//  group by flags.fisc_yr,
       flags.fisc_per,		--//        flags.fisc_per,
       flags."cluster",		--//        flags.cluster,
       flags.market,		--//        flags.market,
       md5(nvl(sell_out_channel,'soc')||nvl(retail_environment,'re')||nvl(region,'reg')||nvl(zone_name,'zn')||
           nvl(city,'cty')||nvl(prod_hier_l1,'ph1')||nvl(prod_hier_l2,'ph2')||
           nvl(prod_hier_l3,'ph3')||nvl(prod_hier_l4,'ph4')||nvl(prod_hier_l5,'ph5')||
           nvl(global_product_franchise,'gpf')||nvl(global_product_brand,'gpb')||
           nvl(global_product_sub_brand,'gpsb')||nvl(global_product_segment,'gps')||
           nvl(global_product_subsegment,'gpss')||nvl(global_product_category,'gpc')||
           nvl(global_product_subcategory,'gpsc')),
       flags.data_src,		--//        flags.data_src,
       flags.distributor_code,		--//        flags.distributor_code,
       flags.distributor_name,		--//        flags.distributor_name,
       flags.sell_out_channel,		--//        flags.sell_out_channel,
       flags.region,		--//        flags.region,
       flags.zone_name,		--//        flags.zone_name,
       flags.city,		--//        flags.city,
       flags.retail_environment,		--//        flags.retail_environment,
       flags.prod_hier_l1,		--//        flags.prod_hier_l1,
       flags.prod_hier_l2,		--//        flags.prod_hier_l2,
       flags.prod_hier_l3,		--//        flags.prod_hier_l3,
       flags.prod_hier_l4,		--//        flags.prod_hier_l4,
       flags.prod_hier_l5,		--//        flags.prod_hier_l5,
       flags.global_product_franchise,		--//        flags.global_product_franchise,
       flags.global_product_brand,		--//        flags.global_product_brand,
       flags.global_product_sub_brand,		--//        flags.global_product_sub_brand,
       flags.global_product_segment,		--//        flags.global_product_segment,
       flags.global_product_subsegment,		--//        flags.global_product_subsegment,
       flags.global_product_category,		--//        flags.global_product_category,
       flags.global_product_subcategory,		--//        flags.global_product_subcategory,
       (case when mdp_flag ='Y' then 1 else 0 end) ,
       target_complaince ,
       store_code,
       product_code
),
sg_rpt_retail_excellence_summary_base as (
select 
fisc_yr::numeric(18,0) AS fisc_yr,
fisc_per::numeric(18,0) AS fisc_per,
"cluster"::varchar(100) AS cluster,
market::varchar(50) AS market,
flag_agg_dim_key::varchar(32) AS flag_agg_dim_key,
data_src::varchar(3) AS data_src,
distributor_code::varchar(225) AS distributor_code,
distributor_name::varchar(801) AS distributor_name,
sell_out_channel::varchar(112) AS sell_out_channel,
region::varchar(150) AS region,
zone_name::varchar(150) AS zone_name,
city::varchar(11) AS city,
retail_environment::varchar(255) AS retail_environment,
prod_hier_l1::varchar(9) AS prod_hier_l1,
prod_hier_l2::varchar(200) AS prod_hier_l2,
prod_hier_l3::varchar(200) AS prod_hier_l3,
prod_hier_l4::varchar(200) AS prod_hier_l4,
prod_hier_l5::varchar(200) AS prod_hier_l5,
prod_hier_l6::varchar(1) AS prod_hier_l6,
prod_hier_l7::varchar(1) AS prod_hier_l7,
prod_hier_l8::varchar(1) AS prod_hier_l8,
prod_hier_l9::varchar(1) AS prod_hier_l9,
global_product_franchise::varchar(30) AS global_product_franchise,
global_product_brand::varchar(30) AS global_product_brand,
global_product_sub_brand::varchar(100) AS global_product_sub_brand,
global_product_segment::varchar(50) AS global_product_segment,
global_product_subsegment::varchar(100) AS global_product_subsegment,
global_product_category::varchar(50) AS global_product_category,
global_product_subcategory::varchar(50) AS global_product_subcategory,
store_code::varchar(225) AS store_code,
product_code::varchar(225) AS product_code,
sales_value::numeric(38,6) AS sales_value,
sales_qty::numeric(38,6) AS sales_qty,
avg_sales_qty::numeric(38,6) AS avg_sales_qty,
lm_sales::numeric(38,6) AS lm_sales,
lm_sales_qty::numeric(38,6) AS lm_sales_qty,
lm_avg_sales_qty::numeric(38,6) AS lm_avg_sales_qty,
p3m_sales::numeric(38,6) AS p3m_sales,
p3m_qty::numeric(38,6) AS p3m_qty,
p3m_avg_qty::numeric(38,6) AS p3m_avg_qty,
p6m_sales::numeric(38,6) AS p6m_sales,
p6m_qty::numeric(38,6) AS p6m_qty,
p6m_avg_qty::numeric(38,6) AS p6m_avg_qty,
p12m_sales::numeric(38,6) AS p12m_sales,
p12m_qty::numeric(38,6) AS p12m_qty,
p12m_avg_qty::numeric(38,6) AS p12m_avg_qty,
f3m_sales::numeric(38,6) AS f3m_sales,
f3m_qty::numeric(38,6) AS f3m_qty,
f3m_avg_qty::numeric(38,6) AS f3m_avg_qty,
list_price::numeric(20,4) AS list_price,
lm_sales_flag::numeric(18,0) AS lm_sales_flag,
p3m_sales_flag::numeric(18,0) AS p3m_sales_flag,
p6m_sales_flag::numeric(18,0) AS p6m_sales_flag,
p12m_sales_flag::numeric(18,0) AS p12m_sales_flag,
mdp_flag::numeric(18,0) AS mdp_flag,
size_of_price_lm::numeric(38,14) AS size_of_price_lm,
size_of_price_p3m::numeric(38,14) AS size_of_price_p3m,
size_of_price_p6m::numeric(38,14) AS size_of_price_p6m,
size_of_price_p12m::numeric(38,14) AS size_of_price_p12m,
sales_value_list_price::numeric(38,12) AS sales_value_list_price,
lm_sales_lp::numeric(38,12) AS lm_sales_lp,
p3m_sales_lp::numeric(38,12) AS p3m_sales_lp,
p6m_sales_lp::numeric(38,12) AS p6m_sales_lp,
p12m_sales_lp::numeric(38,12) AS p12m_sales_lp,
size_of_price_lm_lp::numeric(38,20) AS size_of_price_lm_lp,
size_of_price_p3m_lp::numeric(38,20) AS size_of_price_p3m_lp,
size_of_price_p6m_lp::numeric(38,20) AS size_of_price_p6m_lp,
size_of_price_p12m_lp::numeric(38,20) AS size_of_price_p12m_lp,
target_complaince::numeric(38,6) AS target_complaince
from   final  
)
--Final select

select * from sg_rpt_retail_excellence_summary_base 
