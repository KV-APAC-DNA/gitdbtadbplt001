--import cte
with edw_vw_cal_retail_excellence_dim as (
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),
wks_anz_sellout_re_act_lm as (
    select * from {{ ref('pcfwks_integration__wks_anz_sellout_re_act_lm') }}
),
wks_anz_sellout_re_act_l3m as (
    select * from {{ ref('pcfwks_integration__wks_anz_sellout_re_act_l3m') }}
),

wks_anz_sellout_re_act_l6m as (
    select * from {{ ref('pcfwks_integration__wks_anz_sellout_re_act_l6m') }}
),

wks_anz_sellout_re_act_l12m as (
    select * from {{ ref('pcfwks_integration__wks_anz_sellout_re_act_l12m') }}
),

wks_anz_sellout_base_retail_excellence as (
    select * from {{ ref('pcfwks_integration__wks_anz_sellout_base_retail_excellence') }}
),

--final cte
anz_sellout_re_actuals  as (
select re_base_dim.cntry_cd,
       re_base_dim.cntry_nm,
       re_base_dim.data_src,
       substring(base_dim.month,1,4) as year,
       base_dim.month as mnth_id,
       re_base_dim.soldto_code,
       re_base_dim.distributor_code,
       re_base_dim.distributor_name,
       re_base_dim.store_code,
       re_base_dim.ean,
       store_name,
       retail_environment,
	   store_type,store_grade,list_price,msl_product_desc,sku_code,
	   region,zone_or_area,
       channel_name,
       sap_parent_customer_key,
       sap_parent_customer_description,
       sap_customer_channel_key,
       sap_customer_channel_description,
       sap_customer_sub_channel_key,
       sap_sub_channel_description,
       sap_go_to_mdl_key,
       sap_go_to_mdl_description,
       sap_banner_key,
       sap_banner_description,
       sap_banner_format_key,
       sap_banner_format_description,
       customer_segment_key,
       customer_segment_description,
       global_product_franchise,
       global_product_brand,
       global_product_sub_brand,
       global_product_variant,
       global_product_segment,
       global_product_subsegment,
       global_product_category,
       global_product_subcategory,
       global_put_up_description,
       pka_product_key,
       pka_product_key_description,
       cm.so_sls_qty as cm_sales_qty,
       cm.so_sls_value as cm_sales,
       cm.so_avg_qty as cm_avg_sales_qty,
       cm.sales_value_list_price as sales_value_list_price,
       lm_sales as lm_sales,
       lm_sales_qty as lm_sales_qty,
       lm_avg_sales_qty as lm_avg_sales_qty,
       lm_sales_lp as lm_sales_lp,
       l3m_sales as p3m_sales,
       l3m_sales_qty as p3m_qty,
       l3m_avg_sales_qty as p3m_avg_qty,
       l3m_sales_lp as p3m_sales_lp,
       f3m_sales as f3m_sales,
       f3m_sales_qty as f3m_qty,
       f3m_avg_sales_qty as f3m_avg_qty,
       l6m_sales as p6m_sales,
       l6m_sales_qty as p6m_qty,
       l6m_avg_sales_qty as p6m_avg_qty,
       l6m_sales_lp as p6m_sales_lp,
       l12m_sales as p12m_sales,
       l12m_sales_qty as p12m_qty,
       l12m_avg_sales_qty as p12m_avg_qty,
       l12m_sales_lp as p12m_sales_lp,
       case
         when lm_sales > 0 then 'y'
         else 'n'
       end as lm_sales_flag,
       case
         when p3m_sales > 0 then 'y'
         else 'n'
       end as p3m_sales_flag,
       case
         when p6m_sales > 0 then 'y'
         else 'n'
       end as p6m_sales_flag,
       case
         when p12m_sales > 0 then 'y'
         else 'n'
       end as p12m_sales_flag,
       sysdate() as crt_dttm
from (select distinct cntry_cd,
             sellout_dim_key,
             month
      from (select cntry_cd,
                   sellout_dim_key,
                   month
            from wks_anz_sellout_re_act_lm
            where lm_sales is not null
            union all
            select cntry_cd,
                   sellout_dim_key,
                   month
            from wks_anz_sellout_re_act_l3m
            where l3m_sales is not null
            union all
            select cntry_cd,
                   sellout_dim_key,
                   month
            from wks_anz_sellout_re_act_l6m
            where l6m_sales is not null
            union all
            select cntry_cd,
                   sellout_dim_key,
                   month
            from wks_anz_sellout_re_act_l12m
            where l12m_sales is not null)) base_dim
  left join (select distinct cntry_cd,
                    cntry_nm,
                    data_src,
                    sellout_dim_key,
                    distributor_code,
                    distributor_name,
                    soldto_code,
                    store_code,
                    ean,
                    store_name,
                    retail_environment,
					store_type,store_grade,list_price,msl_product_desc,sku_code,
					region,zone_or_area,
                    channel_name,
                    sap_parent_customer_key,
                    sap_parent_customer_description,
                    sap_customer_channel_key,
                    sap_customer_channel_description,
                    sap_customer_sub_channel_key,
                    sap_sub_channel_description,
                    sap_go_to_mdl_key,
                    sap_go_to_mdl_description,
                    sap_banner_key,
                    sap_banner_description,
                    sap_banner_format_key,
                    sap_banner_format_description,
                    customer_segment_key,
                    customer_segment_description,
                    global_product_franchise,
                    global_product_brand,
                    global_product_sub_brand,
                    global_product_variant,
                    global_product_segment,
                    global_product_subsegment,
                    global_product_category,
                    global_product_subcategory,
                    global_put_up_description,
                    pka_product_key,
                    pka_product_key_description
             from wks_anz_sellout_base_retail_excellence where  mnth_id >= (select last_28mnths from edw_vw_cal_retail_excellence_dim)::numeric
	  and mnth_id <= (select last_2mnths from edw_vw_cal_retail_excellence_dim)::numeric) re_base_dim
         on re_base_dim.cntry_cd = base_dim.cntry_cd
        and re_base_dim.sellout_dim_key = base_dim.sellout_dim_key
  left outer join (select distinct cntry_cd,
                          sellout_dim_key,
                          mnth_id,
                          so_sls_qty,
                          so_sls_value,
                          so_avg_qty,
                          sales_value_list_price
                   from wks_anz_sellout_base_retail_excellence where  mnth_id >= (select last_28mnths from edw_vw_cal_retail_excellence_dim)::numeric
	  and mnth_id <= (select last_2mnths from edw_vw_cal_retail_excellence_dim)::numeric) cm
               on base_dim.cntry_cd = cm.cntry_cd
              and base_dim.month = cm.mnth_id
              and base_dim.sellout_dim_key = cm.sellout_dim_key
  left outer join
--last month
wks_anz_sellout_re_act_lm lm
               on base_dim.cntry_cd = lm.cntry_cd
              and base_dim.month = lm.month
              and base_dim.sellout_dim_key = lm.sellout_dim_key
  left outer join
--l3m
wks_anz_sellout_re_act_l3m l3m
               on base_dim.cntry_cd = l3m.cntry_cd
              and base_dim.month = l3m.month
              and base_dim.sellout_dim_key = l3m.sellout_dim_key
  left outer join
--l6m
wks_anz_sellout_re_act_l6m l6m
               on base_dim.cntry_cd = l6m.cntry_cd
              and base_dim.month = l6m.month
              and base_dim.sellout_dim_key = l6m.sellout_dim_key
  left outer join
--l12m
wks_anz_sellout_re_act_l12m l12m
               on base_dim.cntry_cd = l12m.cntry_cd
              and base_dim.month = l12m.month
              and base_dim.sellout_dim_key = l12m.sellout_dim_key
			  
where base_dim.month >= (select last_17mnths from edw_vw_cal_retail_excellence_dim)::numeric
  and base_dim.month <= (select last_2mnths from edw_vw_cal_retail_excellence_dim)::numeric 		
),
final as(
select 
cntry_cd::VARCHAR(2) AS cntry_cd,		
cntry_nm::VARCHAR(50) AS cntry_nm,
data_src::VARCHAR(14) AS data_src,
year::VARCHAR(16) AS year,
mnth_id::VARCHAR(23) AS mnth_id,
soldto_code::VARCHAR(255) AS soldto_code,
distributor_code::VARCHAR(32) AS distributor_code,
distributor_name::VARCHAR(255) AS distributor_name,
store_code::VARCHAR(100) AS store_code,
ean::VARCHAR(150) AS ean,
store_name::VARCHAR(601) AS store_name,
retail_environment::VARCHAR(225) AS retail_environment,
store_type::VARCHAR(255) AS store_type,
store_grade::VARCHAR(150) AS store_grade,
list_price::NUMERIC(38,6) AS list_price,
msl_product_desc::VARCHAR(300) AS msl_product_desc,
sku_code::VARCHAR(40) AS sku_code,
region::VARCHAR(150) AS region,
zone_or_area::VARCHAR(150) AS zone_or_area,
channel_name::VARCHAR(150) AS channel_name,
sap_parent_customer_key::VARCHAR(12) AS sap_parent_customer_key,
sap_parent_customer_description::VARCHAR(75) AS sap_parent_customer_description,
sap_customer_channel_key::VARCHAR(12) AS sap_customer_channel_key,
sap_customer_channel_description::VARCHAR(75) AS sap_customer_channel_description,
sap_customer_sub_channel_key::VARCHAR(12) AS sap_customer_sub_channel_key,
sap_sub_channel_description::VARCHAR(75) AS sap_sub_channel_description,
sap_go_to_mdl_key::VARCHAR(12) AS sap_go_to_mdl_key,
sap_go_to_mdl_description::VARCHAR(75) AS sap_go_to_mdl_description,
sap_banner_key::VARCHAR(12) AS sap_banner_key,
sap_banner_description::VARCHAR(75) AS sap_banner_description,
sap_banner_format_key::VARCHAR(12) AS sap_banner_format_key,
sap_banner_format_description::VARCHAR(75) AS sap_banner_format_description,
customer_segment_key::VARCHAR(12) AS customer_segment_key,
customer_segment_description::VARCHAR(50) AS customer_segment_description,
global_product_franchise::VARCHAR(30) AS global_product_franchise,
global_product_brand::VARCHAR(30) AS global_product_brand,
global_product_sub_brand::VARCHAR(100) AS global_product_sub_brand,
global_product_variant::VARCHAR(100) AS global_product_variant,
global_product_segment::VARCHAR(50) AS global_product_segment,
global_product_subsegment::VARCHAR(100) AS global_product_subsegment,
global_product_category::VARCHAR(50) AS global_product_category,
global_product_subcategory::VARCHAR(50) AS global_product_subcategory,
global_put_up_description::VARCHAR(100) AS global_put_up_description,
pka_product_key::VARCHAR(68) AS pka_product_key,
pka_product_key_description::VARCHAR(255) AS pka_product_key_description,
cm_sales_qty::NUMERIC(38,6) AS cm_sales_qty,
cm_sales::NUMERIC(38,6) AS cm_sales,
cm_avg_sales_qty::NUMERIC(10,2) AS cm_avg_sales_qty,
sales_value_list_price::NUMERIC(38,12) AS sales_value_list_price,
lm_sales::NUMERIC(38,6) AS lm_sales,
lm_sales_qty::NUMERIC(38,6) AS lm_sales_qty,
lm_avg_sales_qty::NUMERIC(10,2) AS lm_avg_sales_qty,
lm_sales_lp::NUMERIC(38,12) AS lm_sales_lp,
p3m_sales::NUMERIC(38,6) AS p3m_sales,
p3m_qty::NUMERIC(38,6) AS p3m_qty,
p3m_avg_qty::NUMERIC(38,6) AS p3m_avg_qty,
p3m_sales_lp::NUMERIC(38,12) AS p3m_sales_lp,
f3m_sales::NUMERIC(38,6) AS f3m_sales,
f3m_qty::NUMERIC(38,6) AS f3m_qty,
f3m_avg_qty::NUMERIC(38,6) AS f3m_avg_qty,
p6m_sales::NUMERIC(38,6) AS p6m_sales,
p6m_qty::NUMERIC(38,6) AS p6m_qty,
p6m_avg_qty::NUMERIC(38,6) AS p6m_avg_qty,
p6m_sales_lp::NUMERIC(38,12) AS p6m_sales_lp,
p12m_sales::NUMERIC(38,6) AS p12m_sales,
p12m_qty::NUMERIC(38,6) AS p12m_qty,
p12m_avg_qty::NUMERIC(38,6) AS p12m_avg_qty,
p12m_sales_lp::NUMERIC(38,12) AS p12m_sales_lp,
lm_sales_flag::VARCHAR(1) AS lm_sales_flag,
p3m_sales_flag::VARCHAR(1) AS p3m_sales_flag,
p6m_sales_flag::VARCHAR(1) AS p6m_sales_flag,
p12m_sales_flag::VARCHAR(1) AS p12m_sales_flag,
crt_dttm::timestamp as crt_dttm
  from anz_sellout_re_actuals
)
--final select
select * from final