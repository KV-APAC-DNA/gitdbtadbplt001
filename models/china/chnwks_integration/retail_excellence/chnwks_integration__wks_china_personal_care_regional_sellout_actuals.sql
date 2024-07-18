with wks_china_personal_care_base_retail_excellence 
as (select * from {{ref('chnwks_integration__wks_china_personal_care_base_retail_excellence')}}),

edw_vw_cal_retail_excellence_dim 
as (select * from {{ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim')}}),

wks_china_personal_care_rebase_dim_values
as (select * from {{ref('chnwks_integration__wks_china_personal_care_rebase_dim_values')}}),

wks_china_personal_care_regional_sellout_basedim
as (select * from {{ref('chnwks_integration__wks_china_personal_care_regional_sellout_basedim')}}),

wks_china_personal_care_regional_sellout_act_lm
as (select * from {{ref('chnwks_integration__wks_china_personal_care_regional_sellout_act_lm')}}),

wks_china_personal_care_regional_sellout_act_l3m
as (select * from {{ref('chnwks_integration__wks_china_personal_care_regional_sellout_act_l3m')}}),

wks_china_personal_care_regional_sellout_act_l6m
as (select * from {{ref('chnwks_integration__wks_china_personal_care_regional_sellout_act_l6m')}}),

wks_china_personal_care_regional_sellout_act_l12m
as (select * from {{ref('chnwks_integration__wks_china_personal_care_regional_sellout_act_l12m')}}),

wks_china_personal_care_actuals as
(
    select re_base_dim.cntry_cd,
       re_base_dim.cntry_nm,
       substring(base_dim.month,1,4) as "year",
       base_dim.month as mnth_id,
	   re_base_dim.soldto_code,
       re_base_dim.distributor_code,
	   re_base_dim.ean,
	   re_base_dim.product_code,
	   re_base_dim.distributor_name,
       re_base_dim.store_code,
	   re_base_dim.store_name,
       re_base_dim.store_type,
       --re_base_dim.ean_code,
       re_base_dim.mapped_sku_code,
	   re_base_dim.msl_product_desc,
       re_base_dim.data_src,
       re_base_dim.region,
       re_base_dim.zone,
       re_base_dim.city,
	   --re_base_dim.customer_product_desc as sku_code_desc,
       re_base_dim.sap_parent_customer_key,
       re_base_dim.sap_parent_customer_description,
       re_base_dim.sap_customer_channel_key,
       re_base_dim.sap_customer_channel_description,
       re_base_dim.sap_customer_sub_channel_key,
       re_base_dim.sap_sub_channel_description,
       re_base_dim.sap_go_to_mdl_key,
       re_base_dim.sap_go_to_mdl_description,
       re_base_dim.sap_banner_key,
       re_base_dim.sap_banner_description,
       re_base_dim.sap_banner_format_key,
       re_base_dim.sap_banner_format_description,
       re_base_dim.retail_environment,
       re_base_dim.customer_segment_key,
       re_base_dim.customer_segment_description,
       re_base_dim.global_product_franchise,
       re_base_dim.global_product_brand,
       re_base_dim.global_product_sub_brand,
       re_base_dim.global_product_variant,
       re_base_dim.global_product_segment,
       re_base_dim.global_product_subsegment,
       re_base_dim.global_product_category,
       re_base_dim.global_product_subcategory,
       re_base_dim.global_put_up_description,
       re_base_dim.pka_product_key,
       re_base_dim.pka_product_key_desc,
       cm.so_sls_qty as cm_sales_qty,
       cm.so_sls_value as cm_sales,
       cm.so_avg_qty as cm_avg_sales_qty,
       cm.sales_value_list_price as cm_sales_value_list_price,
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
         when lm_sales > 0 then 'Y'
         else 'N'
       end as lm_sales_flag,
       case
         when p3m_sales > 0 then 'Y'
         else 'N'
       end as p3m_sales_flag,
       case
         when p6m_sales > 0 then 'Y'
         else 'N'
       end as p6m_sales_flag,
       case
         when p12m_sales > 0 then 'Y'
         else 'N'
       end as p12m_sales_flag,
       sysdate() as crt_dttm
from wks_china_personal_care_regional_sellout_basedim base_dim 
  left join  wks_china_personal_care_rebase_dim_values re_base_dim
         on re_base_dim.cntry_cd = base_dim.cntry_cd
        and re_base_dim.sellout_dim_key = base_dim.sellout_dim_key
  left outer join (select distinct cntry_cd,
                          sellout_dim_key,
                          mnth_id,
                          so_sls_qty,
                          so_sls_value,
                          so_avg_qty,
                          sales_value_list_price
                   from wks_china_personal_care_base_retail_excellence where mnth_id >= (select last_17mnths from edw_vw_cal_retail_excellence_dim)
and mnth_id <= (select last_2mnths from edw_vw_cal_retail_excellence_dim)) cm
               on base_dim.cntry_cd = cm.cntry_cd
              and base_dim.month = cm.mnth_id
              and base_dim.sellout_dim_key = cm.sellout_dim_key
  left outer join
--last month
wks_china_personal_care_regional_sellout_act_lm lm
               on base_dim.cntry_cd = lm.cntry_cd
              and base_dim.month = lm.month
              and base_dim.sellout_dim_key = lm.sellout_dim_key
  left outer join
--l3m
wks_china_personal_care_regional_sellout_act_l3m l3m
               on base_dim.cntry_cd = l3m.cntry_cd
              and base_dim.month = l3m.month
              and base_dim.sellout_dim_key = l3m.sellout_dim_key
  left outer join
--l6m
wks_china_personal_care_regional_sellout_act_l6m l6m
               on base_dim.cntry_cd = l6m.cntry_cd
              and base_dim.month = l6m.month
              and base_dim.sellout_dim_key = l6m.sellout_dim_key
  left outer join
--l12m
wks_china_personal_care_regional_sellout_act_l12m l12m
               on base_dim.cntry_cd = l12m.cntry_cd
              and base_dim.month = l12m.month
              and base_dim.sellout_dim_key = l12m.sellout_dim_key
),

wks_china_personal_care_actuals_final
as 
(
   select 
cntry_cd :: varchar(2) as cntry_cd,
cntry_nm :: varchar(50) as cntry_nm,
"year" :: varchar(16) as "year",
mnth_id	:: varchar(23) as mnth_id,
soldto_code	:: varchar(255) as soldto_code,
distributor_code :: varchar(100) as distributor_code,
ean::varchar(150) as ean,
product_code::varchar(150) as product_code,
distributor_name::varchar(356) as distributor_name,
store_code::varchar(100) as store_code,
store_name::varchar(601) as store_name,
store_type::varchar(382) as store_type,
mapped_sku_code::varchar(40) as mapped_sku_code,
msl_product_desc::varchar(300) as msl_product_desc,
data_src::varchar(14) as data_src,
region::varchar(150) as region,
zone::varchar(150) as zone,
city::varchar(150) as city,
sap_parent_customer_key::varchar(12) as sap_parent_customer_key,
sap_parent_customer_description::varchar(75) as sap_parent_customer_description,
sap_customer_channel_key::varchar(12) as sap_customer_channel_key,
sap_customer_channel_description::varchar(75) as sap_customer_channel_description,
sap_customer_sub_channel_key::varchar(12) as sap_customer_sub_channel_key,
sap_sub_channel_description::varchar(75) as sap_sub_channel_description,
sap_go_to_mdl_key::varchar(12) as sap_go_to_mdl_key,
sap_go_to_mdl_description::varchar(75) as sap_go_to_mdl_description,
sap_banner_key::varchar(12) as sap_banner_key,
sap_banner_description::varchar(75) as sap_banner_description,
sap_banner_format_key::varchar(12) as sap_banner_format_key,
sap_banner_format_description::varchar(75) as sap_banner_format_description,
retail_environment::varchar(50) as retail_environment,
customer_segment_key::varchar(12) as customer_segment_key,
customer_segment_description::varchar(50) as customer_segment_description,
global_product_franchise::varchar(30) as global_product_franchise,
global_product_brand::varchar(30) as global_product_brand,
global_product_sub_brand::varchar(100) as global_product_sub_brand,
global_product_variant::varchar(100) as global_product_variant,
global_product_segment::varchar(50) as global_product_segment,
global_product_subsegment::varchar(100) as global_product_subsegment,
global_product_category::varchar(50) as global_product_category,
global_product_subcategory::varchar(50) as global_product_subcategory,
global_put_up_description::varchar(100) as global_put_up_description,
pka_product_key::varchar(68) as pka_product_key,
pka_product_key_desc::varchar(382) as pka_product_key_desc,
cm_sales_qty::numeric(38,6) as cm_sales_qty,
cm_sales::numeric(38,6) as cm_sales,
cm_avg_sales_qty::numeric(38,6) as cm_avg_sales_qty,
cm_sales_value_list_price::numeric(38,12) as cm_sales_value_list_price,
lm_sales::numeric(38,6) as lm_sales,
lm_sales_qty::numeric(38,6) as lm_sales_qty,
lm_avg_sales_qty::numeric(38,6) as lm_avg_sales_qty,
lm_sales_lp::numeric(38,12) as lm_sales_lp,
p3m_sales::numeric(38,6) as p3m_sales,
p3m_qty::numeric(38,6) as p3m_qty,
p3m_avg_qty::numeric(38,6) as p3m_avg_qty,
p3m_sales_lp::numeric(38,12) as p3m_sales_lp,
f3m_sales::numeric(38,6) as f3m_sales,
f3m_qty::numeric(38,6) as f3m_qty,
f3m_avg_qty::numeric(38,6) as f3m_avg_qty,
p6m_sales::numeric(38,6) as p6m_sales,
p6m_qty::numeric(38,6) as p6m_qty,
p6m_avg_qty::numeric(38,6) as p6m_avg_qty,
p6m_sales_lp::numeric(38,12) as p6m_sales_lp,
p12m_sales::numeric(38,6) as p12m_sales,
p12m_qty::numeric(38,6) as p12m_qty,
p12m_avg_qty::numeric(38,6) as p12m_avg_qty,
p12m_sales_lp::numeric(38,12) as p12m_sales_lp,
lm_sales_flag::varchar(1) as lm_sales_flag,
p3m_sales_flag::varchar(1) as p3m_sales_flag,
p6m_sales_flag::varchar(1) as p6m_sales_flag,
p12m_sales_flag::varchar(1) as p12m_sales_flag,
crt_dttm :: timestamp without time zone as crt_dttm
from wks_china_personal_care_actuals
)


select * from wks_china_personal_care_actuals_final