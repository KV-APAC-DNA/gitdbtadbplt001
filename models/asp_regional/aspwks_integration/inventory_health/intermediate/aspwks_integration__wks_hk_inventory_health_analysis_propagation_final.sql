
with wks_hk_inventory_health_analysis_propagation as (
select * from {{ ref('aspwks_integration__wks_hk_inventory_health_analysis_propagation') }}
),
wks_hk_sellout_for_inv_analysis as (
select * from {{ ref('ntawks_integration__wks_hk_sellout_for_inv_analysis') }}
),
wks_hk_inventory_healthy_unhealthy_analysis as (
select * from {{ ref('ntawks_integration__wks_hk_inventory_healthy_unhealthy_analysis') }}
),
transformed as (
SELECT inv.*,healthy_inv.healthy_inventory
      ,wkly_avg.min_date
      ,datediff ( week,wkly_avg.min_date, last_day(to_date(left(cal_mnth_id,4)||right(cal_mnth_id,2),'yyyymm')) )diff_weeks
      ,case when least(diff_weeks,52) <= 0 then 1 else least(diff_weeks,52) end as l12m_weeks
      ,case when least(diff_weeks,26) <= 0 then 1 else least(diff_weeks,26) end as l6m_weeks 
      ,case when least(diff_weeks,13) <= 0 then 1 else least(diff_weeks,13) end as l3m_weeks
      ,inv.last_12months_so_val /l12m_weeks as l12m_weeks_avg_sales  
      ,inv.last_6months_so_val /l6m_weeks as l6m_weeks_avg_sales
      ,inv.last_3months_so_val /l3m_weeks as l3m_weeks_avg_sales
      ,last_12months_so_val_usd /l12m_weeks as l12m_weeks_avg_sales_usd  
      ,last_6months_so_val_usd /l6m_weeks as l6m_weeks_avg_sales_usd
      ,last_3months_so_val_usd /l3m_weeks as l3m_weeks_avg_sales_usd
      ,last_12months_so_qty /l12m_weeks as l12m_weeks_avg_sales_qty  
      ,last_6months_so_qty /l6m_weeks as l6m_weeks_avg_sales_qty
      ,last_3months_so_qty /l3m_weeks as l3m_weeks_avg_sales_qty
               
  FROM wks_hk_inventory_health_analysis_propagation  inv 
  LEFT JOIN
          (select  * from wks_hk_sellout_for_inv_analysis            
          )  wkly_avg       

       ON   inv.sap_prnt_cust_key            = wkly_avg.sap_prnt_cust_key
			and inv.dstrbtr_grp_cd               = wkly_avg.dstrbtr_grp_cd
			and inv.global_put_up_desc           	= wkly_avg.pka_size_desc			
            and inv.global_prod_brand                        = wkly_avg.brand            
            and inv.global_prod_variant                      = wkly_avg.variant          
            and inv.global_prod_segment                      = wkly_avg.segment          
            and inv.global_prod_category                = wkly_avg.prod_category    
			and inv.pka_product_key              = wkly_avg.pka_product_key 
  LEFT JOIN (select * from wks_hk_inventory_healthy_unhealthy_analysis) healthy_inv
		ON 	cal_mnth_id 							= healthy_inv.month 
			and inv.dstrbtr_grp_cd               = healthy_inv.dstrbtr_grp_cd
			and inv.sap_prnt_cust_key            = healthy_inv.sap_prnt_cust_key
			and inv.global_put_up_desc           	= healthy_inv.pka_size_desc			
            and inv.global_prod_brand                        = healthy_inv.brand            
            and inv.global_prod_variant                      = healthy_inv.variant          
            and inv.global_prod_segment                      = healthy_inv.segment          
            and inv.global_prod_category                = healthy_inv.prod_category    
			and inv.pka_product_key              = healthy_inv.pka_product_key
  ),
final as (
select
cal_year::number(18,0) as cal_year,
cal_qrtr_no::varchar(11) as cal_qrtr_no,
cal_mnth_id::varchar(23) as cal_mnth_id,
cal_mnth_no::number(18,0) as cal_mnth_no,
cntry_nm::varchar(9) as cntry_nm,
dstrbtr_grp_cd::varchar(50) as dstrbtr_grp_cd,
dstrbtr_grp_cd_name::varchar(2) as dstrbtr_grp_cd_name,
global_prod_franchise::varchar(30) as global_prod_franchise,
global_prod_brand::varchar(30) as global_prod_brand,
global_prod_sub_brand::varchar(100) as global_prod_sub_brand,
global_prod_variant::varchar(100) as global_prod_variant,
global_prod_segment::varchar(50) as global_prod_segment,
global_prod_subsegment::varchar(100) as global_prod_subsegment,
global_prod_category::varchar(50) as global_prod_category,
global_prod_subcategory::varchar(50) as global_prod_subcategory,
global_put_up_desc::varchar(30) as global_put_up_desc,
sku_cd::varchar(40) as sku_cd,
sku_description::varchar(100) as sku_description,
pka_product_key::varchar(68) as pka_product_key,
pka_product_key_description::varchar(255) as pka_product_key_description,
product_key::varchar(68) as product_key,
product_key_description::varchar(255) as product_key_description,
from_ccy::varchar(3) as from_ccy,
to_ccy::varchar(3) as to_ccy,
exch_rate::number(15,5) as exch_rate,
sap_prnt_cust_key::varchar(12) as sap_prnt_cust_key,
sap_prnt_cust_desc::varchar(50) as sap_prnt_cust_desc,
sap_cust_chnl_key::varchar(12) as sap_cust_chnl_key,
sap_cust_chnl_desc::varchar(50) as sap_cust_chnl_desc,
sap_cust_sub_chnl_key::varchar(12) as sap_cust_sub_chnl_key,
sap_sub_chnl_desc::varchar(50) as sap_sub_chnl_desc,
sap_go_to_mdl_key::varchar(12) as sap_go_to_mdl_key,
sap_go_to_mdl_desc::varchar(50) as sap_go_to_mdl_desc,
sap_bnr_key::varchar(12) as sap_bnr_key,
sap_bnr_desc::varchar(50) as sap_bnr_desc,
sap_bnr_frmt_key::varchar(12) as sap_bnr_frmt_key,
sap_bnr_frmt_desc::varchar(50) as sap_bnr_frmt_desc,
retail_env::varchar(50) as retail_env,
region::varchar(8) as region,
zone_or_area::varchar(8) as zone_or_area,
si_sls_qty::number(38,5) as si_sls_qty,
si_gts_val::number(38,5) as si_gts_val,
si_gts_val_usd::number(38,5) as si_gts_val_usd,
inventory_quantity::number(38,5) as inventory_quantity,
inventory_val::number(38,5) as inventory_val,
inventory_val_usd::number(38,5) as inventory_val_usd,
so_sls_qty::number(38,5) as so_sls_qty,
so_trd_sls::number(38,5) as so_trd_sls,
so_trd_sls_usd::number(30,0) as so_trd_sls_usd,
si_all_db_val::number(38,5) as si_all_db_val,
si_all_db_val_usd::number(38,5) as si_all_db_val_usd,
si_inv_db_val::number(38,5) as si_inv_db_val,
si_inv_db_val_usd::number(38,5) as si_inv_db_val_usd,
last_3months_so_qty::number(38,0) as last_3months_so_qty,
last_6months_so_qty::number(38,0) as last_6months_so_qty,
last_12months_so_qty::number(38,0) as last_12months_so_qty,
last_3months_so_val::number(38,4) as last_3months_so_val,
last_3months_so_val_usd::number(38,5) as last_3months_so_val_usd,
last_6months_so_val::number(38,4) as last_6months_so_val,
last_6months_so_val_usd::number(38,5) as last_6months_so_val_usd,
last_12months_so_val::number(38,4) as last_12months_so_val,
last_12months_so_val_usd::number(38,5) as last_12months_so_val_usd,
propagate_flag::varchar(1) as propagate_flag,
propagate_from::number(18,0) as propagate_from,
reason::varchar(100) as reason,
last_36months_so_val::number(38,4) as last_36months_so_val,
healthy_inventory::varchar(1) as healthy_inventory,
min_date::date as min_date,
diff_weeks::number(38,0) as diff_weeks,
l12m_weeks::number(38,0) as l12m_weeks,
l6m_weeks::number(38,0) as l6m_weeks,
l3m_weeks::number(38,0) as l3m_weeks,
l12m_weeks_avg_sales::number(38,4) as l12m_weeks_avg_sales,
l6m_weeks_avg_sales::number(38,4) as l6m_weeks_avg_sales,
l3m_weeks_avg_sales::number(38,4) as l3m_weeks_avg_sales,
l12m_weeks_avg_sales_usd::number(38,5) as l12m_weeks_avg_sales_usd,
l6m_weeks_avg_sales_usd::number(38,5) as l6m_weeks_avg_sales_usd,
l3m_weeks_avg_sales_usd::number(38,5) as l3m_weeks_avg_sales_usd,
l12m_weeks_avg_sales_qty::number(38,0) as l12m_weeks_avg_sales_qty,
l6m_weeks_avg_sales_qty::number(38,0) as l6m_weeks_avg_sales_qty,
l3m_weeks_avg_sales_qty::number(38,0) as l3m_weeks_avg_sales_qty
from transformed
)
select * from final
