with edw_vw_vn_sellout_sales_fact as (
select * from {{ ref('vnmedw_integration__edw_vw_vn_sellout_sales_fact') }}
),
itg_vn_distributor_sap_sold_to_mapping as (
select * from {{ source('vnmitg_integration','itg_vn_distributor_sap_sold_to_mapping') }}
),
itg_vn_dms_distributor_dim as (
select * from {{ ref('vnmitg_integration__itg_vn_dms_distributor_dim') }}
),
itg_vn_dms_customer_dim as (
select * from {{ ref('vnmitg_integration__itg_vn_dms_customer_dim') }}
),
edw_vw_os_time_dim as (
select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
itg_vn_dms_product_dim as (
select * from {{ ref('vnmitg_integration__itg_vn_dms_product_dim') }}
),
itg_vn_mt_sellin_dksh as (
select * from {{ ref('vnmitg_integration__itg_vn_mt_sellin_dksh') }}
),
itg_vn_mt_sellin_dksh_history as (
select * from {{ ref('vnmitg_integration__itg_vn_mt_sellin_dksh_history') }}
),
itg_query_parameters as (
select * from {{ source('aspitg_integration', 'itg_query_parameters') }}
),
edw_vw_vn_mt_dist_products as (
select * from {{ ref('vnmedw_integration__edw_vw_vn_mt_dist_products') }}
),
edw_vw_vn_mt_dist_customers as (
select * from {{ ref('vnmedw_integration__edw_vw_vn_mt_dist_customers') }}
),
itg_mds_ap_customer360_config as (
select * from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }}
),
transformed as (
select
base.data_src,
base.cntry_cd,
base.cntry_nm,
base.year,
base.mnth_id,
base.day,
base.univ_year,
base.univ_month,
base.soldto_code,
base.distributor_code,
base.distributor_name,
base.store_cd,
base.store_name,
base.store_type,
base.ean,
base.matl_num,
base.Customer_Product_Desc,
base.region,
base.zone_or_area,
base.so_sls_qty,
base.so_sls_value,
current_timestamp() as crtd_dttm,
current_timestamp() as updt_dttm
FROM
(
--SELL-OUT GT
select 'SELL-OUT' as data_src,
       'VN' as 	cntry_cd,
       'Vietnam' as cntry_nm,
       time_dim."year"::int as year,
       time_dim.mnth_id::int as mnth_id,
	   so_fact.bill_date as day,
	   time_dim.cal_year::int  as univ_year,
	   time_dim.cal_mnth_no::int as univ_month,
	   d.sap_sold_to_code as soldto_code,
       so_fact.dstrbtr_grp_cd as distributor_code,
       d.dstrbtr_name as distributor_name,
       so_fact.cust_cd as store_cd,
       'NA' as store_name,
	   cust.shop_type as store_type,
       'NA' as ean,
       so_fact.sap_matl_num as matl_num,
	   prod_dim.product_name as customer_product_desc,
	   d.region as region,
	   d.province as zone_or_area,
	   so_fact.sls_qty as so_sls_qty, 
	   so_fact.jj_net_sls as so_sls_value
      from edw_vw_vn_sellout_sales_fact so_fact
	join(select dstrb.dstrbtr_id, mapp.sap_sold_to_code, dstrb.territory_dist,dstrb.dstrbtr_type, dstrb.dstrbtr_name, dstrb.region, dstrb.province from itg_vn_distributor_sap_sold_to_mapping mapp, itg_vn_dms_distributor_dim dstrb 
	where mapp.distributor_id = nvl(dstrb.mapped_spk,dstrb.dstrbtr_id)
	) d on so_fact.dstrbtr_grp_cd = d.dstrbtr_id
	left join itg_vn_dms_customer_dim cust
	  on so_fact.dstrbtr_grp_cd = cust.dstrbtr_id and so_fact.cust_cd = cust.outlet_id
	left join edw_vw_os_time_dim time_dim on so_fact.bill_date::date = time_dim.cal_date
	left join itg_vn_dms_product_dim prod_dim on prod_dim.product_code=so_fact.dstrbtr_matl_num
	where so_fact.sls_qty <> 0 OR so_fact.ret_qty <> 0 OR so_fact.grs_trd_sls <> 0 OR so_fact.ret_val <> 0 OR so_fact.trd_discnt <> 0 and so_fact.cntry_cd = 'VN'
	
union all

-- SELL-OUT (MTI)
select 'SELL-OUT' as data_src,
       'VN' as 	cntry_cd,
       'Vietnam' as cntry_nm,
       time_dim."year"::int as year,
       time_dim.mnth_id::int as mnth_id,
       --invoice_date as day,
	   to_date(invoice_date,'YYYYMMDD')  as day,
	   time_dim.cal_year::int  as univ_year,
	   time_dim.cal_mnth_no::int as univ_month,
       iqp.parameter_value as soldto_code,
       'NA' as distributor_code,
       cust.account as distributor_name,
       so_fact.custcode as store_cd,
       so_fact.customer as store_name,
	   cust.retail_environment as store_type,
       prd.barcode as ean,
       prd.jnj_sap_code as matl_num,
	   so_fact.product as customer_product_desc,
	   cust.region as region,
	   cust.province as zone_or_area,
	   qty_exclude_foc as so_sls_qty, 
	   case when channel = 'ECOM' then gross_amount_wo_vat else net_amount_wo_vat end as so_sls_value
  from (
		select productid, product, custcode, customer, channel, sellin_sub_channel, province, region, cust_group, invoice_date, qty_exclude_foc, net_amount_wo_vat, gross_amount_wo_vat
		   from ( select dense_rank()
				  over( 
				  partition by productid, custcode, billing_no, invoice_date, order_no
				  order by filename desc) as rnk, 
				  productid, product, custcode, customer, channel, sellin_sub_channel, province, region, cust_group, invoice_date, qty_exclude_foc, net_amount_wo_vat, gross_amount_wo_vat
				  from itg_vn_mt_sellin_dksh) where rnk = 1
		union all 
		 select productid, product, custcode, customer, channel, sellin_sub_channel, province, region, cust_group, invoice_date, qty_exclude_foc, net_amount_wo_vat, gross_amount_wo_vat
		   from itg_vn_mt_sellin_dksh_history
	) so_fact
	left join itg_query_parameters iqp
	on iqp.parameter_name='vn_dksh_soldto_code' and iqp.country_code='VN'   
	left join edw_vw_os_time_dim time_dim on so_fact.invoice_date = time_dim.cal_date_id
	left join edw_vw_vn_mt_dist_products prd on so_fact.productid = prd.code
	left join edw_vw_vn_mt_dist_customers cust on so_fact.custcode = cust.code
	  )base
where NOT (nvl(base.so_sls_value, 0) = 0 and nvl(base.so_sls_qty, 0) = 0) AND base.day > (select to_date(param_value,'YYYY-MM-DD') from itg_mds_ap_customer360_config where code='min_date') 
AND base.mnth_id>= (case when (select param_value from itg_mds_ap_customer360_config where code='base_load_vn')='ALL' then '190001' else to_char(add_months(to_date(current_date::varchar, 'YYYY-MM-DD'), -((select param_value from itg_mds_ap_customer360_config where code='base_load_vn')::integer)), 'YYYYMM')
end)
),
final as (
select 
data_src::varchar(8) as data_src,
cntry_cd::varchar(2) as cntry_cd,
cntry_nm::varchar(7) as cntry_nm,
year::number(18,0) as year,
mnth_id::number(18,0) as mnth_id,
day::date as day,
univ_year::number(18,0) as univ_year,
univ_month::number(18,0) as univ_month,
soldto_code::varchar(300) as soldto_code,
distributor_code::varchar(30) as distributor_code,
distributor_name::varchar(750) as distributor_name,
store_cd::varchar(30) as store_cd,
store_name::varchar(100) as store_name,
store_type::varchar(300) as store_type,
ean::varchar(40) as ean,
matl_num::varchar(40) as matl_num,
customer_product_desc::varchar(255) as customer_product_desc,
region::varchar(750) as region,
zone_or_area::varchar(750) as zone_or_area,
so_sls_qty::number(15,4) as so_sls_qty,
so_sls_value::number(38,23) as so_sls_value,
crtd_dttm::timestamp_ntz(9) as crtd_dttm,
updt_dttm::timestamp_ntz(9) as updt_dttm
from transformed
)
select * from final