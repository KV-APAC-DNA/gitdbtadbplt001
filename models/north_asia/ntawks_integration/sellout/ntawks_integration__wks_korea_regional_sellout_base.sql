with edw_pos_fact as (
select * from {{ ref('ntaedw_integration__edw_pos_fact') }}
),
edw_vw_os_time_dim as (
select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_ims_fact as (
select * from {{ ref('ntaedw_integration__edw_ims_fact') }}
),
edw_customer_attr_flat_dim as (
select * from aspedw_integration.edw_customer_attr_flat_dim
),
wks_parameter_gt_sellout as (
select * from {{ ref('ntawks_integration__wks_parameter_gt_sellout') }}
),
v_kr_ecommerce_sellout as (
select * from {{ ref('ntaedw_integration__v_kr_ecommerce_sellout') }}
),
itg_mds_ap_customer360_config as (
select * from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }}
),
transformed as (
SELECT
BASE.data_src,
BASE.cntry_cd,
BASE.cntry_nm,
BASE.year,
BASE.mnth_id,
BASE.week_id,
BASE.day,
BASE.soldto_code,
BASE.distributor_code,
BASE.distributor_name,
BASE.store_cd,
BASE.store_name,
BASE.store_type,
BASE.ean,
BASE.matl_num,
BASE.Customer_Product_Desc,
BASE.region,
BASE.zone_or_area,
BASE.so_sls_qty,
BASE.so_sls_value,
BASE.msl_product_code,
BASE.msl_product_desc,
--BASE.store_grade,
UPPER(BASE.retail_env) as retail_env,
BASE.channel,
current_timestamp() AS crtd_dttm,
current_timestamp() AS updt_dttm
FROM
(
--POS
SELECT 'POS' AS DATA_SRC,
       'KR' AS 	CNTRY_CD,
       'Korea' AS CNTRY_NM,
       time_dim."year"::INT AS YEAR,
       time_dim.mnth_id::INT AS MNTH_ID,
	   (time_dim."year" || LPAD(time_dim.wk,2,'00'))::INT AS WEEK_ID,
	   POS.pos_dt AS DAY,
	   sold_to_party as SOLDTO_CODE,
       POS.sls_grp_cd AS DISTRIBUTOR_CODE,
       POS.sls_grp AS DISTRIBUTOR_NAME,
       POS.str_cd AS STORE_CD,
       POS.str_nm AS STORE_NAME,
	   POS.store_type,
       ean_num AS EAN,
       POS.matl_num AS MATL_NUM,
	   vend_prod_nm AS Customer_Product_Desc,
	   'NA' AS region,
	   'NA' AS zone_or_area,
	   POS.sls_qty as SO_SLS_QTY, 
	   POS.sls_amt as SO_SLS_VALUE,
	   ean_num as msl_product_code,
		vend_prod_nm as msl_product_desc,
		store_type as retail_env,
		channel
      FROM  edw_pos_fact POS
	  left join edw_vw_os_time_dim as time_dim
on POS.pos_dt = time_dim.cal_date 
WHERE ctry_cd = 'KR' and crncy_cd = 'KRW' 
and not (sls_qty = 0 and sls_amt = 0)

UNION ALL

--SELL-OUT
SELECT 'SELL-OUT' AS DATA_SRC,
       'KR' AS 	CNTRY_CD,
       'Korea' AS CNTRY_NM,
       time_dim."year"::INT AS YEAR,
       time_dim.mnth_id::INT AS MNTH_ID,
	   (time_dim."year" || LPAD(time_dim.wk,2,'00'))::INT AS WEEK_ID,
       ims_txn_dt AS DAY,
       sls.cust_cd as SOLDTO_CODE,
       sls.dstr_cd AS DISTRIBUTOR_CODE,
       sls.dstr_nm AS DISTRIBUTOR_NAME,
       sls.sub_customer_code AS STORE_CD,
       sls.sub_customer_name AS STORE_NAME,
	   cust.store_typ as store_type,
       sls.ean_num AS EAN,
       sls.prod_cd AS MATL_NUM,
	   prod_nm AS Customer_Product_Desc,
	   'NA' AS region,
	   'NA' AS zone_or_area,
	   sls.sls_qty as SO_SLS_QTY, 
	   sls.sls_amt as SO_SLS_VALUE,
	   ean_num as msl_product_code,
		prod_nm as msl_product_desc,
		store_typ as retail_env,
		channel
from edw_ims_fact sls LEFT JOIN edw_vw_os_time_dim time_dim
on sls.ims_txn_dt = time_dim.cal_date
left join (SELECT DISTINCT aw_remote_key, store_typ, channel FROM edw_customer_attr_flat_dim
WHERE cntry = 'Korea') cust ON sls.cust_cd::text = cust.aw_remote_key::text
where ctry_cd = 'KR' and crncy_cd = 'KRW' and
upper(dstr_nm) IN (SELECT DISTINCT upper(dstr_nm) FROM wks_parameter_gt_sellout
WHERE upper(country_cd) = 'KR' AND upper(parameter_name) = 'GT_SELLOUT') and not (sls_qty = 0 and sls_amt = 0)

UNION ALL

--ECOM
SELECT 'ECOM' AS DATA_SRC,
       'KR' AS 	CNTRY_CD,
       'Korea' AS CNTRY_NM,
       fisc_year::INT AS YEAR,
       trunc(LEFT(fisc_year,4)||RIGHT(LPAD(fisc_month,2,'00'),2))::INT AS MNTH_ID,
	   (LEFT(fisc_year,4)||LPAD(date_part(w,cal_day),2,'00'))::INT AS WEEK_ID,
	   cal_day AS DAY,
	   distributor_code as SOLDTO_CODE,
       distributor_code AS DISTRIBUTOR_CODE,
       distributor_name AS DISTRIBUTOR_NAME,
       NULL AS STORE_CD,
       subcustomer_name AS STORE_NAME,
	   store_type,
       ean AS EAN,
       jnj_sku_code AS MATL_NUM,
	   product_name AS Customer_Product_Desc,
	   'NA' AS region,
	   'NA' AS zone_or_area,
	   sales_qty as SO_SLS_QTY, 
	   sales_value_lcy as SO_SLS_VALUE,
	   'NA' as msl_product_code,
		'NA' as msl_product_desc,
		'NA' as retail_env,
		'NA' as channel
      from v_kr_ecommerce_sellout where from_crncy = 'KRW' and to_crncy = 'KRW' and not (sales_qty = 0 and sales_value_lcy = 0)) BASE
WHERE NOT (nvl(BASE.so_sls_value, 0) = 0 and nvl(BASE.so_sls_qty, 0) = 0) AND BASE.day > (select to_date(param_value,'YYYY-MM-DD') from itg_mds_ap_customer360_config where code='min_date') 
AND BASE.mnth_id>= (case when (select param_value from itg_mds_ap_customer360_config where code='base_load_kr')='ALL' THEN '190001' ELSE to_char(add_months(to_date(current_date::varchar, 'YYYY-MM-DD'), -((select param_value from itg_mds_ap_customer360_config where code='base_load_kr')::integer)), 'YYYYMM')
END)
),
final as (
select 
data_src::varchar(8) as data_src,
cntry_cd::varchar(2) as cntry_cd,
cntry_nm::varchar(5) as cntry_nm,
year::number(18,0) as year,
mnth_id::number(18,0) as mnth_id,
week_id::number(18,0) as week_id,
day::date as day,
soldto_code::varchar(2000) as soldto_code,
distributor_code::varchar(2000) as distributor_code,
distributor_name::varchar(2000) as distributor_name,
store_cd::varchar(50) as store_cd,
store_name::varchar(255) as store_name,
store_type::varchar(100) as store_type,
ean::varchar(100) as ean,
matl_num::varchar(255) as matl_num,
customer_product_desc::varchar(500) as customer_product_desc,
region::varchar(2) as region,
zone_or_area::varchar(2) as zone_or_area,
so_sls_qty::number(38,0) as so_sls_qty,
so_sls_value::float as so_sls_value,
msl_product_code::varchar(100) as msl_product_code,
msl_product_desc::varchar(255) as msl_product_desc,
retail_env::varchar(150) as retail_env,
channel::varchar(100) as channel,
crtd_dttm::timestamp_ntz(9) as crtd_dttm,
updt_dttm::timestamp_ntz(9) as updt_dttm
from transformed
)
select * from final