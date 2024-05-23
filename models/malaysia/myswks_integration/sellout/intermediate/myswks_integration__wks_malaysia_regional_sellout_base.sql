with
itg_my_sellout_sales_fact as (
select * from {{ ref('mysitg_integration__itg_my_sellout_sales_fact') }}
),
itg_my_customer_dim as (
select * from {{ ref('mysitg_integration__itg_my_customer_dim') }}
),
itg_my_dstrbtrr_dim as (
select * from {{ ref('mysitg_integration__itg_my_dstrbtrr_dim') }}
),
itg_my_dstrbtr_cust_dim as (
select * from {{ ref('mysitg_integration__itg_my_dstrbtr_cust_dim') }}
),
itg_my_outlet_attr as (
select * from {{ ref('mysitg_integration__itg_my_outlet_attr') }}
),
edw_vw_os_time_dim as (
select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
itg_my_gt_outlet_exclusion as (
select * from {{ source('mysitg_integration', 'itg_my_gt_outlet_exclusion') }}
),
itg_my_pos_sales_fact as (
select * from {{ ref('mysitg_integration__itg_my_pos_sales_fact') }}
),
itg_my_customer_dim as (
select * from {{ ref('mysitg_integration__itg_my_customer_dim') }}
),
itg_my_pos_cust_mstr as (
select * from {{ ref('mysitg_integration__itg_my_pos_cust_mstr') }}
),
itg_mds_my_customer_hierarchy as (
select * from {{ ref('mysitg_integration__itg_mds_my_customer_hierarchy') }}
),
itg_mds_ap_customer360_config as (
select * from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }}
),
sellout as (
SELECT 'SELL-OUT' AS DATA_SRC,
       'MY' AS 	CNTRY_CD,
       'Malaysia' AS CNTRY_NM,
	   b."year" AS YEAR,
       b.mnth_id AS MNTH_ID,
	   NULL AS WEEK_ID,
       sls.sls_ord_dt AS DAY,
	   b.cal_year as univ_year,
	   Right(b.cal_mnth_id,2)::INT as univ_month,
       sls.dstrbtr_id as SOLDTO_CODE,
       sls.dstrbtr_id AS DISTRIBUTOR_CODE,
       imcd.dstrbtr_grp_nm AS DISTRIBUTOR_NAME,
       imoa.outlet_id AS STORE_CD,
       imoa.outlet_desc AS STORE_NAME,
	   imoa.outlet_type1 as store_type,
       dist.lvl1 AS DSTRBTR_LVL1,
       dist.lvl2 AS DSTRBTR_LVL2,
       dist.lvl3 AS DSTRBTR_LVL3,
       sls.ean_num AS EAN,
       sls.sap_matl_num AS MATL_NUM,
	   sls.dstrbtr_prod_desc AS Customer_Product_Desc,
		'NA' AS region,
		'NA' AS zone_or_area,
	   sls.qty_pc as SO_SLS_QTY, 
	   sls.total_amt_bfr_tax as SO_SLS_VALUE,
	   sls.ean_num as msl_product_code,
		sls.dstrbtr_prod_desc as msl_product_desc,
		attr.outlet_type2 as store_grade,
		'GT' as retail_env
		--'NA' as channel
from itg_my_sellout_sales_fact sls LEFT JOIN itg_my_customer_dim imcd
on ltrim(imcd.cust_id, '0') = ltrim(sls.dstrbtr_id, '0')
left join --itg_my_dstrbtr_cust_dim imoa
	(select * from (select distinct ltrim(outlet_id, '0') as outlet_id, ltrim(cust_id, '0') as cust_id, outlet_desc, outlet_type1, row_number() over(partition by ltrim(outlet_id, '0'),ltrim(cust_id, '0') order by crtd_dttm DESC) as rnk from itg_my_dstrbtr_cust_dim) where rnk=1) imoa
  on ltrim(sls.cust_cd, '0') = ltrim(imoa.outlet_id, '0')
  and ltrim(sls.dstrbtr_id, '0') = ltrim(imoa.cust_id, '0')
left join itg_my_dstrbtrr_dim dist on ltrim(imcd.dstrbtr_grp_cd, '0') = ltrim(dist.cust_id, '0')
left join edw_vw_os_time_dim b on sls.sls_ord_dt = b.cal_date
left join --itg_my_outlet_attr attr
	(select distinct ltrim(cust_id, '0') as cust_id , ltrim(outlet_id, '0') as outlet_id, outlet_type2 from itg_my_outlet_attr) attr
	on ltrim(sls.dstrbtr_id, '0')=ltrim(attr.cust_id, '0') and ltrim(imoa.outlet_id, '0') = ltrim(attr.outlet_id, '0')
	--ltrim(sls.cust_cd, '0') = ltrim(attr.outlet_id, '0')
where NOT (COALESCE(ltrim(imcd.dstrbtr_grp_cd, '0'), '0') || COALESCE(trim(sls.cust_cd), '0') 
  IN (SELECT DISTINCT COALESCE(dstrbtr_cd, '0')|| COALESCE(outlet_cd, '0')
               FROM itg_my_gt_outlet_exclusion))
),
pos as (
SELECT 'POS' AS DATA_SRC,
       'MY' AS 	CNTRY_CD,
       'Malaysia' AS CNTRY_NM,
       LEFT(JJ_MNTH_ID,4)::INT AS YEAR,
       JJ_MNTH_ID AS MNTH_ID,
	   JJ_YR_WEEK_NO AS WEEK_ID,
	   --CASE WHEN NVL(JJ_YR_WEEK_NO,'NA')<>'NA' THEN trunc(to_date(RIGHT(JJ_YR_WEEK_NO,2)::INTEGER||' '||LEFT(JJ_YR_WEEK_NO,4)::INTEGER,'WW YYYY'))-(date_part(dow,trunc(to_date(RIGHT(JJ_YR_WEEK_NO,2)::INTEGER||' '||LEFT(JJ_YR_WEEK_NO,4)::INTEGER,'WW YYYY'))))::integer+1 else TO_DATE(JJ_MNTH_ID|| '01','YYYYMMDD') end AS DAY, 
   CASE 
        WHEN NVL(JJ_YR_WEEK_NO, 'NA') <> 'NA' THEN 
            TO_DATE(SUBSTRING(JJ_YR_WEEK_NO, 1, 4) || '-01-01', 'YYYY-MM-DD') + 
            (CAST(SUBSTRING(JJ_YR_WEEK_NO, 5, 2) AS INTEGER) - 1) * 7  
        ELSE TO_DATE(JJ_MNTH_ID || '01', 'YYYYMMDD') 
    END AS DAY,
	   LEFT(JJ_MNTH_ID,4)::INT  as univ_year,
	   Right(JJ_MNTH_ID,2)::INT as univ_month,
	   imcd.dstrbtr_grp_cd as SOLDTO_CODE,
       POS.cust_id AS DISTRIBUTOR_CODE,
       POS.cust_nm AS DISTRIBUTOR_NAME,
       POS.store_cd AS STORE_CD,
       POS.store_nm AS STORE_NAME,
	   coalesce(b.store_type, b.store_frmt, b.dept_nm) as store_type,
       dist.lvl1 AS DSTRBTR_LVL1,
       dist.lvl2 AS DSTRBTR_LVL2,
       dist.lvl3 AS DSTRBTR_LVL3,
       NULL AS EAN,
       POS.sap_matl_num AS MATL_NUM,
	   POS.mt_item_desc AS Customer_Product_Desc,
		'NA' AS region,
		'NA' AS zone_or_area,
	   POS.qty as SO_SLS_QTY, 
	   POS.SO_VAL as SO_SLS_VALUE,
	   POS.sap_matl_num as msl_product_code,
		POS.mt_item_desc as msl_product_desc,
		--cust_hier.channel as store_grade,
		cust_hier.reg_group as store_grade,
		'MT' as retail_env
      FROM  itg_my_pos_sales_fact POS
	  left join (select cust_id, store_cd,max(store_type) as store_type,max(store_frmt)as store_frmt,max(dept_nm) as dept_nm
	  From itg_my_pos_cust_mstr group by cust_id, store_cd) b on POS.cust_id = b.cust_id and POS.store_cd = b.store_cd
	  LEFT JOIN itg_my_customer_dim imcd
on ltrim(imcd.cust_id, '0') = ltrim(POS.cust_id, '0')
left join itg_my_dstrbtrr_dim dist on ltrim(imcd.dstrbtr_grp_cd, '0') = ltrim(dist.cust_id, '0')
left join itg_mds_my_customer_hierarchy cust_hier
  ON ltrim(imcd.dstrbtr_grp_cd, '0') = ltrim(cust_hier.sold_to, '0')
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
BASE.univ_year,
BASE.univ_month,
BASE.soldto_code,
BASE.distributor_code,
BASE.distributor_name,
BASE.store_cd,
BASE.store_name,
BASE.store_type,
BASE.dstrbtr_lvl1,
BASE.dstrbtr_lvl2 ,
BASE.dstrbtr_lvl3,
BASE.ean,
BASE.matl_num,
BASE.Customer_Product_Desc,
BASE.region,
BASE.zone_or_area,
BASE.so_sls_qty,
BASE.so_sls_value,
BASE.msl_product_code,
BASE.msl_product_desc,
BASE.store_grade,
UPPER(BASE.retail_env) as retail_env,
current_timestamp() AS crtd_dttm,
current_timestamp() AS updt_dttm
FROM
(
select * from sellout

UNION ALL

select * from pos
  ) BASE
WHERE NOT (nvl(BASE.so_sls_value, 0) = 0 and nvl(BASE.so_sls_qty, 0) = 0) AND BASE.day > (select to_date(param_value,'YYYY-MM-DD') from itg_mds_ap_customer360_config where code='min_date') 
AND base.mnth_id>= (case when (select param_value from itg_mds_ap_customer360_config where code='base_load_my')='ALL' then '190001' else to_char(add_months(to_date(current_date::varchar, 'YYYY-MM-DD'), -((select param_value from itg_mds_ap_customer360_config where code='base_load_my')::integer)), 'YYYYMM')
end)
),
final as (
select
data_src::varchar(8) as data_src,
cntry_cd::varchar(2) as cntry_cd,
cntry_nm::varchar(8) as cntry_nm,
year::number(18,0) as year,
mnth_id::varchar(23) as mnth_id,
week_id::varchar(10) as week_id,
day::date as day,
univ_year::number(18,0) as univ_year,
univ_month::number(18,0) as univ_month,
soldto_code::varchar(50) as soldto_code,
distributor_code::varchar(50) as distributor_code,
distributor_name::varchar(255) as distributor_name,
store_cd::varchar(50) as store_cd,
store_name::varchar(255) as store_name,
store_type::varchar(255) as store_type,
dstrbtr_lvl1::varchar(40) as dstrbtr_lvl1,
dstrbtr_lvl2::varchar(40) as dstrbtr_lvl2,
dstrbtr_lvl3::varchar(40) as dstrbtr_lvl3,
ean::varchar(50) as ean,
matl_num::varchar(255) as matl_num,
customer_product_desc::varchar(255) as customer_product_desc,
region::varchar(2) as region,
zone_or_area::varchar(2) as zone_or_area,
so_sls_qty::number(22,6) as so_sls_qty,
so_sls_value::number(22,6) as so_sls_value,
msl_product_code::varchar(50) as msl_product_code,
msl_product_desc::varchar(255) as msl_product_desc,
store_grade::varchar(256) as store_grade,
retail_env::varchar(3) as retail_env,
crtd_dttm::timestamp_ntz(9) as crtd_dttm,
updt_dttm::timestamp_ntz(9) as updt_dttm
from transformed
)
select * from final



