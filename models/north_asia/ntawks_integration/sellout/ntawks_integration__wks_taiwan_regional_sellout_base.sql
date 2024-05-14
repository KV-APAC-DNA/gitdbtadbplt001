with 
edw_pos_fact as (
select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.EDW_POS_FACT
),
EDW_VW_OS_TIME_DIM as (
select * from DEV_DNA_CORE.SNENAV01_WORKSPACE.EDW_VW_OS_TIME_DIM
),
EDW_IMS_FACT as (
select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.EDW_IMS_FACT
),
itg_ims_dstr_cust_attr as (
select * from DEV_DNA_CORE.SNAPNTAITG_INTEGRATION.ITG_IMS_DSTR_CUST_ATTR
),
itg_mds_ap_customer360_config as (
select * from DEV_DNA_CORE.SNAPASPITG_INTEGRATION.ITG_MDS_AP_CUSTOMER360_CONFIG
),
pos as (
SELECT 'POS' AS DATA_SRC,
       'TW' AS 	CNTRY_CD,
       'Taiwan' AS CNTRY_NM,
       time_dim."year"::INT AS YEAR,
       time_dim.mnth_id::INT AS MNTH_ID,
	   POS.pos_dt AS DAY,
	   time_dim.cal_year::INT  as univ_year,
	   time_dim.cal_mnth_no::INT as univ_month,
	   sold_to_party as SOLDTO_CODE,
       'NA' AS DISTRIBUTOR_CODE,
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
	   POS.sls_amt as SO_SLS_VALUE
      FROM  edw_pos_fact POS
	  left join edw_vw_os_time_dim as time_dim
	on POS.pos_dt = time_dim.cal_date 
	WHERE ctry_cd = 'TW' and crncy_cd = 'TWD' 
	and   (sls_qty != 0 or sls_amt != 0)
),
sellout as (
SELECT 'SELL-OUT' AS DATA_SRC,
       'TW' AS 	CNTRY_CD,
       'Taiwan' AS CNTRY_NM,
       time_dim."year"::INT AS YEAR,
       time_dim.mnth_id::INT AS MNTH_ID,
       ims_txn_dt AS DAY,
	   time_dim.cal_year::INT  as univ_year,
	   time_dim.cal_mnth_no::INT as univ_month,
       sls.dstr_cd as SOLDTO_CODE,
       sls.dstr_cd AS DISTRIBUTOR_CODE,
       sls.dstr_nm AS DISTRIBUTOR_NAME,
       sls.cust_cd AS STORE_CD,
       sls.cust_nm AS STORE_NAME,
	   dc.store_type as store_type,
       sls.ean_num AS EAN,
       sls.prod_cd AS MATL_NUM,
	   prod_nm AS Customer_Product_Desc,
	   'NA' AS region,
	   'NA' AS zone_or_area,
	   (sls_qty - rtrn_qty) as SO_SLS_QTY, 
	   (sls_amt - rtrn_amt) as SO_SLS_VALUE
from edw_ims_fact sls LEFT JOIN edw_vw_os_time_dim time_dim
on sls.ims_txn_dt = time_dim.cal_date
left join (SELECT DISTINCT dstr_cd, "replace"("replace"("replace"("replace"(dstr_cust_cd::text, 'Indirect'::text, ''::text),
  'Direct'::text, ''::text), 'Indi'::text, ''::text), 'Dire'::text, ''::text) AS dstr_cust_cd, 
store_type FROM itg_ims_dstr_cust_attr where ctry_cd = 'TW'
) dc on sls.dstr_cd = dc.dstr_cd and sls.cust_cd = dc.dstr_cust_cd
where ctry_cd = 'TW' and crncy_cd = 'TWD'  and (sls.prod_cd not like '1U%' OR sls.prod_cd not like 'COUNTER TOP%' OR sls.prod_cd not like 'DUMPBIN%' OR sls.prod_cd IS not NULL OR sls.prod_cd != '')  
),
final as (
SELECT
BASE.data_src,
BASE.cntry_cd,
BASE.cntry_nm,
BASE.year,
BASE.mnth_id,
BASE.day,
BASE.univ_year,
BASE.univ_month,
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
current_timestamp() AS crtd_dttm,
current_timestamp() AS updt_dttm
FROM
(
--POS
select * from pos
	
UNION ALL

select * from sellout
	  )BASE
WHERE NOT (nvl(base.so_sls_value, 0) = 0 and nvl(base.so_sls_qty, 0) = 0) AND base.day > (select to_date(param_value,'YYYY-MM-DD') from itg_mds_ap_customer360_config where code='min_date') 
AND base.mnth_id>= (case when (select param_value from itg_mds_ap_customer360_config where code='base_load_tw')='ALL' then '190001' else to_char(add_months(to_date(current_date::varchar, 'YYYY-MM-DD'), -((select param_value from itg_mds_ap_customer360_config where code='base_load_tw')::integer)), 'YYYYMM')
end)
)
select * from final
