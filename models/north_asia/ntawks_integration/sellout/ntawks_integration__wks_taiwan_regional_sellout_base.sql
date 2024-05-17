with 
edw_pos_fact as (
select * from dev_dna_core.snapntaedw_integration.edw_pos_fact
),
edw_vw_os_time_dim as (
select * from dev_dna_core.snenav01_workspace.edw_vw_os_time_dim
),
edw_ims_fact as (
select * from dev_dna_core.snapntaedw_integration.edw_ims_fact
),
itg_ims_dstr_cust_attr as (
select * from dev_dna_core.snapntaitg_integration.itg_ims_dstr_cust_attr
),
itg_mds_ap_customer360_config as (
select * from dev_dna_core.snapaspitg_integration.itg_mds_ap_customer360_config
),
edw_vw_pop6_store as (
select * from dev_dna_core.snapntaedw_integration.edw_vw_pop6_store
),
itg_query_parameters as (
select * from dev_dna_core.snapntaitg_integration.itg_query_parameters
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
	   POS.sls_amt as SO_SLS_VALUE,
	   ean_num as msl_product_code,
		vend_prod_nm as msl_product_desc,
		--'NA'as store_grade,
		pop6.retail_environment_ps as retail_env,
		pop6.channel as channel
      FROM  edw_pos_fact POS
	  left join edw_vw_os_time_dim as time_dim
	on POS.pos_dt = time_dim.cal_date 
	left join (select * from
		(select *,  row_number() OVER(PARTITION BY pop_code, customer order by retail_environment_ps, channel NULLS LAST) rnk  from 
		(select distinct case when customer in 
		--('Carrefour 家樂福','RT-Mart 大潤發','PX 全聯', 'Cosmed 康是美', 'Poya 寶雅')
		(select parameter_value from itg_query_parameters where country_code='TW' and parameter_name='customer' and parameter_type='split')
		then ltrim(reverse(split_part(reverse(pop_code),'-',1)),'0')
		  when customer in 
		  --('Watsons 屈臣氏') 
		  (select parameter_value from itg_query_parameters where country_code='TW' and parameter_name='customer' and parameter_type='suffix')
		  then ltrim(right(pop_code,3),'0') 
		  else ltrim(pop_code,'0') end as pop_code, customer, retail_environment_ps, channel from edw_vw_pop6_store )
		where NULLIF(pop_code,'') is not null) 
		where rnk=1) pop6
	on ltrim(POS.str_cd,'0') = pop6.pop_code and POS.sls_grp = pop6.customer
	WHERE ctry_cd = 'TW' and crncy_cd = 'TWD' 
	and not (sls_qty = 0 and sls_amt = 0)
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
	   (sls_amt - rtrn_amt) as SO_SLS_VALUE,
	   sls.ean_num as msl_product_code,
		prod_nm as msl_product_desc,
		--'NA'as store_grade,
		pop6.retail_environment_ps as retail_env,
		pop6.channel as channel
from edw_ims_fact sls LEFT JOIN edw_vw_os_time_dim time_dim
on sls.ims_txn_dt = time_dim.cal_date
left join (SELECT DISTINCT dstr_cd, "replace"("replace"("replace"("replace"(dstr_cust_cd::text, 'Indirect'::text, ''::text),
  'Direct'::text, ''::text), 'Indi'::text, ''::text), 'Dire'::text, ''::text) AS dstr_cust_cd, 
store_type FROM itg_ims_dstr_cust_attr where ctry_cd = 'TW'
) dc on sls.dstr_cd = dc.dstr_cd and sls.cust_cd = dc.dstr_cust_cd
left join (select * from
	(select *,  row_number() OVER(PARTITION BY pop_code, customer order by retail_environment_ps, channel NULLS LAST) rnk  from 
	(select distinct case when customer in ('Carrefour 家樂福','RT-Mart 大潤發','PX 全聯', 'Cosmed 康是美', 'Poya 寶雅') then ltrim(reverse(split_part(reverse(pop_code),'-',1)),'0')
	  when customer in ('Watsons 屈臣氏') then ltrim(right(pop_code,3),'0') 
	  else ltrim(pop_code,'0') end as pop_code, customer, retail_environment_ps, channel from edw_vw_pop6_store )
	where NULLIF(pop_code,'') is not null) 
	where rnk=1) pop6
	on ltrim(sls.cust_cd,'0') = pop6.pop_code and sls.dstr_nm = pop6.customer
where ctry_cd = 'TW' and crncy_cd = 'TWD'  and not (sls.prod_cd like '1U%' OR sls.prod_cd like 'COUNTER TOP%' OR sls.prod_cd like 'DUMPBIN%' OR sls.prod_cd IS NULL OR sls.prod_cd = '') 
),
transformed as (
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
select * from pos
	
UNION ALL

select * from sellout
	  )BASE
WHERE NOT (nvl(BASE.so_sls_value, 0) = 0 and nvl(BASE.so_sls_qty, 0) = 0) AND BASE.day > (select to_date(param_value,'YYYY-MM-DD') from itg_mds_ap_customer360_config where code='min_date') 
AND base.mnth_id>= (case when (select param_value from itg_mds_ap_customer360_config where code='base_load_tw')='ALL' then '190001' else to_char(add_months(to_date(current_date::varchar, 'YYYY-MM-DD'), -((select param_value from itg_mds_ap_customer360_config where code='base_load_tw')::integer)), 'YYYYMM')
end)
),
final as (
select
    data_src::varchar(8) as data_src,
    cntry_cd::varchar(2) as cntry_cd,
    cntry_nm::varchar(6) as cntry_nm,
    year::number(18,0) as year,
    mnth_id::number(18,0) as mnth_id,
    day::date as day,
    univ_year::number(18,0) as univ_year,
    univ_month::number(18,0) as univ_month,
    soldto_code::varchar(100) as soldto_code,
    distributor_code::varchar(10) as distributor_code,
    distributor_name::varchar(100) as distributor_name,
    store_cd::varchar(50) as store_cd,
    store_name::varchar(100) as store_name,
    store_type::varchar(255) as store_type,
    ean::varchar(100) as ean,
    matl_num::varchar(255) as matl_num,
    customer_product_desc::varchar(255) as customer_product_desc,
    region::varchar(2) as region,
    zone_or_area::varchar(2) as zone_or_area,
    so_sls_qty::number(18,0) as so_sls_qty,
    so_sls_value::number(22,5) as so_sls_value,
    msl_product_code::varchar(100) as msl_product_code,
    msl_product_desc::varchar(255) as msl_product_desc,
    retail_env::varchar(300) as retail_env,
    channel::varchar(200) as channel,
    crtd_dttm::timestamp_ntz(9) as crtd_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
from transformed   
)
select * from final
