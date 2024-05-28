with edw_indonesia_noo_analysis as(
select * from DEV_DNA_CORE.IDNEDW_INTEGRATION.EDW_INDONESIA_NOO_ANALYSIS
),
edw_vw_os_time_dim as(
select * from DEV_DNA_CORE.SNENAV01_WORKSPACE.EDW_VW_OS_TIME_DIM
),
edw_id_pos_sellout as(
select * from DEV_DNA_CORE.IDNEDW_INTEGRATION.EDW_ID_POS_SELLOUT
),
itg_query_parameters as(
select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_QUERY_PARAMETERS
),
itg_mds_ap_customer360_config as(
select * from DEV_DNA_CORE.ASPITG_INTEGRATION.ITG_MDS_AP_CUSTOMER360_CONFIG
),
union1 as(
	SELECT 'SELL-OUT' AS DATA_SRC,
       'ID' AS 	CNTRY_CD,
       'Indonesia' AS CNTRY_NM,
	   b."year" AS YEAR,
       b.mnth_id AS MNTH_ID,
	   (b."year" || LPAD(b.wk,2,'00'))::INT AS WEEK_ID,   
       to_date(sls.bill_dt) AS DAY,
	   b.cal_year as univ_year,
	   Right(b.cal_mnth_id,2)::INT as univ_month,
       sls.jj_sap_dstrbtr_id as SOLDTO_CODE,
       sls.dstrbtr_grp_cd AS DISTRIBUTOR_CODE,
       sls.dstrbtr_grp_nm AS DISTRIBUTOR_NAME,
       NVL(sls.latest_jjid,sls.cust_id_map) AS STORE_CD,
       sls.cust_nm_map AS STORE_NAME,
	   sls.latest_outlet_type as store_type,
       sls.latest_chnl AS DSTRBTR_LVL1,
       sls.latest_franchise AS DSTRBTR_LVL2,
       sls.latest_cust_grp2 AS DSTRBTR_LVL3,
       'NA' AS EAN,
       sls.jj_sap_cd_mp_prod_id AS MATL_NUM,
	   sls.jj_sap_cd_mp_prod_desc AS Customer_Product_Desc,
	   sls.latest_put_up as put_up,
	   sls.latest_region as region,
	   sls.latest_area as zone_or_area,
	   sls.sls_qty as SO_SLS_QTY, 
	   sls.niv as SO_SLS_VALUE,
	   sls.latest_put_up as msl_product_code,
		sls.latest_put_up as msl_product_desc,
		sls.latest_outlet_type as retail_env,
		sls.latest_cust_grp2 as channel
	from edw_indonesia_noo_analysis sls LEFT JOIN edw_vw_os_time_dim b 
		on to_date(sls.bill_dt)= b.CAL_DATE
),
union2 as(
SELECT 'POS' AS DATA_SRC,
       'ID' AS 	CNTRY_CD,
       'Indonesia' AS CNTRY_NM,
       LEFT(yearmonth,4)::INT AS YEAR,
       yearmonth AS MNTH_ID,
	   null AS WEEK_ID,
	   TO_DATE(yearmonth||'01','YYYYMMDD') AS DAY,
	   LEFT(yearmonth,4)::INT  as univ_year,
	   Right(yearmonth,2)::INT as univ_month,
	   iqp.parameter_value as SOLDTO_CODE,
       POS.dstrbtr_grp_cd AS DISTRIBUTOR_CODE,
       POS.dstrbtr_grp_cd AS DISTRIBUTOR_NAME,
       nvl(POS.customer_store_code, POS.customer_brnch_code) AS STORE_CD,
       nvl(POS.customer_store_name, POS.customer_brnch_name) AS STORE_NAME,
	   POS.customer_store_channel as store_type,
       NULL AS DSTRBTR_LVL1,
       NULL AS DSTRBTR_LVL2,
       NULL AS DSTRBTR_LVL3,
       'NA' AS EAN,
       POS.jj_sap_prod_id AS MATL_NUM,
	   POS.customer_product_desc AS Customer_Product_Desc,
	   'NA' as put_up,
	   'NA' as region,
	   'NA' as zone_or_area,
	   POS.sales_qty as SO_SLS_QTY, 
	   POS.sales_value as SO_SLS_VALUE,
	   'NA' as msl_product_code,
		'NA' as msl_product_desc,
		'NA' as retail_env,
		'NA' as channel
	   
      FROM  edw_id_pos_sellout POS
	  LEFT JOIN itg_query_parameters iqp
on upper(trim(iqp.parameter_name)) = upper(trim(POS.dstrbtr_grp_cd)) and parameter_type='sold_to_code' and country_code='ID'
where dataset = 'Sellout' and jj_sap_prod_id is not null and upper(trim(POS.dstrbtr_grp_cd)) in (select distinct upper(trim(parameter_name)) from itg_query_parameters where parameter_type='sold_to_code' and country_code='ID')

),
transformed as(
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
		base.put_up,
		BASE.region,
		BASE.zone_or_area,
		BASE.so_sls_qty,
		BASE.so_sls_value,
		BASE.msl_product_code,
		BASE.msl_product_desc,
		BASE.retail_env,
		BASE.channel,
		convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crtd_dttm,
		convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm
	FROM
	(
		select * from union1
		--where sls.franchise <> 'OTX NON JUPITER'
		UNION ALL
		select * from union2
	) BASE
	WHERE NOT (nvl(BASE.so_sls_value, 0) = 0 and nvl(BASE.so_sls_qty, 0) = 0) AND BASE.day > (select to_date(param_value,'YYYY-MM-DD') from itg_mds_ap_customer360_config where code='min_date') 
	AND BASE.mnth_id>= (case when (select param_value from itg_mds_ap_customer360_config where code='base_load_id')='ALL' THEN '190001' ELSE to_char(add_months(to_date(current_timestamp()), -((select param_value from itg_mds_ap_customer360_config where code='base_load_id')::integer)), 'YYYYMM')
	END)
)
select * from transformed