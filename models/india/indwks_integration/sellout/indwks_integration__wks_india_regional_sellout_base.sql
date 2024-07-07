with v_rpt_sales_details_with_history as
(
    select * from {{ ref('indedw_integration__v_rpt_sales_details_with_history') }}
),
edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_vw_pos_offtake as
(
    select * from {{ ref('indedw_integration__edw_vw_pos_offtake') }}
),
itg_mds_in_key_accounts_mapping as
(
    select * from {{ ref('inditg_integration__itg_mds_in_key_accounts_mapping') }}
),
edw_ecommerce_offtake as
(
    select * from {{ source('indedw_integration', 'edw_ecommerce_offtake') }}
),
itg_mds_in_key_accounts_mapping_offtake_upd as
(
    select * from {{ source('inditg_integration', 'itg_mds_in_key_accounts_mapping_offtake_upd') }}
),
edw_product_key_attributes as
(
    select * from {{ ref('aspedw_integration__edw_product_key_attributes') }}
),
itg_mds_ap_customer360_config as
(
    select * from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }}
),
union1 as
(
    SELECT 'IN' AS CNTRY_CD,
             'India' AS CNTRY_NM,
             'SELL-OUT' AS DATA_SRC,
             SELLOUT.CUSTOMER_CODE AS DISTRIBUTOR_CODE,
             SELLOUT.CUSTOMER_NAME AS DISTRIBUTOR_NAME,
             SELLOUT.RETAILER_CODE AS STORE_CODE,
             SELLOUT.RETAILER_NAME AS STORE_NAME,
             SELLOUT.RETAILER_CHANNEL_3 AS STORE_TYPE_CODE,
             SELLOUT.CUSTOMER_CODE AS SOLD_TO_CODE,
             SELLOUT.PRODUCT_CODE AS SKU_CD,
			 SELLOUT.product_name AS CUSTOMER_PRODUCT_DESC,
			 SELLOUT.region_name as region,
			 SELLOUT.zone_name as zone_or_area,
			 SELLOUT.RTRUNIQUECODE AS RTRUNIQUECODE,
             SELLOUT.region_name AS region_name,
             SELLOUT.zone_name AS zone_name,
             SELLOUT.town_name AS town_name,
             SELLOUT.business_channel AS business_channel,
             SELLOUT.retailer_category_name AS retailer_category_name,
             SELLOUT.class_desc AS class_desc,
             SELLOUT.MOTHERSKU_CODE AS MOTHERSKU_CODE,
             SELLOUT.MOTHERSKU_NAME AS MOTHERSKU_NAME,
             SELLOUT.rtrlatitude AS rtrlatitude,
             SELLOUT.rtrlongitude AS rtrlongitude,								
             'NA' AS EAN,
             B."year"::INT as YEAR,
             B.MNTH_ID::INT AS MNTH_ID,
             TO_DATE(TRIM(SELLOUT.INVOICE_DATE), 'YYYYMMDD') as DAY,
			 B.cal_year as univ_year,
			 Right(B.cal_mnth_id,2)::INT as univ_month,           
             SUM(SELLOUT.QUANTITY) AS SO_SLS_QTY,
             SUM(SELLOUT.ACHIEVEMENT_NR) AS SO_SLS_VALUE,
			 SELLOUT.MOTHERSKU_CODE as msl_product_code,
			SELLOUT.MOTHERSKU_NAME as msl_product_desc,
			SELLOUT.class_desc as store_grade,
			retailer_category_name as retail_env,
			SELLOUT.business_channel as channel,
			RTRUNIQUECODE AS distributor_additional_attribute1,
		   RTRLATITUDE AS distributor_additional_attribute2,
		   RTRLONGITUDE AS distributor_additional_attribute3
		   FROM V_RPT_SALES_DETAILS_WITH_HISTORY SELLOUT
           LEFT JOIN EDW_VW_OS_TIME_DIM B
           ON SELLOUT.DAY=B.CAL_DATE_ID
             GROUP BY SELLOUT.CUSTOMER_CODE,
               SELLOUT.CUSTOMER_NAME,
               SELLOUT.RETAILER_CODE,
               SELLOUT.RETAILER_NAME,
               SELLOUT.RETAILER_CHANNEL_3,
               SELLOUT.CUSTOMER_CODE,
               SELLOUT.PRODUCT_CODE,
			   SELLOUT.product_name,
			   sellout.rtruniquecode,
               SELLOUT.region_name,
               SELLOUT.zone_name,
               SELLOUT.town_name,
               SELLOUT.business_channel,
               SELLOUT.retailer_category_name,
               SELLOUT.class_desc,
               SELLOUT.MOTHERSKU_CODE,
               SELLOUT.MOTHERSKU_NAME,
               SELLOUT.CLASS_DESC,
               SELLOUT.rtrlatitude,
               SELLOUT.rtrlongitude,
               EAN,
               B."year",
               B.MNTH_ID,             
               TO_DATE(TRIM(SELLOUT.INVOICE_DATE), 'YYYYMMDD'),
			   B.cal_year,
			   Right(B.cal_mnth_id,2)
),
union2 as
(
    SELECT 'IN' AS 	CNTRY_CD,
       'India' AS CNTRY_NM,
	   'POS' AS DATA_SRC,
	   account_name AS DISTRIBUTOR_CODE,
       account_name AS DISTRIBUTOR_NAME,
       store_cd AS STORE_CODE,
       store_name AS STORE_NAME,
	   re AS STORE_TYPE_CODE,
	   ka.code as SOLD_TO_CODE,
	   sap_cd AS SKU_CD,
	   article_name AS CUSTOMER_PRODUCT_DESC,
	   "region" as REGION,
	   zone as ZONE_OR_AREA,
	   'NA' AS RTRUNIQUECODE,
		 'NA' AS region_name,
		 'NA' AS zone_name,
		 'NA' AS town_name,
		 'NA' AS business_channel,
		 'NA' AS retailer_category_name,
		 'NA' AS class_desc,
		 'NA' AS MOTHERSKU_CODE,
		 'NA' AS MOTHERSKU_NAME,
		 'NA' AS rtrlatitude,
		 'NA' AS rtrlongitude,	
	   'NA' AS EAN,
       fisc_year::INT AS YEAR,
       period::INT AS MNTH_ID,
	   TO_DATE(period|| '01','YYYYMMDD') AS DAY,
	   LEFT(period,4)::INT  as univ_year,
	   Right(period,2)::INT as univ_month,
	   sls_qty as SO_SLS_QTY, 
	   sls_val_lcy as SO_SLS_VALUE,
	   'NA' as msl_product_code,
		'NA' as msl_product_desc,
		'NA'as store_grade,
		'NA' as retail_env,
		'NA' as channel,
		'NA' AS distributor_additional_attribute1,
	   'NA' AS distributor_additional_attribute2,
	   'NA' AS distributor_additional_attribute3
      from edw_vw_pos_offtake pos left join 
	(select upper(account_name_code) account_name_code, max(code) as code from itg_mds_in_key_accounts_mapping where channel_name_code = 'Key Account' group by 1) ka
	on upper(pos.account_name) = upper(ka.account_name_code) 
	where level = 'J&J'
),
union3 as
(
    SELECT 'IN' AS 	CNTRY_CD,
       'India' AS CNTRY_NM,
	   'ECOM' AS DATA_SRC,
	   platform AS DISTRIBUTOR_CODE,
       account_name AS DISTRIBUTOR_NAME,
       'NA' AS STORE_CODE,
       'NA' AS STORE_NAME,
	   'NA' AS STORE_TYPE_CODE,
	   ka.code as SOLD_TO_CODE,
	   pkm.sku AS SKU_CD,
	   retailer_product_name AS CUSTOMER_PRODUCT_DESC,
	   'NA' as region,
	   'NA' as zone_or_area,
	   'NA' AS RTRUNIQUECODE,
		 'NA' AS region_name,
		 'NA' AS zone_name,
		 'NA' AS town_name,
		 'NA' AS business_channel,
		 'NA' AS retailer_category_name,
		 'NA' AS class_desc,
		 'NA' AS MOTHERSKU_CODE,
		 'NA' AS MOTHERSKU_NAME,
		 'NA' AS rtrlatitude,
		 'NA' AS rtrlongitude,
	   ean AS EAN,
       LEFT(transaction_date,4)::INT AS YEAR,
       (LEFT(transaction_date,4)||SUBSTRING(transaction_date, 6, 2))::INT AS MNTH_ID,
	   TO_DATE(transaction_date,'YYYY-MM-DD') AS DAY,
	   LEFT(transaction_date,4)::INT  as univ_year,
	   SUBSTRING(transaction_date, 6, 2)::INT as univ_month,
	   sales_qty as SO_SLS_QTY, 
	   sales_value as SO_SLS_VALUE,
	   'NA' as msl_product_code,
		'NA' as msl_product_desc,
		'NA'as store_grade,
		'NA' as retail_env,
		'NA' as channel,
		'NA' AS distributor_additional_attribute1,
	   'NA' AS distributor_additional_attribute2,
	   'NA' AS distributor_additional_attribute3
      from edw_ecommerce_offtake ecom left join 
	(select upper(account_name_as_per_offtake_data) as account_name_code, max(code) as code
	from itg_mds_in_key_accounts_mapping_offtake_upd where channel_name_code = 'E-Commerce' group by 1) ka
	on upper(ecom.account_name) = upper(ka.account_name_code)
	left join
	(select a.ctry_nm, a.ean_upc, a.sku, a.pka_productkey, a.pka_productdesc
	FROM ( SELECT ctry_nm, ltrim(ean_upc, '0') AS ean_upc, ltrim(matl_num, '0') as sku, pka_productkey, pka_productdesc, lst_nts AS nts_date
         FROM edw_product_key_attributes
        WHERE (matl_type_cd = 'FERT' OR matl_type_cd = 'HALB' OR matl_type_cd = 'SAPR') AND lst_nts IS NOT null) a
	JOIN ( SELECT ctry_nm, ltrim(ean_upc, '0') AS ean_upc, ltrim(matl_num, '0') as sku, lst_nts AS latest_nts_date, 
        row_number() OVER( 
        PARTITION BY ctry_nm, ean_upc
        ORDER BY lst_nts DESC) AS row_number
         FROM edw_product_key_attributes
        WHERE (matl_type_cd = 'FERT' OR matl_type_cd = 'HALB' OR matl_type_cd = 'SAPR') AND lst_nts IS NOT null
        ) b
	ON a.ctry_nm = b.ctry_nm AND a.ean_upc = b.ean_upc AND a.sku = b.sku
	AND b.latest_nts_date = a.nts_date AND b.row_number = 1  where a.ctry_nm = 'India') pkm
	ON pkm.ean_upc = ecom.ean
),
transformed as
(
    SELECT 
        BASE.CNTRY_CD,
        BASE.CNTRY_NM,
        BASE.DATA_SRC,
        BASE.DISTRIBUTOR_CODE,
        BASE.DISTRIBUTOR_NAME,
        BASE.STORE_CODE,
        BASE.STORE_NAME,
        BASE.STORE_TYPE_CODE,
        BASE.SOLD_TO_CODE,
        BASE.SKU_CD,
        BASE.CUSTOMER_PRODUCT_DESC,	  
        BASE.region,
        BASE.zone_or_area,
        Base.RTRUNIQUECODE,
        BASE.REGION_NAME,
        BASE.ZONE_NAME,
        BASE.TOWN_NAME,
        BASE.BUSINESS_CHANNEL,
        BASE.RETAILER_CATEGORY_NAME,
        BASE.CLASS_DESC,
        BASE.MOTHERSKU_CODE,
        BASE.MOTHERSKU_NAME,
        BASE.RTRLATITUDE,
        BASE.RTRLONGITUDE,
        BASE.EAN,
        BASE.year,
        BASE.MNTH_ID,
        BASE.day,
        BASE.univ_year,
        BASE.univ_month, 
        BASE.SO_SLS_QTY,
        BASE.SO_SLS_VALUE,
        BASE.msl_product_code,
        BASE.msl_product_desc,
        BASE.store_grade,
        BASE.retail_env,
        BASE.channel,
        BASE.distributor_additional_attribute1,
        BASE.distributor_additional_attribute2,
        BASE.distributor_additional_attribute3,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
	FROM
	(
		select * from union1
		UNION ALL
		select * from union2
        UNION ALL
		select * from union3
	) BASE
    WHERE NOT (nvl(BASE.so_sls_value, 0) = 0 and nvl(BASE.so_sls_qty, 0) = 0) AND BASE.day > (select to_date(param_value,'YYYY-MM-DD') from itg_mds_ap_customer360_config where code='min_date') 
    AND BASE.mnth_id>= (case when (select param_value from itg_mds_ap_customer360_config where code='base_load_in')='ALL' THEN '190001' ELSE to_char(add_months(to_date(current_timestamp()), -((select param_value from itg_mds_ap_customer360_config where code='base_load_in')::integer)), 'YYYYMM')
    END)
)
select * from transformed
