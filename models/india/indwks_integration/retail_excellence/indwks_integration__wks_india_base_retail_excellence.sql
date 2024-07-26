



as (

    SELECT COUNTRY_CODE AS CNTRY_CD,
       COUNTRY_NAME AS CNTRY_NM,
       DATA_SOURCE,
       MD5(nvl(DISTRIBUTOR_CODE,'dc')||nvl(DISTRIBUTOR_NAME,'dn')||nvl (STORE_CODE,'rtr')||nvl(STORE_NAME,'sn')
           ||nvl(STORE_TYPE,'st')||nvl(REGION_NAME,'rn')||nvl(ZONE_NAME,'zn')||nvl(TOWN_NAME,'tn')
           ||nvl(BUSINESS_CHANNEL,'bc')||nvl(RETAILER_CATEGORY_NAME,'rcn')|| nvl(CLASS_DESC,'cd')
           ||MOTHERSKU_CODE||nvl(MOTHERSKU_NAME,'msn')
           ||nvl(SAP_PARENT_CUSTOMER_KEY, 'spck')||nvl(SAP_PARENT_CUSTOMER_DESCRIPTION,'spscd') 
           ||nvl(SAP_CUSTOMER_CHANNEL_KEY,'scck')||nvl(SAP_CUSTOMER_CHANNEL_DESCRIPTION,'sccd') 
           ||nvl(SAP_CUSTOMER_SUB_CHANNEL_KEY, 'scsck')||nvl(SAP_SUB_CHANNEL_DESCRIPTION,'sscd')
           ||nvl(CUSTOMER_SEGMENT_KEY,'csk')||nvl(CUSTOMER_SEGMENT_DESCRIPTION,'csd')
           ||nvl(SAP_GO_TO_MDL_KEY,'sgmk')||nvl(SAP_GO_TO_MDL_DESCRIPTION,'sgmd')
           ||nvl(SAP_BANNER_KEY,'sbk')||nvl(SAP_BANNER_DESCRIPTION,'sbd') 
           ||nvl(SAP_BANNER_FORMAT_KEY,'sbfk')||nvl (SAP_BANNER_FORMAT_DESCRIPTION,'sbfd') 
           ||nvl(RETAIL_ENVIRONMENT,'re')
           ||nvl (GLOBAL_PRODUCT_FRANCHISE,'gpf') ||nvl (GLOBAL_PRODUCT_BRAND,'gpb') 
           ||nvl (GLOBAL_PRODUCT_SUB_BRAND,'gpsb') ||nvl (GLOBAL_PRODUCT_VARIANT,'gpv') 
           ||nvl (GLOBAL_PRODUCT_SEGMENT,'gps')||nvl (GLOBAL_PRODUCT_SUBSEGMENT,'gpss') 
           ||nvl (GLOBAL_PRODUCT_CATEGORY,'gpc') ||nvl (GLOBAL_PRODUCT_SUBCATEGORY,'gpsc') 
           ||nvl (GLOBAL_PUT_UP_DESCRIPTION,'gpud') ||nvl(PKA_PRODUCT_KEY,'pk') ||nvl(PKA_PRODUCT_KEY_DESCRIPTION,'pkd')
           ) AS SELLOUT_DIM_KEY,--add gcph hier as well   
       YEAR,
       MNTH_ID,
       DISTRIBUTOR_CODE,
       DISTRIBUTOR_NAME,
       --SOLDTO_CODE,
       STORE_CODE,
       STORE_NAME,
       STORE_TYPE,
       REGION_NAME,
       ZONE_NAME,
       TOWN_NAME,
       BUSINESS_CHANNEL,
       RETAILER_CATEGORY_NAME,
       CLASS_DESC,
       --RTRLATITUDE,
       --RTRLONGITUDE,
       MOTHERSKU_CODE,
       MOTHERSKU_NAME,
       SAP_PARENT_CUSTOMER_KEY,
       SAP_PARENT_CUSTOMER_DESCRIPTION,
       SAP_CUSTOMER_CHANNEL_KEY,
       SAP_CUSTOMER_CHANNEL_DESCRIPTION,
       SAP_CUSTOMER_SUB_CHANNEL_KEY,
       SAP_SUB_CHANNEL_DESCRIPTION,
       SAP_GO_TO_MDL_KEY,
       SAP_GO_TO_MDL_DESCRIPTION,
       SAP_BANNER_KEY,
       SAP_BANNER_DESCRIPTION,
       SAP_BANNER_FORMAT_KEY,
       SAP_BANNER_FORMAT_DESCRIPTION,
       RETAIL_ENVIRONMENT,
       --REGION,
       --ZONE_OR_AREA,
       CUSTOMER_SEGMENT_KEY,
       CUSTOMER_SEGMENT_DESCRIPTION,
       GLOBAL_PRODUCT_FRANCHISE,
       GLOBAL_PRODUCT_BRAND,
       GLOBAL_PRODUCT_SUB_BRAND,
       GLOBAL_PRODUCT_VARIANT,
       GLOBAL_PRODUCT_SEGMENT,
       GLOBAL_PRODUCT_SUBSEGMENT,
       GLOBAL_PRODUCT_CATEGORY,
       GLOBAL_PRODUCT_SUBCATEGORY,
       GLOBAL_PUT_UP_DESCRIPTION,
       --EAN,
       --SKU_CODE,SKU_DESCRIPTION,
       PKA_PRODUCT_KEY,
       PKA_PRODUCT_KEY_DESCRIPTION,
       CAST(SUM(SELLOUT_SALES_QUANTITY) AS DECIMAL(38,6)) AS SO_SLS_QTY,
       CAST(SUM(SELLOUT_SALES_VALUE) AS NUMERIC(38,6)) AS SO_SLS_VALUE,
       CAST(AVG(SELLOUT_SALES_QUANTITY) AS DECIMAL(38,6)) AS SO_AVG_QTY,
       SUM(SALES_VALUE_LIST_PRICE) AS SALES_VALUE_LIST_PRICE,
	   SYSDATE AS CRT_DTTM from
	   
(SELECT COUNTRY_CODE ,
       COUNTRY_NAME,
       DATA_SOURCE, 
       YEAR,
       MNTH_ID,
       DISTRIBUTOR_CODE,
       DISTRIBUTOR_NAME,
       --SOLDTO_CODE,
       distributor_additional_attribute1 as STORE_CODE,--RTRUNIQUECODE AS STORE_CODE,
       upper(STORE_NAME) as STORE_NAME,
       STORE_TYPE,
       region as REGION_NAME,
       zone_or_area as ZONE_NAME,
       'NA' as TOWN_NAME,
       channel as BUSINESS_CHANNEL,
       retail_env as RETAILER_CATEGORY_NAME,
       store_grade as CLASS_DESC,
       --RTRLATITUDE, distributor_additional_attribute2 as RTRLATITUDE,
       --RTRLONGITUDE, distributor_additional_attribute3 as RTRLONGITUDE,
       msl_product_code as MOTHERSKU_CODE,
       msl_product_desc as MOTHERSKU_NAME,
       SAP_PARENT_CUSTOMER_KEY,
       SAP_PARENT_CUSTOMER_DESCRIPTION,
       SAP_CUSTOMER_CHANNEL_KEY,
       SAP_CUSTOMER_CHANNEL_DESCRIPTION,
       SAP_CUSTOMER_SUB_CHANNEL_KEY,
       SAP_SUB_CHANNEL_DESCRIPTION,
       SAP_GO_TO_MDL_KEY,
       SAP_GO_TO_MDL_DESCRIPTION,
       SAP_BANNER_KEY,
       SAP_BANNER_DESCRIPTION,
       SAP_BANNER_FORMAT_KEY,
       SAP_BANNER_FORMAT_DESCRIPTION,
       RETAIL_ENVIRONMENT,
       --REGION,
       --ZONE_OR_AREA,
       CUSTOMER_SEGMENT_KEY,
       CUSTOMER_SEGMENT_DESCRIPTION,
       GLOBAL_PRODUCT_FRANCHISE,
       GLOBAL_PRODUCT_BRAND,
       GLOBAL_PRODUCT_SUB_BRAND,
       GLOBAL_PRODUCT_VARIANT,
       GLOBAL_PRODUCT_SEGMENT,
       GLOBAL_PRODUCT_SUBSEGMENT,
       GLOBAL_PRODUCT_CATEGORY,
       GLOBAL_PRODUCT_SUBCATEGORY,
       GLOBAL_PUT_UP_DESCRIPTION,
       EAN,
       SKU_CODE,SKU_DESCRIPTION,
       PKA_PRODUCT_KEY,
       PKA_PRODUCT_KEY_DESCRIPTION,
       SELLOUT_SALES_QUANTITY,
       SELLOUT_SALES_VALUE,
       ((NVL(LP1.AMOUNT::NUMERIC(38,6),LP2.AMOUNT::NUMERIC(38,6)))*MAIN.SELLOUT_SALES_QUANTITY) AS SALES_VALUE_LIST_PRICE
        FROM (select * FROM RG_EDW.EDW_RPT_REGIONAL_SELLOUT_OFFTAKE 
WHERE COUNTRY_CODE = 'IN' and DATA_SOURCE = 'SELL-OUT') MAIN --IN_EDW.EDW_INDIA_REGIONAL_SELLOUT MAIN
        left join (Select * from (select distinct a.material,a.dt_from,a.valid_to,a.amount,a.sls_org,b.ctry_key,row_number() OVER(PARTITION BY 
	   ltrim(a.material, 0) ORDER BY to_date(a.valid_to, 'YYYYMMDD') DESC, to_date(a.dt_from, 'YYYYMMDD') DESC) AS rn FROM 
	   (select material,dt_from,valid_to,max(amount) as amount,sls_org,cdl_dttm,currency from rg_sdl.sdl_raw_sap_bw_price_list where sls_org IN 
	   --('5100','6100','510A','6000')
	   (select distinct sls_org from rg_edw.edw_sales_org_dim where stats_crncy IN ('INR', 'LKR', 'BDT'))
	   group by material,dt_from,valid_to,sls_org,cdl_dttm,currency) a LEFT JOIN (select distinct ctry_key,sls_org,crncy_key from rg_edw.edw_sales_org_dim where ctry_key<>'') b on a.sls_org=b.sls_org and a.currency=b.crncy_key and a.cdl_dttm = (select max(cdl_dttm) 
	   from rg_sdl.sdl_raw_sap_bw_price_list)) where rn=1) lp1 on ltrim(main.sku_code,'0') = ltrim(lp1.material, '0') and 
	   TRIM(UPPER(main.country_code))=TRIM(UPPER(lp1.ctry_key)) and (main.cal_date between to_date(dt_from, 'YYYYMMDD') and 
	   to_date(valid_to, 'YYYYMMDD'))   
	   left join (Select * from (SELECT distinct ltrim(material, 0) AS material, amount, pg_catalog.row_number() OVER(PARTITION BY 
	   ltrim(material, 0) ORDER BY to_date(valid_to, 'YYYYMMDD') DESC, to_date(dt_from, 'YYYYMMDD') DESC) AS rn FROM rg_edw.edw_list_price 
	   WHERE sls_org in (select distinct sls_org from rg_edw.edw_sales_org_dim where ctry_key = 'IN')) where rn = 1) lp2 on ltrim(main.sku_code, '0') = ltrim(lp2.material, '0'))
        
      WHERE DATA_SOURCE = 'SELL-OUT'
	  and MNTH_ID >= (select last_36mnths from rg_edw.edw_vw_cal_Retail_excellence_Dim)::numeric
	  and mnth_id <= (select prev_mnth from rg_edw.edw_vw_cal_Retail_excellence_Dim)::numeric
      --AND   MNTH_ID >(SELECT TO_CHAR(add_months ((SELECT to_date(MAX(mnth_id),'YYYYMM') FROM rg_edw.edw_rpt_regional_sellout_offtake where country_code='IN' and data_source='SELL-OUT'  ),-(SELECT CAST(parameter_value AS INT) FROM in_itg.itg_query_parameters WHERE parameter_name = 'RETAIL_EXCELLENCE_BASE_NO_OF_MONTHS')),'yyyymm'))
     -- AND   MNTH_ID <= TO_CHAR(SYSDATE,'YYYYMM')
     --AND  NOT (NVL(SELLOUT_SALES_QUANTITY,0) = 0 AND NVL(SELLOUT_SALES_QUANTITY,0) = 0) 
     GROUP BY COUNTRY_CODE,
       COUNTRY_NAME,
       DATA_SOURCE,
        YEAR,
       MNTH_ID,
       DISTRIBUTOR_CODE,
       DISTRIBUTOR_NAME,
       --SOLDTO_CODE,
       STORE_CODE,
       STORE_NAME,
       STORE_TYPE,
       REGION_NAME ,
       ZONE_NAME ,
       TOWN_NAME,
       BUSINESS_CHANNEL,
       RETAILER_CATEGORY_NAME,
       CLASS_DESC,
       --RTRLATITUDE,
       --RTRLONGITUDE,
       MOTHERSKU_CODE,
       MOTHERSKU_NAME,
       SAP_PARENT_CUSTOMER_KEY,
       SAP_PARENT_CUSTOMER_DESCRIPTION,
       SAP_CUSTOMER_CHANNEL_KEY,
       SAP_CUSTOMER_CHANNEL_DESCRIPTION,
       SAP_CUSTOMER_SUB_CHANNEL_KEY,
       SAP_SUB_CHANNEL_DESCRIPTION,
       SAP_GO_TO_MDL_KEY,
       SAP_GO_TO_MDL_DESCRIPTION,
       SAP_BANNER_KEY,
       SAP_BANNER_DESCRIPTION,
       SAP_BANNER_FORMAT_KEY,
       SAP_BANNER_FORMAT_DESCRIPTION,
       RETAIL_ENVIRONMENT,
       --REGION,
       --ZONE_OR_AREA,
       CUSTOMER_SEGMENT_KEY,
       CUSTOMER_SEGMENT_DESCRIPTION,
       GLOBAL_PRODUCT_FRANCHISE,
       GLOBAL_PRODUCT_BRAND,
       GLOBAL_PRODUCT_SUB_BRAND,
       GLOBAL_PRODUCT_VARIANT,
       GLOBAL_PRODUCT_SEGMENT,
       GLOBAL_PRODUCT_SUBSEGMENT,
       GLOBAL_PRODUCT_CATEGORY,
       GLOBAL_PRODUCT_SUBCATEGORY,
       GLOBAL_PUT_UP_DESCRIPTION,
       --EAN,
       --SKU_CODE,SKU_DESCRIPTION,
       PKA_PRODUCT_KEY,
       PKA_PRODUCT_KEY_DESCRIPTION
)