with wks_india_regional_sellout_base as 
(
    select * from {{ ref('indwks_integration__wks_india_regional_sellout_base') }}
),
edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_gch_producthierarchy as 
(
    select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
),
edw_material_dim as 
(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
edw_product_key_attributes as
(
    select * from {{ ref('aspedw_integration__edw_product_key_attributes') }}
),
edw_gch_customerhierarchy as
(
    select * from {{ ref('aspedw_integration__edw_gch_customerhierarchy') }}
),
edw_customer_sales_dim as
(
    select * from {{ ref('aspedw_integration__edw_customer_sales_dim') }}
),
edw_customer_base_dim as
(
    select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),
edw_company_dim as
(
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
edw_dstrbtn_chnl as
(
    select * from {{ ref('aspedw_integration__edw_dstrbtn_chnl') }}
),
edw_sales_org_dim as
(
    select * from {{ ref('aspedw_integration__edw_sales_org_dim') }}
),
edw_code_descriptions as
(
    select * from {{ ref('aspedw_integration__edw_code_descriptions') }}
),
edw_subchnl_retail_env_mapping as
(
    select * from {{ source('aspedw_integration', 'edw_subchnl_retail_env_mapping') }}
),
edw_code_descriptions_manual as
(
    select * from {{ source('aspedw_integration', 'edw_code_descriptions_manual') }}
),
vw_edw_reg_exch_rate as
(
    select * from {{ ref('aspedw_integration__vw_edw_reg_exch_rate') }}
),
CAL as
( 
    SELECT DISTINCT 
        CAL_YEAR AS YEAR,
       CAL_QRTR_NO AS QRTR_NO,
       CAL_MNTH_ID AS MNTH_ID,
       CAL_MNTH_NO AS MNTH_NO
    FROM EDW_VW_OS_TIME_DIM
),
PRODUCT as
( 
    SELECT DISTINCT
        EMD.matl_num AS SAP_MATL_NUM, 
        EMD.matl_desc AS SAP_MAT_DESC,
        EMD.matl_type_cd AS SAP_MAT_TYPE_CD,
        EMD.matl_type_desc AS SAP_MAT_TYPE_DESC,
        EMD.prodh1 AS SAP_PROD_SGMT_CD,
        EMD.prodh1_txtmd AS SAP_PROD_SGMT_DESC,
        EMD.base_prod_desc AS SAP_BASE_PROD_DESC,
        EMD.mega_brnd_desc AS SAP_MEGA_BRND_DESC,
        EMD.brnd_desc AS SAP_BRND_DESC,
        EMD.varnt_desc AS SAP_VRNT_DESC,
        EMD.put_up_desc AS SAP_PUT_UP_DESC,
        EMD.prodh2 AS SAP_GRP_FRNCHSE_CD,
        EMD.prodh2_txtmd AS SAP_GRP_FRNCHSE_DESC,
        EMD.prodh3 AS SAP_FRNCHSE_CD,
        EMD.prodh3_txtmd AS SAP_FRNCHSE_DESC,
        EMD.prodh4 AS SAP_PROD_FRNCHSE_CD,
        EMD.prodh4_txtmd AS SAP_PROD_FRNCHSE_DESC,
        EMD.prodh5 AS SAP_PROD_MJR_CD,
        EMD.prodh5_txtmd AS SAP_PROD_MJR_DESC,
        EMD.prodh5 AS SAP_PROD_MNR_CD,
        EMD.prodh5_txtmd AS SAP_PROD_MNR_DESC,
        EMD.prodh6 AS SAP_PROD_HIER_CD,
        EMD.prodh6_txtmd AS SAP_PROD_HIER_DESC,
        EMD.pka_product_key as pka_product_key,
        EMD.pka_product_key_description as pka_product_key_description,
        EGPH."region" AS GPH_REGION,
        EGPH.regional_franchise AS GPH_REG_FRNCHSE,
        EGPH.regional_franchise_group AS GPH_REG_FRNCHSE_GRP,
        EGPH.GCPH_FRANCHISE AS GPH_PROD_FRNCHSE,
        EGPH.GCPH_BRAND AS GPH_PROD_BRND,
        EGPH.GCPH_SUBBRAND AS GPH_PROD_SUB_BRND,
        EGPH.GCPH_VARIANT AS GPH_PROD_VRNT,
        EGPH.GCPH_NEEDSTATE AS GPH_PROD_NEEDSTATE,
        EGPH.GCPH_CATEGORY AS GPH_PROD_CTGRY,
        EGPH.GCPH_SUBCATEGORY AS GPH_PROD_SUBCTGRY,
        EGPH.GCPH_SEGMENT AS GPH_PROD_SGMNT,
        EGPH.GCPH_SUBSEGMENT AS GPH_PROD_SUBSGMNT,
        EGPH.PUT_UP_CODE AS GPH_PROD_PUT_UP_CD,
        EGPH.PUT_UP_DESCRIPTION AS GPH_PROD_PUT_UP_DESC,
        EGPH.SIZE AS GPH_PROD_SIZE,
        EGPH.UNIT_OF_MEASURE AS GPH_PROD_SIZE_UOM,
        row_number() over( partition by sap_matl_num order by sap_matl_num) rnk
        FROM edw_material_dim EMD,
        EDW_GCH_PRODUCTHIERARCHY EGPH 
        WHERE LTRIM(EMD.MATL_NUM,0) = ltrim(EGPH.MATERIALNUMBER(+),0)
        AND   EMD.PROD_HIER_CD <> ''
),
PROD_KEY1 as
(
    select a.ctry_nm, a.ean_upc, a.sku, a.pka_productkey, a.pka_productdesc
    FROM (SELECT ctry_nm, ltrim(ean_upc, '0') AS ean_upc, ltrim(matl_num, '0') as sku, pka_productkey, pka_productdesc, lst_nts AS nts_date
        FROM edw_product_key_attributes
        WHERE (matl_type_cd = 'FERT' OR matl_type_cd = 'HALB' OR matl_type_cd = 'SAPR') AND lst_nts IS NOT null) a
    JOIN (SELECT ctry_nm, ltrim(ean_upc, '0') AS ean_upc, ltrim(matl_num, '0') as sku, lst_nts AS latest_nts_date, 
        row_number() OVER( 
        PARTITION BY ctry_nm, ean_upc
        ORDER BY lst_nts DESC) AS row_number
        FROM edw_product_key_attributes
        WHERE (matl_type_cd = 'FERT' OR matl_type_cd = 'HALB' OR matl_type_cd = 'SAPR') AND lst_nts IS NOT null) b
    ON a.ctry_nm = b.ctry_nm AND a.ean_upc = b.ean_upc AND a.sku = b.sku
    AND b.latest_nts_date = a.nts_date AND b.row_number = 1 AND a.ctry_nm = 'India'
),
PROD_KEY2 as
(
    SELECT a.ctry_nm, a.ean_upc, a.sku, a.pka_productkey, a.pka_productdesc
	FROM (SELECT ctry_nm, ltrim(ean_upc, '0') AS ean_upc, ltrim(matl_num, '0') as sku, pka_productkey, pka_productdesc, lst_nts AS nts_date
        FROM edw_product_key_attributes
        WHERE (matl_type_cd = 'FERT' OR matl_type_cd = 'HALB' OR matl_type_cd = 'SAPR') AND lst_nts IS NOT null) a
	JOIN ( SELECT ctry_nm, ltrim(ean_upc, '0') AS ean_upc, ltrim(matl_num, '0') as sku, lst_nts AS latest_nts_date, 
        row_number() OVER( 
        PARTITION BY ctry_nm, ean_upc
        ORDER BY lst_nts DESC) AS row_number
        FROM edw_product_key_attributes
        WHERE (matl_type_cd = 'FERT' OR matl_type_cd = 'HALB' OR matl_type_cd = 'SAPR') AND lst_nts IS NOT null) b
	ON a.ctry_nm = b.ctry_nm AND a.ean_upc = b.ean_upc AND a.sku = b.sku
	AND b.latest_nts_date = a.nts_date AND b.row_number = 1 and a.ctry_nm = 'India'
),
CUSTOMER as
(
    SELECT * FROM 
    (SELECT DISTINCT 
ECBD.CUST_NUM AS SAP_CUST_ID,
ECBD.CUST_NM AS SAP_CUST_NM,
ECSD.SLS_ORG AS SAP_SLS_ORG,
ECD.COMPANY AS SAP_CMP_ID,
ECD.CTRY_KEY AS SAP_CNTRY_CD,
ECD.CTRY_NM AS SAP_CNTRY_NM,
ECSD.PRNT_CUST_KEY AS SAP_PRNT_CUST_KEY, 
CDDES_PCK.CODE_DESC AS SAP_PRNT_CUST_DESC, 
ECSD.CHNL_KEY AS SAP_CUST_CHNL_KEY,  
CDDES_CHNL.CODE_DESC AS SAP_CUST_CHNL_DESC,  
ECSD.SUB_CHNL_KEY AS SAP_CUST_SUB_CHNL_KEY,  
CDDES_SUBCHNL.CODE_DESC AS SAP_SUB_CHNL_DESC,  
ECSD.GO_TO_MDL_KEY AS SAP_GO_TO_MDL_KEY,  
CDDES_GTM.CODE_DESC AS SAP_GO_TO_MDL_DESC,  
ECSD.BNR_KEY AS SAP_BNR_KEY,  
CDDES_BNRKEY.CODE_DESC AS SAP_BNR_DESC,  
ECSD.BNR_FRMT_KEY AS SAP_BNR_FRMT_KEY,  
CDDES_BNRFMT.CODE_DESC AS SAP_BNR_FRMT_DESC,  
SUBCHNL_RETAIL_ENV.RETAIL_ENV,
EGCH.GCGH_REGION AS GCH_REGION,
EGCH.GCGH_CLUSTER AS GCH_CLUSTER,
EGCH.GCGH_SUBCLUSTER AS GCH_SUBCLUSTER,
EGCH.GCGH_MARKET AS GCH_MARKET,
EGCH.GCCH_RETAIL_BANNER AS GCH_RETAIL_BANNER,
ECSD.SEGMT_KEY AS CUST_SEGMT_KEY,
CODES_SEGMENT.code_desc AS cust_segment_desc,
ROW_NUMBER() OVER (PARTITION BY SAP_CUST_ID ORDER BY SAP_PRNT_CUST_KEY DESC) AS RANK

FROM 
EDW_GCH_CUSTOMERHIERARCHY EGCH,
EDW_CUSTOMER_SALES_DIM ECSD,
EDW_CUSTOMER_BASE_DIM ECBD,
EDW_COMPANY_DIM ECD,
EDW_DSTRBTN_CHNL EDC,
EDW_SALES_ORG_DIM ESOD,
EDW_CODE_DESCRIPTIONS CDDES_PCK,       
EDW_CODE_DESCRIPTIONS CDDES_BNRKEY,
EDW_CODE_DESCRIPTIONS CDDES_BNRFMT,
EDW_CODE_DESCRIPTIONS CDDES_CHNL,
EDW_CODE_DESCRIPTIONS CDDES_GTM,
EDW_CODE_DESCRIPTIONS CDDES_SUBCHNL,
EDW_SUBCHNL_RETAIL_ENV_MAPPING SUBCHNL_RETAIL_ENV,
edw_code_descriptions_manual CODES_SEGMENT,
      (SELECT DISTINCT CUST_NUM,REC_CRT_DT,PRNT_CUST_KEY,ROW_NUMBER() OVER (PARTITION BY CUST_NUM ORDER BY REC_CRT_DT DESC)RN from EDW_CUSTOMER_SALES_DIM 
	  where sls_org in 
	 (select distinct sls_org from edw_sales_org_dim where stats_crncy IN ('INR', 'LKR', 'BDT'))
	 )A
WHERE EGCH.CUSTOMER (+) = ECBD.CUST_NUM
AND   ECSD.CUST_NUM = ECBD.CUST_NUM
AND   A.CUST_NUM = ECSD.CUST_NUM
AND   ECSD.DSTR_CHNL = EDC.DISTR_CHAN
AND   ECSD.SLS_ORG = ESOD.SLS_ORG
AND   ESOD.SLS_ORG_CO_CD = ECD.CO_CD
AND   ECSD.SLS_ORG IN 
	(select distinct sls_org from edw_sales_org_dim where stats_crncy IN ('INR', 'LKR', 'BDT'))
AND A.RN=1
AND   trim(Upper(CDDES_PCK.CODE_TYPE(+))) = 'PARENT CUSTOMER KEY'
AND   CDDES_PCK.CODE(+) = ECSD.PRNT_CUST_KEY
AND   trim(Upper(cddes_bnrkey.code_type(+))) = 'BANNER KEY'
AND   CDDES_BNRKEY.CODE(+) = ECSD.BNR_KEY
AND   trim(Upper(cddes_bnrfmt.code_type(+))) = 'BANNER FORMAT KEY'
AND   CDDES_BNRFMT.CODE(+) = ECSD.BNR_FRMT_KEY
AND   trim(Upper(cddes_chnl.code_type(+))) = 'CHANNEL KEY'
AND   CDDES_CHNL.CODE(+) = ECSD.CHNL_KEY
AND   trim(Upper(cddes_gtm.code_type(+))) = 'GO TO MODEL KEY'
AND   CDDES_GTM.CODE(+) = ECSD.GO_TO_MDL_KEY
AND   trim(Upper(cddes_subchnl.code_type(+)))= 'SUB CHANNEL KEY'
AND   CDDES_SUBCHNL.CODE(+) = ECSD.SUB_CHNL_KEY
AND   UPPER(SUBCHNL_RETAIL_ENV.SUB_CHANNEL(+)) = UPPER(CDDES_SUBCHNL.CODE_DESC)
AND   CODES_SEGMENT.code_type(+) = 'Customer Segmentation Key'
AND   CODES_SEGMENT.CODE(+) = ECSD.segmt_key
)where RANK=1
),
CURRENCY as
(
    SELECT * FROM vw_edw_reg_exch_rate 
    WHERE cntry_key='IN' AND TO_CCY='USD' AND JJ_MNTH_ID=(SELECT MAX(JJ_MNTH_ID) FROM vw_edw_reg_exch_rate)
),
transformed as
(
    SELECT 
        YEAR,
        QRTR_NO,
        MNTH_ID,
        mnth_no,
        CAL_DATE,
        univ_year,
        univ_month,
        COUNTRY_CODE,	   
        COUNTRY_NAME,
        DATA_SOURCE,
        Customer_Product_Desc,
        SOLDTO_CODE,
        DISTRIBUTOR_CODE,
        DISTRIBUTOR_NAME,
        STORE_CODE,
        STORE_NAME,
        store_type,
        DISTRIBUTOR_ADDITIONAL_ATTRIBUTE1,
        DISTRIBUTOR_ADDITIONAL_ATTRIBUTE2,
        DISTRIBUTOR_ADDITIONAL_ATTRIBUTE3,
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
        REGION,
        ZONE_OR_AREA,
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
        SKU_CODE,
        SKU_DESCRIPTION,
        CASE WHEN PKA_PRODUCT_KEY IN ('N/A','NA') THEN 'NA'
            ELSE PKA_PRODUCT_KEY END AS PKA_PRODUCT_KEY,
        CASE WHEN PKA_PRODUCT_KEY_DESCRIPTION IN ('N/A','NA') THEN 'NA'
            ELSE PKA_PRODUCT_KEY_DESCRIPTION END AS PKA_PRODUCT_KEY_DESCRIPTION,
        FROM_CURRENCY,
        TO_CURRENCY,
        EXCHANGE_RATE,
        SELLOUT_SALES_QUANTITY,
        SELLOUT_SALES_VALUE,
        SELLOUT_SALES_VALUE_USD,
        RTRUNIQUECODE,
        REGION_NAME,
        ZONE_NAME,
        TOWN_NAME,
        BUSINESS_CHANNEL,
        RETAILER_CATEGORY_NAME,
        CLASS_DESC,
        MOTHERSKU_CODE,
        MOTHERSKU_NAME,
        RTRLATITUDE,
        RTRLONGITUDE,
        msl_product_code,
        msl_product_desc,
        store_grade,
        retail_env,
        channel,
        crtd_dttm,
        updt_dttm
    FROM
(
    SELECT 
        CAL.YEAR AS YEAR,
        CAST(CAL.QRTR_NO AS VARCHAR) AS QRTR_NO,
        CAST(CAL.MNTH_ID AS VARCHAR) AS MNTH_ID,
        CAL.MNTH_NO AS MNTH_NO,
        SELLOUT.DAY AS CAL_DATE,
        SELLOUT.univ_year  AS univ_year,
        SELLOUT.univ_month  AS univ_month,
        SELLOUT.CNTRY_CD AS country_code,
        SELLOUT.CNTRY_NM AS country_name,
        SELLOUT.DATA_SRC AS data_source,
        SELLOUT.Customer_Product_Desc,
        TRIM(NVL (NULLIF(SELLOUT.SOLD_TO_CODE,''),'NA')) AS SOLDTO_CODE,
        TRIM(NVL (NULLIF(SELLOUT.DISTRIBUTOR_CODE,''),'NA')) AS DISTRIBUTOR_CODE,
        TRIM(NVL (NULLIF(SELLOUT.DISTRIBUTOR_NAME,''),'NA')) AS DISTRIBUTOR_NAME,
        TRIM(NVL (NULLIF(SELLOUT.store_code,''),'NA')) AS store_code,
        TRIM(NVL (NULLIF(SELLOUT.store_name,''),'NA')) AS store_name,
        TRIM(NVL (NULLIF(SELLOUT.STORE_TYPE_CODE,''),'NA')) AS store_type,
        TRIM(NVL (NULLIF(SELLOUT.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE1,''),'NA'))  AS DISTRIBUTOR_ADDITIONAL_ATTRIBUTE1,
        TRIM(NVL (NULLIF(SELLOUT.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE2,''),'NA'))  AS DISTRIBUTOR_ADDITIONAL_ATTRIBUTE2,
        TRIM(NVL (NULLIF(SELLOUT.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE3,''),'NA'))  AS DISTRIBUTOR_ADDITIONAL_ATTRIBUTE3,
        TRIM(NVL (NULLIF(CUSTOMER.SAP_PRNT_CUST_KEY,''),'NA')) AS sap_parent_customer_key,
        UPPER(TRIM(NVL (NULLIF(CUSTOMER.SAP_PRNT_CUST_DESC,''),'NA'))) AS sap_parent_customer_description,
        TRIM(NVL (NULLIF(CUSTOMER.SAP_CUST_CHNL_KEY,''),'NA')) AS sap_customer_channel_key,
        UPPER(TRIM(NVL (NULLIF(CUSTOMER.SAP_CUST_CHNL_DESC,''),'NA'))) AS sap_customer_channel_description,
        TRIM(NVL (NULLIF(CUSTOMER.SAP_CUST_SUB_CHNL_KEY,''),'NA')) AS sap_customer_sub_channel_key,
        UPPER(TRIM(NVL (NULLIF(CUSTOMER.SAP_SUB_CHNL_DESC,''),'NA'))) AS sap_sub_channel_description,
        TRIM(NVL (NULLIF(CUSTOMER.SAP_GO_TO_MDL_KEY,''),'NA')) AS SAP_GO_TO_MDL_KEY,
        UPPER(TRIM(NVL (NULLIF(CUSTOMER.SAP_GO_TO_MDL_DESC,''),'NA'))) AS SAP_GO_TO_MDL_DESCRIPTION,
        TRIM(NVL (NULLIF(CUSTOMER.SAP_BNR_KEY,''),'NA')) AS SAP_BANNER_KEY,
        UPPER(TRIM(NVL (NULLIF(CUSTOMER.SAP_BNR_DESC,''),'NA'))) AS SAP_BANNER_DESCRIPTION,
        TRIM(NVL (NULLIF(CUSTOMER.SAP_BNR_FRMT_KEY,''),'NA')) AS SAP_BANNER_FORMAT_KEY,
        UPPER(TRIM(NVL (NULLIF(CUSTOMER.SAP_BNR_FRMT_DESC,''),'NA'))) AS SAP_BANNER_FORMAT_DESCRIPTION,
        TRIM(NVL (NULLIF(CUSTOMER.RETAIL_ENV,''),'NA')) AS retail_environment,
        TRIM(NVL (NULLIF(SELLOUT.REGION,''),'NA')) AS REGION,
        TRIM(NVL (NULLIF(SELLOUT.ZONE_OR_AREA,''),'NA')) AS ZONE_OR_AREA,
        TRIM(NVL (NULLIF(CUSTOMER.CUST_SEGMT_KEY,''),'NA')) AS CUSTOMER_SEGMENT_KEY,
        TRIM(NVL (NULLIF(CUSTOMER.cust_segment_desc,''),'NA')) AS CUSTOMER_SEGMENT_DESCRIPTION,
        TRIM(NVL (NULLIF(PRODUCT.gph_prod_frnchse ,''),'NA'))  AS GLOBAL_PRODUCT_FRANCHISE,
        TRIM(NVL (NULLIF(PRODUCT.GPH_PROD_BRND ,''),'NA'))  AS GLOBAL_PRODUCT_BRAND,
        TRIM(NVL (NULLIF(PRODUCT.GPH_PROD_SUB_BRND ,''),'NA'))  AS GLOBAL_PRODUCT_SUB_BRAND,
        TRIM(NVL (NULLIF(PRODUCT.GPH_PROD_VRNT  ,''),'NA')) AS GLOBAL_PRODUCT_VARIANT,
        TRIM(NVL (NULLIF(PRODUCT.GPH_PROD_SGMNT,''),'NA'))   AS GLOBAL_PRODUCT_SEGMENT,
        TRIM(NVL (NULLIF(PRODUCT.GPH_PROD_SUBSGMNT,''),'NA'))   AS GLOBAL_PRODUCT_SUBSEGMENT,
        TRIM(NVL (NULLIF(PRODUCT.GPH_PROD_CTGRY,''),'NA'))   AS GLOBAL_PRODUCT_CATEGORY,
        TRIM(NVL (NULLIF(PRODUCT.GPH_PROD_SUBCTGRY  ,''),'NA')) AS GLOBAL_PRODUCT_SUBCATEGORY,
        TRIM(NVL (NULLIF(PRODUCT.gph_prod_put_up_desc,''),'NA'))  AS GLOBAL_PUT_UP_DESCRIPTION,
        SELLOUT.EAN as EAN,
        LTRIM(SELLOUT.SKU_CD,0) AS SKU_CODE,
        UPPER(PRODUCT.SAP_MAT_DESC) AS SKU_DESCRIPTION,
        CASE WHEN  TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY,''),'NA'))
        WHEN TRIM(NVL (NULLIF(PROD_KEY1.pka_productkey,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PROD_KEY1.pka_productkey,''),'NA'))
        WHEN TRIM(NVL (NULLIF(PROD_KEY2.pka_productkey,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PROD_KEY2.pka_productkey,''),'NA'))
        ELSE TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY,''),'NA')) END AS PKA_PRODUCT_KEY,
        CASE WHEN  TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY_DESCRIPTION,''),'NA'))
        WHEN TRIM(NVL (NULLIF(PROD_KEY1.pka_productkey,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PROD_KEY1.pka_productdesc,''),'NA'))
        WHEN TRIM(NVL (NULLIF(PROD_KEY2.pka_productkey,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PROD_KEY2.pka_productdesc,''),'NA'))
        ELSE TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY_DESCRIPTION,''),'NA')) END AS PKA_PRODUCT_KEY_DESCRIPTION,
        CURRENCY.FROM_CCY as FROM_Currency,
        CURRENCY.TO_CCY as to_currency,
        (CURRENCY.EXCH_RATE/(CURRENCY.from_ratio*CURRENCY.to_ratio))::NUMERIC(15,5) as exchange_rate,
        SUM(SELLOUT.SO_SLS_QTY) AS SELLOUT_SALES_QUANTITY,
        SUM(SELLOUT.SO_SLS_VALUE) AS SELLOUT_SALES_VALUE,
        SUM(SO_SLS_VALUE*(CURRENCY.EXCH_RATE/(CURRENCY.from_ratio*CURRENCY.to_ratio)))::NUMERIC(38,11) AS SELLOUT_SALES_VALUE_USD,
        TRIM(NVL (NULLIF(SELLOUT.RTRUNIQUECODE,''),'NA')) AS RTRUNIQUECODE,
        TRIM(NVL (NULLIF(SELLOUT.REGION_NAME,''),'NA')) AS REGION_NAME,
        TRIM(NVL (NULLIF(SELLOUT.ZONE_NAME,''),'NA')) AS ZONE_NAME,
        TRIM(NVL (NULLIF(SELLOUT.TOWN_NAME,''),'NA')) AS TOWN_NAME,
        TRIM(NVL (NULLIF(SELLOUT.BUSINESS_CHANNEL,''),'NA')) AS BUSINESS_CHANNEL,
        TRIM(NVL (NULLIF(SELLOUT.RETAILER_CATEGORY_NAME,''),'NA')) AS RETAILER_CATEGORY_NAME,
        TRIM(NVL (NULLIF(SELLOUT.CLASS_DESC,''),'NA')) AS CLASS_DESC,
        TRIM(NVL (NULLIF(SELLOUT.MOTHERSKU_CODE,''),'NA')) AS MOTHERSKU_CODE,
        TRIM(NVL (NULLIF(SELLOUT.MOTHERSKU_NAME,''),'NA')) AS MOTHERSKU_NAME,
        TRIM(NVL (NULLIF(SELLOUT.RTRLATITUDE,''),'NA')) AS RTRLATITUDE,
        TRIM(NVL (NULLIF(SELLOUT.RTRLONGITUDE,''),'NA')) AS RTRLONGITUDE,
        TRIM(NVL (NULLIF(SELLOUT.msl_product_code,''),'NA')) AS msl_product_code,
        TRIM(NVL (NULLIF(SELLOUT.msl_product_desc,''),'NA')) AS msl_product_desc,
        TRIM(NVL (NULLIF(SELLOUT.store_grade,''),'NA')) AS store_grade,
        TRIM(NVL (NULLIF(SELLOUT.retail_env,''),'NA')) AS retail_env,
        TRIM(NVL (NULLIF(SELLOUT.channel,''),'NA')) AS channel,
        SELLOUT.crtd_dttm,
        SELLOUT.updt_dttm
    FROM WKS_INDIA_REGIONAL_SELLOUT_BASE  SELLOUT

LEFT JOIN CAL
ON SELLOUT.MNTH_ID=CAL.MNTH_ID

LEFT JOIN PRODUCT
ON  LTRIM(SELLOUT.SKU_CD,'0')= LTRIM(PRODUCT.SAP_MATL_NUM,'0') and rnk=1

LEFT JOIN PROD_KEY1
ON ltrim(SELLOUT.SKU_CD, '0') = ltrim(PROD_KEY1.sku, '0')

LEFT JOIN PROD_KEY2
ON ltrim(SELLOUT.ean, '0') = ltrim(PROD_KEY2.ean_upc, '0') 

LEFT JOIN CUSTOMER
ON LTRIM(SELLOUT.SOLD_TO_CODE,'0')= LTRIM(CUSTOMER.SAP_CUST_ID,'0')

LEFT JOIN CURRENCY
ON UPPER(SELLOUT.CNTRY_NM) = UPPER(CURRENCY.CNTRY_NM)
AND CURRENCY.FROM_CCY = 'INR'

GROUP BY 
    CAL.YEAR,
    CAL.QRTR_NO,
    CAL.MNTH_ID,
    CAL.MNTH_NO,
    SELLOUT.DAY,
    SELLOUT.univ_year,
    SELLOUT.univ_month,
    SELLOUT.CNTRY_CD,
    SELLOUT.CNTRY_NM,
    SELLOUT.DATA_SRC,
    SELLOUT.customer_product_desc,
    SELLOUT.SOLD_TO_CODE, 
    SELLOUT.DISTRIBUTOR_CODE, 
    SELLOUT.DISTRIBUTOR_NAME, 
    SELLOUT.store_code, 
    SELLOUT.store_name, 
    SELLOUT.store_type_code,
    SELLOUT.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE1,
    SELLOUT.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE2,
    SELLOUT.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE3,
    CUSTOMER.SAP_PRNT_CUST_KEY, 
    CUSTOMER.SAP_PRNT_CUST_DESC, 
    CUSTOMER.SAP_CUST_CHNL_KEY, 
    CUSTOMER.SAP_CUST_CHNL_DESC, 
    CUSTOMER.SAP_CUST_SUB_CHNL_KEY,
    CUSTOMER.SAP_SUB_CHNL_DESC, 
    CUSTOMER.SAP_GO_TO_MDL_KEY, 
    CUSTOMER.SAP_GO_TO_MDL_DESC, 
    CUSTOMER.SAP_BNR_KEY, 
    CUSTOMER.SAP_BNR_DESC, 
    CUSTOMER.SAP_BNR_FRMT_KEY,
    CUSTOMER.SAP_BNR_FRMT_DESC, 
    CUSTOMER.RETAIL_ENV, 
    SELLOUT.REGION, 
    SELLOUT.ZONE_OR_AREA, 
    CUSTOMER.CUST_SEGMT_KEY, 
    CUSTOMER.cust_segment_desc, 
    PRODUCT.gph_prod_frnchse, 
    PRODUCT.GPH_PROD_BRND, 
    PRODUCT.GPH_PROD_SUB_BRND, 
    PRODUCT.GPH_PROD_VRNT, 
    PRODUCT.GPH_PROD_SGMNT, 
    PRODUCT.GPH_PROD_SUBSGMNT, 
    PRODUCT.GPH_PROD_CTGRY, 
    PRODUCT.GPH_PROD_SUBCTGRY, 
    PRODUCT.gph_prod_put_up_desc, 
    SELLOUT.EAN,
    SELLOUT.SKU_CD,
    PRODUCT.SAP_MAT_DESC,
    CASE WHEN  TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY,''),'NA'))
    WHEN TRIM(NVL (NULLIF(PROD_KEY1.pka_productkey,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PROD_KEY1.pka_productkey,''),'NA'))
    WHEN TRIM(NVL (NULLIF(PROD_KEY2.pka_productkey,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PROD_KEY2.pka_productkey,''),'NA'))
    ELSE TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY,''),'NA')) END,
    CASE WHEN  TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY_DESCRIPTION,''),'NA'))
    WHEN TRIM(NVL (NULLIF(PROD_KEY1.pka_productkey,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PROD_KEY1.pka_productdesc,''),'NA'))
    WHEN TRIM(NVL (NULLIF(PROD_KEY2.pka_productkey,''),'NA')) NOT IN ('N/A','NA') THEN TRIM(NVL (NULLIF(PROD_KEY2.pka_productdesc,''),'NA'))
    ELSE TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY_DESCRIPTION,''),'NA')) END,
    CURRENCY.FROM_CCY,
    CURRENCY.TO_CCY,
    (CURRENCY.EXCH_RATE/(CURRENCY.from_ratio*CURRENCY.to_ratio)),
    SELLOUT.RTRUNIQUECODE,
    SELLOUT.REGION_NAME,
    SELLOUT.ZONE_NAME,
    SELLOUT.TOWN_NAME,
    SELLOUT.BUSINESS_CHANNEL,
    SELLOUT.RETAILER_CATEGORY_NAME,
    SELLOUT.CLASS_DESC,
    SELLOUT.MOTHERSKU_CODE,
    SELLOUT.MOTHERSKU_NAME,
    SELLOUT.RTRLATITUDE,
    SELLOUT.RTRLONGITUDE,
    SELLOUT.msl_product_code,
    SELLOUT.msl_product_desc,
    SELLOUT.store_grade,
    SELLOUT.retail_env,
    SELLOUT.channel,
    SELLOUT.crtd_dttm,
    SELLOUT.updt_dttm
    HAVING NOT (SUM(SELLOUT.SO_SLS_VALUE) = 0 and SUM(SELLOUT.SO_SLS_QTY) = 0))
)
select * from transformed
