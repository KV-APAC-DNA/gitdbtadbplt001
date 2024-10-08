with wks_philippines_regional_sellout_base as
(
    select * from {{ ref('phlwks_integration__wks_philippines_regional_sellout_base') }}
),
edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_material_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
edw_gch_producthierarchy as
(
    select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
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
itg_query_parameters as
(
    select * from {{ source('phlitg_integration', 'itg_query_parameters') }}
),
edw_material_sales_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
final as
(
    SELECT  
        CAST(TIME.YEAR AS VARCHAR(10)) AS YEAR,
        CAST(TIME.QRTR_NO AS VARCHAR(14)) AS QRTR_NO,
        CAST(SELLOUT.MNTH_ID AS VARCHAR(21)) AS MNTH_ID,
        CAST(TIME.MNTH_NO AS VARCHAR(10)) AS mnth_no,
        SELLOUT.DAY  AS CAL_DATE,
        SELLOUT.univ_year  AS univ_year,
        SELLOUT.univ_month  AS univ_month,
        SELLOUT.CNTRY_CD AS COUNTRY_CODE,	   
        SELLOUT.CNTRY_NM AS COUNTRY_NAME,
        SELLOUT.DATA_SRC AS DATA_SOURCE,
        CASE WHEN SELLOUT.DATA_SRC='SELL-OUT' THEN UPPER(TRIM(NVL (NULLIF(PRODUCT.SAP_MAT_DESC,''),'NA'))) ELSE trim(SELLOUT.Customer_Product_Desc) END AS Customer_Product_Desc,
        TRIM(NVL (NULLIF(SELLOUT.SOLDTO_CODE,''),'NA')) AS SOLDTO_CODE,
        TRIM(NVL (NULLIF(SELLOUT.DISTRIBUTOR_CODE,''),'NA')) AS DISTRIBUTOR_CODE,
        TRIM(NVL (NULLIF(SELLOUT.DISTRIBUTOR_NAME,''),'NA')) AS DISTRIBUTOR_NAME,
        TRIM(NVL (NULLIF(SELLOUT.STORE_CD,''),'NA')) AS store_code,
        TRIM(NVL (NULLIF(SELLOUT.store_name,''),'NA')) AS store_name,
        TRIM(NVL (NULLIF(SELLOUT.store_type,''),'NA')) AS store_type,
        DSTRBTR_LVL1 AS DISTRIBUTOR_ADDITIONAL_ATTRIBUTE1,
        DSTRBTR_LVL2 AS DISTRIBUTOR_ADDITIONAL_ATTRIBUTE2,
        DSTRBTR_LVL3 AS DISTRIBUTOR_ADDITIONAL_ATTRIBUTE3,
        TRIM(NVL (NULLIF(CUST.SAP_PRNT_CUST_KEY,''),'NA')) AS SAP_PARENT_CUSTOMER_KEY,
        UPPER(TRIM(NVL (NULLIF(CUST.SAP_PRNT_CUST_DESC,''),'NA'))) AS SAP_PARENT_CUSTOMER_DESCRIPTION,
        TRIM(NVL (NULLIF(CUST.SAP_CUST_CHNL_KEY,''),'NA')) AS SAP_CUSTOMER_CHANNEL_KEY,
        UPPER(TRIM(NVL (NULLIF(CUST.SAP_CUST_CHNL_DESC,''),'NA'))) AS SAP_CUSTOMER_CHANNEL_DESCRIPTION,
        TRIM(NVL (NULLIF(CUST.SAP_CUST_SUB_CHNL_KEY,''),'NA')) AS SAP_CUSTOMER_SUB_CHANNEL_KEY,
        UPPER(TRIM(NVL (NULLIF(CUST.SAP_SUB_CHNL_DESC,''),'NA'))) AS SAP_SUB_CHANNEL_DESCRIPTION,
        TRIM(NVL (NULLIF(CUST.SAP_GO_TO_MDL_KEY,''),'NA')) AS SAP_GO_TO_MDL_KEY,
        UPPER(TRIM(NVL (NULLIF(CUST.SAP_GO_TO_MDL_DESC,''),'NA'))) AS SAP_GO_TO_MDL_DESCRIPTION,
        TRIM(NVL (NULLIF(CUST.SAP_BNR_KEY,''),'NA')) AS SAP_BANNER_KEY,
        UPPER(TRIM(NVL (NULLIF(CUST.SAP_BNR_DESC,''),'NA'))) AS SAP_BANNER_DESCRIPTION,
        TRIM(NVL (NULLIF(CUST.SAP_BNR_FRMT_KEY,''),'NA')) AS SAP_BANNER_FORMAT_KEY,
        UPPER(TRIM(NVL (NULLIF(CUST.SAP_BNR_FRMT_DESC,''),'NA'))) AS SAP_BANNER_FORMAT_DESCRIPTION,
        TRIM(NVL (NULLIF(CUST.RETAIL_ENV,''),'NA')) AS RETAIL_ENVIRONMENT,
        TRIM(NVL (NULLIF(SELLOUT.REGION,''),'NA')) AS REGION,
        TRIM(NVL (NULLIF(SELLOUT.ZONE_OR_AREA,''),'NA')) AS ZONE_OR_AREA,
        TRIM(NVL (NULLIF(CUST.CUST_SEGMT_KEY,''),'NA')) AS CUSTOMER_SEGMENT_KEY,
        TRIM(NVL (NULLIF(CUST.CUST_SEGMENT_DESC,''),'NA')) AS CUSTOMER_SEGMENT_DESCRIPTION, 
        TRIM(NVL (NULLIF(PRODUCT.GPH_PROD_FRNCHSE,''),'NA')) AS GLOBAL_PRODUCT_FRANCHISE,
        TRIM(NVL (NULLIF(PRODUCT.GPH_PROD_BRND,''),'NA')) AS GLOBAL_PRODUCT_BRAND,
        TRIM(NVL (NULLIF(PRODUCT.GPH_PROD_SUB_BRND,''),'NA')) AS GLOBAL_PRODUCT_SUB_BRAND,
        TRIM(NVL (NULLIF(PRODUCT.GPH_PROD_VRNT,''),'NA')) AS GLOBAL_PRODUCT_VARIANT,
        TRIM(NVL (NULLIF(PRODUCT.GPH_PROD_SGMNT,''),'NA')) AS GLOBAL_PRODUCT_SEGMENT,
        TRIM(NVL (NULLIF(PRODUCT.GPH_PROD_SUBSGMNT,''),'NA')) AS GLOBAL_PRODUCT_SUBSEGMENT,
        TRIM(NVL (NULLIF(PRODUCT.GPH_PROD_CTGRY,''),'NA')) AS GLOBAL_PRODUCT_CATEGORY,
        TRIM(NVL (NULLIF(PRODUCT.GPH_PROD_SUBCTGRY,''),'NA')) AS GLOBAL_PRODUCT_SUBCATEGORY,
        TRIM(NVL (NULLIF(PRODUCT.GPH_PROD_PUT_UP_DESC,''),'NA')) AS GLOBAL_PUT_UP_DESCRIPTION,
        SELLOUT.ean AS EAN,
        TRIM(NVL (NULLIF(PRODUCT.SAP_MATL_NUM,''),'NA')) AS SKU_CODE,
        UPPER(TRIM(NVL (NULLIF(PRODUCT.SAP_MAT_DESC,''),'NA'))) AS SKU_DESCRIPTION,
        CASE WHEN  TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY,''),'NA')) IN ('N/A','NA') THEN 'NA'
            ELSE TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY,''),'NA')) END AS PKA_PRODUCT_KEY,
        CASE WHEN  TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY_DESCRIPTION,''),'NA')) IN ('N/A','NA') THEN 'NA'
            ELSE TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY_DESCRIPTION,''),'NA')) END AS PKA_PRODUCT_KEY_DESCRIPTION,
        CAST('PHP' AS VARCHAR) AS FROM_CURRENCY,
        'USD' AS TO_CURRENCY,
            (C.EXCH_RATE/(C.from_ratio*C.to_ratio))::NUMERIC(15,5) AS EXCHANGE_RATE,
            SUM(SO_SLS_QTY) SELLOUT_SALES_QUANTITY,
            SUM(SO_SLS_VALUE) AS SELLOUT_SALES_VALUE,
            SUM(SO_SLS_VALUE*(C.EXCH_RATE/(from_ratio*to_ratio)))::NUMERIC(38,11) SELLOUT_SALES_VALUE_USD,
        SUM(NVL(SO_LIST_PRICE,0)) as LIST_PRICE,
        SUM(NVL(SO_SELLOUT_VALUE_LIST_PRICE,0)) as SELLOUT_VALUE_LIST_PRICE,
            --TRIM(NVL (NULLIF(SELLOUT.msl_product_code,''),'NA')) AS msl_product_code,
            --TRIM(NVL (NULLIF(SELLOUT.msl_product_desc,''),'NA')) AS msl_product_desc,
        CASE WHEN SELLOUT.DATA_SRC='ECOM' THEN 'NA'
             ELSE TRIM(NVL (NULLIF(emsd.mstr_cd,''),'NA')) END AS msl_product_code,
        CASE WHEN SELLOUT.DATA_SRC='ECOM' THEN 'NA'
             ELSE
            (CASE WHEN (UPPER(PRODUCT.PKA_PACKAGE) IN ('MIX PACK', 'ASSORTED PACK') OR PRODUCT.PKA_PACKAGE IS NULL) THEN UPPER(TRIM(NVL (NULLIF(PRODUCT.SAP_MAT_DESC,''),'NA')))
            ELSE (CASE WHEN TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY_DESCRIPTION,''),'NA')) IN ('N/A','NA') THEN 'NA'
            ELSE TRIM(NVL (NULLIF(PRODUCT.PKA_PRODUCT_KEY_DESCRIPTION,''),'NA')) END)
            END) END AS msl_product_desc,
            TRIM(NVL (NULLIF(SELLOUT.channel,''),'NA')) AS channel,
        SELLOUT.crtd_dttm,
        SELLOUT.updt_dttm
    FROM  WKS_PHILIPPINES_REGIONAL_SELLOUT_BASE SELLOUT
    LEFT JOIN (SELECT DISTINCT CAL_YEAR AS YEAR,
        CAL_QRTR_NO AS QRTR_NO,
        CAL_MNTH_ID AS MNTH_ID,
        CAL_MNTH_NO AS MNTH_NO
    FROM EDW_VW_OS_TIME_DIM) TIME
    ON SELLOUT.MNTH_ID=TIME.MNTH_ID
    LEFT JOIN (SELECT DISTINCT 
            EMD.matl_num AS SAP_MATL_NUM,
            EMD.MATL_DESC AS SAP_MAT_DESC,
            EMD.MATL_TYPE_CD AS SAP_MAT_TYPE_CD,
            EMD.MATL_TYPE_DESC AS SAP_MAT_TYPE_DESC,
            EMD.PRODH1 AS SAP_PROD_SGMT_CD,
            EMD.PRODH1_TXTMD AS SAP_PROD_SGMT_DESC,
            EMD.BASE_PROD_DESC AS SAP_BASE_PROD_DESC,
            EMD.MEGA_BRND_DESC AS SAP_MEGA_BRND_DESC,
            EMD.BRND_DESC AS SAP_BRND_DESC,
            EMD.VARNT_DESC AS SAP_VRNT_DESC,
            EMD.PUT_UP_DESC AS SAP_PUT_UP_DESC,
            EMD.PRODH2 AS SAP_GRP_FRNCHSE_CD,
            EMD.PRODH2_TXTMD AS SAP_GRP_FRNCHSE_DESC,
            EMD.PRODH3 AS SAP_FRNCHSE_CD,
            EMD.PRODH3_TXTMD AS SAP_FRNCHSE_DESC,
            EMD.PRODH4 AS SAP_PROD_FRNCHSE_CD,
            EMD.PRODH4_TXTMD AS SAP_PROD_FRNCHSE_DESC,
            EMD.PRODH5 AS SAP_PROD_MJR_CD,
            EMD.PRODH5_TXTMD AS SAP_PROD_MJR_DESC,
            EMD.PRODH5 AS SAP_PROD_MNR_CD,
            EMD.PRODH5_TXTMD AS SAP_PROD_MNR_DESC,
            EMD.PRODH6 AS SAP_PROD_HIER_CD,
            EMD.PRODH6_TXTMD AS SAP_PROD_HIER_DESC,
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
            EMD.PKA_PACKAGE_DESC AS PKA_PACKAGE,
            row_number() over( partition by sap_matl_num order by sap_matl_num) rnk           
            FROM edw_material_dim EMD,
                EDW_GCH_PRODUCTHIERARCHY EGPH
            WHERE LTRIM(EMD.MATL_NUM,'0') = LTRIM(EGPH.MATERIALNUMBER(+),0)
            AND   EMD.PROD_HIER_CD <> '' ) product
    ON LTRIM(SELLOUT.matl_num,'0') =LTRIM(product.sap_matl_num,'0') and rnk=1
    LEFT JOIN (SELECT DISTINCT LTRIM(matl_num,'0') AS sku,
          mstr_cd
       FROM edw_material_sales_dim
       WHERE LTRIM(mstr_cd,'0') <> ' '
       and sls_org in (SELECT parameter_value from itg_query_parameters where country_code='PH' and parameter_name='Customer360' and parameter_type='sls_org')
       and dstr_chnl in (SELECT parameter_value from itg_query_parameters where country_code='PH' and parameter_name='Customer360' and parameter_type='dstr_chnl')) emsd ON LTRIM(emsd.sku,'0') = LTRIM(SELLOUT.matl_num,'0')
    LEFT JOIN (SELECT * FROM (SELECT DISTINCT ECBD.CUST_NUM AS SAP_CUST_ID,
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
    FROM edw_gch_customerhierarchy EGCH,
        edw_customer_sales_dim ECSD,
        edw_customer_base_dim ECBD,
        edw_company_dim ECD,
        edw_dstrbtn_chnl EDC,
        edw_sales_org_dim ESOD,
        edw_code_descriptions CDDES_PCK,
        edw_code_descriptions CDDES_BNRKEY,
        edw_code_descriptions CDDES_BNRFMT,
        edw_code_descriptions CDDES_CHNL,
        edw_code_descriptions CDDES_GTM,
        edw_code_descriptions CDDES_SUBCHNL,
        edw_subchnl_retail_env_mapping SUBCHNL_RETAIL_ENV,
        edw_code_descriptions_manual CODES_SEGMENT,
        (SELECT DISTINCT CUST_NUM,REC_CRT_DT,PRNT_CUST_KEY,ROW_NUMBER() OVER (PARTITION BY CUST_NUM ORDER BY REC_CRT_DT DESC)RN from edw_customer_sales_dim) A
    WHERE EGCH.CUSTOMER(+) = ECBD.CUST_NUM
    AND   ECSD.CUST_NUM = ECBD.CUST_NUM
    AND   A.CUST_NUM = ECSD.CUST_NUM
    AND   ECSD.DSTR_CHNL = EDC.DISTR_CHAN
    AND   ECSD.SLS_ORG = ESOD.SLS_ORG
    AND   ESOD.SLS_ORG_CO_CD = ECD.CO_CD
    AND   A.RN=1
    AND   UPPER(TRIM(CDDES_PCK.CODE_TYPE(+))) = 'PARENT CUSTOMER KEY'
    AND   CDDES_PCK.CODE(+) = ECSD.PRNT_CUST_KEY
    AND   UPPER(TRIM(cddes_bnrkey.code_type(+))) = 'BANNER KEY'
    AND   CDDES_BNRKEY.CODE(+) = ECSD.BNR_KEY
    AND   UPPER(TRIM(cddes_bnrfmt.code_type(+))) = 'BANNER FORMAT KEY'
    AND   CDDES_BNRFMT.CODE(+) = ECSD.BNR_FRMT_KEY
    AND   UPPER(TRIM(cddes_chnl.code_type(+))) = 'CHANNEL KEY'
    AND   CDDES_CHNL.CODE(+) = ECSD.CHNL_KEY
    AND   UPPER(TRIM(cddes_gtm.code_type(+))) = 'GO TO MODEL KEY'
    AND   CDDES_GTM.CODE(+) = ECSD.GO_TO_MDL_KEY
    AND   UPPER(TRIM(cddes_subchnl.code_type(+))) = 'SUB CHANNEL KEY'
    AND   CDDES_SUBCHNL.CODE(+) = ECSD.SUB_CHNL_KEY
    AND   UPPER(SUBCHNL_RETAIL_ENV.SUB_CHANNEL(+)) = UPPER(CDDES_SUBCHNL.CODE_DESC)
    AND   CODES_SEGMENT.code_type(+) = 'Customer Segmentation Key'
    AND   CODES_SEGMENT.CODE(+) = ECSD.segmt_key)
    WHERE RANK = 1) CUST
    ON LTRIM(SELLOUT.SOLDTO_CODE,'0')=LTRIM(CUST.SAP_CUST_ID,'0')

    LEFT JOIN (SELECT *
                FROM vw_edw_reg_exch_rate
                WHERE cntry_key = 'PH'
                AND   TO_CCY = 'USD'
                AND   JJ_MNTH_ID = (SELECT MAX(JJ_MNTH_ID) FROM vw_edw_reg_exch_rate)
                ) C
                
    ON   UPPER(SELLOUT.CNTRY_NM) = UPPER(C.CNTRY_NM)  
                
    where  C.FROM_CCY = 'PHP' 
    GROUP BY 
                TIME.YEAR    ,
                TIME.QRTR_NO  ,
                SELLOUT.DAY,
                SELLOUT.univ_year,
                SELLOUT.univ_month,
                SELLOUT.CNTRY_CD,	   
                SELLOUT.CNTRY_NM,
                SELLOUT.MNTH_ID  ,
                TIME.MNTH_NO  ,
                SELLOUT.DATA_SRC,
                CASE WHEN SELLOUT.DATA_SRC='SELL-OUT' THEN UPPER(TRIM(NVL (NULLIF(PRODUCT.SAP_MAT_DESC,''),'NA'))) ELSE trim(SELLOUT.Customer_Product_Desc) END ,
                SELLOUT.SOLDTO_CODE, 
                DISTRIBUTOR_CODE,
                DISTRIBUTOR_NAME,
                STORE_CD,
                STORE_NAME,
                STORE_TYPE,
                DSTRBTR_LVL1,
                DSTRBTR_LVL2 ,
                DSTRBTR_LVL3 ,
                CUST.SAP_PRNT_CUST_KEY, 
                CUST.SAP_PRNT_CUST_DESC, 
                CUST.SAP_CUST_CHNL_KEY, 
                CUST.SAP_CUST_CHNL_DESC, 
                CUST.SAP_CUST_SUB_CHNL_KEY, 
                CUST.SAP_SUB_CHNL_DESC, 
                CUST.SAP_GO_TO_MDL_KEY, 
                CUST.SAP_GO_TO_MDL_DESC, 
                CUST.SAP_BNR_KEY, 
                CUST.SAP_BNR_DESC, 
                CUST.SAP_BNR_FRMT_KEY, 
                CUST.SAP_BNR_FRMT_DESC, 
                CUST.RETAIL_ENV, 
                SELLOUT.REGION, 
                SELLOUT.ZONE_OR_AREA, 
                CUST.CUST_SEGMT_KEY, 
                CUST.CUST_SEGMENT_DESC, 
                PRODUCT.GPH_PROD_FRNCHSE, 
                PRODUCT.GPH_PROD_BRND, 
                PRODUCT.GPH_PROD_SUB_BRND, 
                PRODUCT.GPH_PROD_VRNT, 
                PRODUCT.GPH_PROD_SGMNT, 
                PRODUCT.GPH_PROD_SUBSGMNT, 
                PRODUCT.GPH_PROD_CTGRY, 
                PRODUCT.GPH_PROD_SUBCTGRY, 
                PRODUCT.GPH_PROD_PUT_UP_DESC, 
                SELLOUT.ean,
                PRODUCT.SAP_MATL_NUM, 
                PRODUCT.SAP_MAT_DESC, 
                PRODUCT.PKA_PRODUCT_KEY, 
                PRODUCT.PKA_PRODUCT_KEY_DESCRIPTION,
                PRODUCT.PKA_PACKAGE, 
                (C.EXCH_RATE/(C.from_ratio*C.to_ratio)),
                --SELLOUT.msl_product_code,
                --SELLOUT.msl_product_desc,
                emsd.mstr_cd,
                SELLOUT.channel,
                SELLOUT.crtd_dttm,
                SELLOUT.updt_dttm
    HAVING NOT (SUM(SELLOUT.so_sls_value) = 0 and SUM(SELLOUT.so_sls_qty) = 0)
)
select * from final