with EDW_GCH_CUSTOMERHIERARCHY as(
    select * from {{ ref('aspedw_integration__edw_gch_customerhierarchy') }}
),

EDW_CUSTOMER_SALES_DIM as(
    select * from {{ ref('aspedw_integration__v_edw_customer_sales_dim') }}
),

EDW_CUSTOMER_BASE_DIM as(
    select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),

EDW_COMPANY_DIM as(
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),

EDW_DSTRBTN_CHNL as(
    select * from {{ ref('aspedw_integration__edw_dstrbtn_chnl') }}
),

EDW_SALES_ORG_DIM as(
    select * from {{ ref('aspedw_integration__edw_sales_org_dim') }}
),

EDW_CODE_DESCRIPTIONS as(
    select * from {{ ref('aspedw_integration__edw_code_descriptions') }}
),

EDW_SUBCHNL_RETAIL_ENV_MAPPING as(
    select * from {{ source('aspedw_integration', 'edw_subchnl_retail_env_mapping') }}
),

EDW_CODE_DESCRIPTIONS_MANUAL as(
    select * from {{ source('aspedw_integration', 'edw_code_descriptions_manual') }}
),


transformation as(
select distinct
ECBD.CUST_NUM as SAP_CUST_ID,
                                ECBD.CUST_NM as SAP_CUST_NM,
                                ECSD.SLS_ORG as SAP_SLS_ORG,
                                ECD.COMPANY as SAP_CMP_ID,
                                ECD.CTRY_KEY as SAP_CNTRY_CD,
                                ECD.CTRY_NM as SAP_CNTRY_NM,
                                ECSD.PRNT_CUST_KEY as SAP_PRNT_CUST_KEY,
                                CDDES_PCK.CODE_DESC as SAP_PRNT_CUST_DESC,
                                ECSD.CHNL_KEY as SAP_CUST_CHNL_KEY,
                                CDDES_CHNL.CODE_DESC as SAP_CUST_CHNL_DESC,
                                ECSD.SUB_CHNL_KEY as SAP_CUST_SUB_CHNL_KEY,
                                CDDES_SUBCHNL.CODE_DESC as SAP_SUB_CHNL_DESC,
                                ECSD.GO_TO_MDL_KEY as SAP_GO_TO_MDL_KEY,
                                CDDES_GTM.CODE_DESC as SAP_GO_TO_MDL_DESC,
                                ECSD.BNR_KEY as SAP_BNR_KEY,
                                CDDES_BNRKEY.CODE_DESC as SAP_BNR_DESC,
                                ECSD.BNR_FRMT_KEY as SAP_BNR_FRMT_KEY,
                                CDDES_BNRFMT.CODE_DESC as SAP_BNR_FRMT_DESC,
                                SUBCHNL_RETAIL_ENV.RETAIL_ENV,
                                --REGZONE.REGION_NAME AS REGION,
                                --REGZONE.ZONE_NAME AS ZONE_OR_AREA,
                                EGCH.GCGH_REGION as GCH_REGION,
                                EGCH.GCGH_CLUSTER as GCH_CLUSTER,
                                EGCH.GCGH_SUBCLUSTER as GCH_SUBCLUSTER,
                                EGCH.GCGH_MARKET as GCH_MARKET,
                                EGCH.GCCH_RETAIL_BANNER as GCH_RETAIL_BANNER,
                                ECSD.SEGMT_KEY as CUST_SEGMT_KEY,
                                CODES_SEGMENT.CODE_DESC as CUST_SEGMENT_DESC,
                                ROW_NUMBER() OVER (PARTITION BY SAP_CUST_ID ORDER BY SAP_PRNT_CUST_KEY DESC) AS RANK
                         from EDW_GCH_CUSTOMERHIERARCHY as EGCH,
                              EDW_CUSTOMER_SALES_DIM as ECSD,
                              EDW_CUSTOMER_BASE_DIM as ECBD,
                              EDW_COMPANY_DIM as ECD,
                              EDW_DSTRBTN_CHNL as EDC,
                              EDW_SALES_ORG_DIM as ESOD,
                              EDW_CODE_DESCRIPTIONS as CDDES_PCK,
                              EDW_CODE_DESCRIPTIONS as CDDES_BNRKEY,
                              EDW_CODE_DESCRIPTIONS as CDDES_BNRFMT,
                              EDW_CODE_DESCRIPTIONS as CDDES_CHNL,
                              EDW_CODE_DESCRIPTIONS as CDDES_GTM,
                              EDW_CODE_DESCRIPTIONS as CDDES_SUBCHNL,
                              EDW_SUBCHNL_RETAIL_ENV_MAPPING as SUBCHNL_RETAIL_ENV,
                              EDW_CODE_DESCRIPTIONS_MANUAL as CODES_SEGMENT,
                              (select distinct
CUST_NUM,
                                      REC_CRT_DT,
                                      PRNT_CUST_KEY,
                                      ROW_NUMBER() over (partition by CUST_NUM order by REC_CRT_DT desc) as RN
                               from EDW_CUSTOMER_SALES_DIM) as A
                         where EGCH.CUSTOMER (+) = ECBD.CUST_NUM
                         and   ECSD.CUST_NUM = ECBD.CUST_NUM
                         and   A.CUST_NUM = ECSD.CUST_NUM
                         and   ECSD.DSTR_CHNL = EDC.DISTR_CHAN
                         and   ECSD.SLS_ORG = ESOD.SLS_ORG
                         and   ESOD.SLS_ORG_CO_CD = ECD.CO_CD
                         and   A.RN = 1
                         and   TRIM(UPPER(CDDES_PCK.CODE_TYPE (+))) = 'PARENT CUSTOMER KEY'
                         and   CDDES_PCK.CODE (+) = ECSD.PRNT_CUST_KEY
                         and   TRIM(UPPER(CDDES_BNRKEY.CODE_TYPE (+))) = 'BANNER KEY'
                         and   CDDES_BNRKEY.CODE (+) = ECSD.BNR_KEY
                         and   TRIM(UPPER(CDDES_BNRFMT.CODE_TYPE (+))) = 'BANNER FORMAT KEY'
                         and   CDDES_BNRFMT.CODE (+) = ECSD.BNR_FRMT_KEY
                         and   TRIM(UPPER(CDDES_CHNL.CODE_TYPE (+))) = 'CHANNEL KEY'
                         and   CDDES_CHNL.CODE (+) = ECSD.CHNL_KEY
                         and   TRIM(UPPER(CDDES_GTM.CODE_TYPE (+))) = 'GO TO MODEL KEY'
                         and   CDDES_GTM.CODE (+) = ECSD.GO_TO_MDL_KEY
                         and   TRIM(UPPER(CDDES_SUBCHNL.CODE_TYPE (+))) = 'SUB CHANNEL KEY'
                         and   CDDES_SUBCHNL.CODE (+) = ECSD.SUB_CHNL_KEY
                         and   UPPER(SUBCHNL_RETAIL_ENV.SUB_CHANNEL (+)) = UPPER(CDDES_SUBCHNL.CODE_DESC)
                         and   CODES_SEGMENT.code_type (+) = 'Customer Segmentation Key'
                         and   CODES_SEGMENT.CODE (+) = ECSD.SEGMT_KEY)
                   
                   
                   
                   
select * from transformation 