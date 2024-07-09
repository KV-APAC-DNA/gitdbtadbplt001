
with edw_vw_os_time_dim as (
select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
vw_edw_reg_exch_rate as (
select * from {{ ref('aspedw_integration__vw_edw_reg_exch_rate') }}
),
edw_vw_greenlight_skus as (
select * from {{ ref('aspedw_integration__edw_vw_greenlight_skus') }}
),
edw_gch_producthierarchy as (
select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
),
edw_material_sales_dim as (
select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
edw_gch_customerhierarchy as (
select * from {{ ref('aspedw_integration__edw_gch_customerhierarchy') }}
),
edw_customer_sales_dim as (
select * from {{ ref('aspedw_integration__edw_customer_sales_dim') }}
),
edw_customer_base_dim as (
select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),
edw_company_dim as (
select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
edw_dstrbtn_chnl  as (
select * from {{ ref('aspedw_integration__edw_dstrbtn_chnl') }}
),
edw_sales_org_dim as (
select * from {{ ref('aspedw_integration__edw_sales_org_dim') }}
),
edw_code_descriptions as (
select * from {{ ref('aspedw_integration__edw_code_descriptions') }}
),
edw_subchnl_retail_env_mapping as (
select * from {{ source('aspedw_integration', 'edw_subchnl_retail_env_mapping') }}
),
edw_customer_dim as (
select * from {{ ref('indedw_integration__edw_customer_dim') }}
),
wks_hk_siso_propagate_final as (
select * from {{ ref('ntawks_integration__wks_hk_siso_propagate_final') }}
),
vw_edw_reg_exch_rate as (
select * from {{ ref('aspedw_integration__vw_edw_reg_exch_rate') }}
),
edw_copa_trans_fact as (
select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }}
),
CUSTOMER AS
(
SELECT DISTINCT 
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
REGZONE.REGION_NAME AS REGION,
REGZONE.ZONE_NAME AS ZONE_OR_AREA,
EGCH.GCGH_REGION AS GCH_REGION,
EGCH.GCGH_CLUSTER AS GCH_CLUSTER,
EGCH.GCGH_SUBCLUSTER AS GCH_SUBCLUSTER,
EGCH.GCGH_MARKET AS GCH_MARKET,
EGCH.GCCH_RETAIL_BANNER AS GCH_RETAIL_BANNER,
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
     (SELECT CUST_NUM,
             MIN(DECODE(CUST_DEL_FLAG,NULL,'O','','O',CUST_DEL_FLAG)) AS CUST_DEL_FLAG
      FROM EDW_CUSTOMER_SALES_DIM
      WHERE SLS_ORG IN ('1110')
      GROUP BY CUST_NUM) A,
(SELECT DISTINCT CUSTOMER_CODE,REGION_NAME,ZONE_NAME FROM EDW_CUSTOMER_DIM) REGZONE
WHERE EGCH.CUSTOMER (+) = ECBD.CUST_NUM
AND   ECSD.CUST_NUM = ECBD.CUST_NUM
AND   DECODE(ECSD.CUST_DEL_FLAG,NULL,'O','','O',ECSD.CUST_DEL_FLAG) = A.CUST_DEL_FLAG
AND   A.CUST_NUM = ECSD.CUST_NUM
AND   ECSD.DSTR_CHNL = EDC.DISTR_CHAN
AND   ECSD.SLS_ORG = ESOD.SLS_ORG
AND   ESOD.SLS_ORG_CO_CD = ECD.CO_CD
AND   ECSD.SLS_ORG IN  ('1110')
AND   UPPER(trim(CDDES_PCK.CODE_TYPE(+))) = 'PARENT CUSTOMER KEY'
AND   CDDES_PCK.CODE(+) = ECSD.PRNT_CUST_KEY
AND   UPPER(trim(cddes_bnrkey.code_type(+))) = 'BANNER KEY'
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
AND   LTRIM(ECSD.CUST_NUM,'0')=REGZONE.CUSTOMER_CODE(+)
),
transformed as (
SELECT month,
		TRIM(NVL (NULLIF(SISO.sap_parent_customer_key,''),'NA')) AS DSTRBTR_GRP_CD,
		TRIM(NVL(NULLIF(T4.GPH_PROD_BRND,''),'NA')) AS BRAND,
        TRIM(NVL(NULLIF(T4.GPH_PROD_VRNT,''),'NA')) AS VARIANT,
        TRIM(NVL(NULLIF(T4.GPH_PROD_SGMNT,''),'NA')) AS SEGMENT, 
        TRIM(NVL(NULLIF(T4.GPH_PROD_CTGRY,''),'NA')) AS PROD_CATEGORY,
		TRIM(NVL(NULLIF(T4.pka_size_desc,''),'NA')) AS pka_size_desc,
		TRIM(NVL(NULLIF(T4.pka_product_key,''),'NA')) AS pka_product_key,
		CASE
         WHEN T3.SAP_PRNT_CUST_KEY = '' OR T3.SAP_PRNT_CUST_KEY IS NULL THEN 'Not Assigned'
         ELSE T3.SAP_PRNT_CUST_KEY
       END AS SAP_PRNT_CUST_KEY,
		sum(last_3months_so_value) as last_3months_so_val,
	    sum(last_6months_so_value) as last_6months_so_val,
	    sum( last_12months_so_value) as last_12months_so_val,
		sum( last_36months_so_value) as last_36months_so_val,
		CASE WHEN COALESCE(last_36months_so_val,0)>0 and COALESCE(last_12months_so_val,0)<=0 THEN 'N'
		ELSE 'Y' END AS healthy_inventory
FROM
(Select * from wks_hk_siso_propagate_final) SISO,
(
          SELECT DISTINCT 
          EMD.matl_num AS SAP_MATL_NUM,
          EMD.pka_product_key as pka_product_key,
		  EMD.pka_size_desc as pka_size_desc,
		  EGPH.GCPH_BRAND AS GPH_PROD_BRND,
          EGPH.GCPH_VARIANT AS GPH_PROD_VRNT,
          EGPH.GCPH_CATEGORY AS GPH_PROD_CTGRY,
          EGPH.GCPH_SEGMENT AS GPH_PROD_SGMNT        
          FROM 
          (Select * from edw_vw_greenlight_skus WHERE sls_org in ( '1110')) EMD,
			EDW_GCH_PRODUCTHIERARCHY EGPH
			WHERE EMD.MATL_NUM = ltrim(EGPH.MATERIALNUMBER(+),0)
			AND   EMD.PROD_HIER_CD <> ''
			AND   LTRIM(EMD.MATL_NUM,'0')  
			IN  (SELECT DISTINCT LTRIM(MATL_NUM,'0') FROM edw_material_sales_dim WHERE sls_org ='1110')
)T4,
(SELECT * FROM CUSTOMER WHERE RANK=1) T3
WHERE LTRIM(T4.SAP_MATL_NUM(+),'0') = SISO.matl_num
AND   LTRIM(T3.SAP_CUST_ID(+),'0') = SISO.sap_parent_customer_key
GROUP BY
month,
sap_parent_customer_key,
GPH_PROD_BRND ,
GPH_PROD_VRNT ,
GPH_PROD_SGMNT , 
GPH_PROD_CTGRY ,
pka_size_desc,
pka_product_key,
sap_prnt_cust_key
),
final as (
select 
month::varchar(23) as month,
dstrbtr_grp_cd::varchar(50) as dstrbtr_grp_cd,
brand::varchar(30) as brand,
variant::varchar(100) as variant,
segment::varchar(50) as segment,
prod_category::varchar(50) as prod_category,
pka_size_desc::varchar(30) as pka_size_desc,
pka_product_key::varchar(68) as pka_product_key,
sap_prnt_cust_key::varchar(12) as sap_prnt_cust_key,
last_3months_so_val::number(38,4) as last_3months_so_val,
last_6months_so_val::number(38,4) as last_6months_so_val,
last_12months_so_val::number(38,4) as last_12months_so_val,
last_36months_so_val::number(38,4) as last_36months_so_val,
healthy_inventory::varchar(1) as healthy_inventory
from transformed
)
select * from final

