with edw_product_dim as
(
    select * from ({{ ref('indedw_integration__edw_product_dim') }})
),
v_pf_sales_stock_inventory_analysis as
(
    select * from {{source('indedw_integration', 'v_pf_sales_stock_inventory_analysis')}}
),
edw_retailer_calendar_dim as
(
    select * from ({{ ref('indedw_integration__edw_retailer_calendar_dim') }})
),
edw_material_dim as
(
    select * from ({{ ref('aspedw_integration__edw_material_dim') }})
),
edw_gch_producthierarchy as
(
    select * from ({{ ref('aspedw_integration__edw_gch_producthierarchy') }})
),
PRODUCT AS
          (
          Select * from 
 ( SELECT DISTINCT
           
          EMD.matl_num AS SAP_MATL_NUM,
          EMD.MATL_DESC AS SAP_MAT_DESC,
          EMD.MATL_TYPE_CD AS SAP_MAT_TYPE_CD,
          EMD.MATL_TYPE_DESC AS SAP_MAT_TYPE_DESC,
        --  EMD.SAP_BASE_UOM_CD AS SAP_BASE_UOM_CD,
        --  EMD.SAP_PRCHSE_UOM_CD AS SAP_PRCHSE_UOM_CD,
          EMD.PRODH1 AS SAP_PROD_SGMT_CD,
          EMD.PRODH1_TXTMD AS SAP_PROD_SGMT_DESC,
        --  EMD.SAP_BASE_PROD_CD AS SAP_BASE_PROD_CD,
          EMD.BASE_PROD_DESC AS SAP_BASE_PROD_DESC,
       --   EMD.SAP_MEGA_BRND_CD AS SAP_MEGA_BRND_CD,
          EMD.MEGA_BRND_DESC AS SAP_MEGA_BRND_DESC,
        --  EMD.SAP_BRND_CD AS SAP_BRND_CD,
          EMD.BRND_DESC AS SAP_BRND_DESC,
       --   EMD.SAP_VRNT_CD AS SAP_VRNT_CD,
          EMD.VARNT_DESC AS SAP_VRNT_DESC,
       --   EMD.SAP_PUT_UP_CD AS SAP_PUT_UP_CD,
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
		  --EMD.greenlight_sku_flag as greenlight_sku_flag,
		  EMD.pka_product_key as pka_product_key,
		  EMD.pka_product_key_description as pka_product_key_description,
		  EMD.pka_product_key as product_key,
		  EMD.pka_size_desc as pka_size_desc,
		  EMD.pka_product_key_description as product_key_description,
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
		  
          FROM 
        -- (Select * from rg_edw.edw_vw_greenlight_skus where sls_org='5100')  EMD,
		(select * from edw_material_dim) EMD,
          EDW_GCH_PRODUCTHIERARCHY EGPH 
          WHERE LTRIM(EMD.MATL_NUM,'0') = ltrim(EGPH.MATERIALNUMBER(+),0)
          AND   EMD.PROD_HIER_CD <> ''
          AND   LTRIM(EMD.MATL_NUM,'0')  
          IN  (SELECT DISTINCT CAST(PRODUCT_CODE AS VARCHAR) FROM EDW_PRODUCT_DIM)   

) where rnk=1
),
final as
(
SELECT 			 min(bill_dt) as min_date,
TRIM(NVL(NULLIF(T3.GPH_PROD_BRND,''),'NA')) AS brand,
TRIM(NVL(NULLIF(T3.GPH_PROD_VRNT,''),'NA')) AS VARIANT,
TRIM(NVL(NULLIF(T3.GPH_PROD_SGMNT,''),'NA')) AS SEGMENT,
TRIM(NVL(NULLIF(T3.GPH_PROD_CTGRY,''),'NA')) AS PROD_CATEGORY,
TRIM(NVL(NULLIF(T3.pka_product_key,''),'NA')) AS pka_product_key,
TRIM(NVL(NULLIF(T3.pka_size_desc,''),'NA')) AS pka_size_desc,
TRIM(NVL(NULLIF(T1.sap_parent_customer_key,''),'Not Assigned')) as SAP_PRNT_CUST_KEY
FROM (SELECT 
case when TIME.MNTH_ID is null then null else cast((left(TIME.MNTH_ID,4)||'-'||substring(TIME.MNTH_ID,5,6)||'-01') as date) end as bill_dt,
CLS.customer_code AS sap_parent_customer_key,
coalesce(nullif(CLS.product_code,''),'NA') AS MATL_NUM,
SO_VAL
FROM (SELECT customer_code,
product_code,
mth_mm,
(sec_prd_nr_value +sec_prd_nr_value_ret)SO_VAL
from 
v_pf_sales_stock_inventory_analysis
where year >=(date_part(year, convert_timezone('UTC', current_timestamp())) -6)
) AS CLS,
(SELECT DISTINCT A.fisc_yr as YEAR,
CAST(A.qtr AS VARCHAR(14)) AS QRTR_NO , 
CAST (A.mth_mm AS  VARCHAR(21)) AS MNTH_ID ,
A.mth_yyyymm as MNTH_NO 
FROM edw_retailer_calendar_dim A) AS TIME
WHERE CLS.mth_mm IS NOT NULL
AND (CLS.mth_mm)=(TIME.MNTH_ID)
AND CLS.SO_VAL>0
)T1,
PRODUCT T3
WHERE   LTRIM(T3.SAP_MATL_NUM(+),'0') = T1.matl_num
GROUP BY
T3.GPH_PROD_BRND,
T3.GPH_PROD_VRNT,
T3.GPH_PROD_SGMNT,
T3.GPH_PROD_CTGRY,
T3.pka_size_desc,
T3.pka_product_key,
T1.sap_parent_customer_key)
select * from final