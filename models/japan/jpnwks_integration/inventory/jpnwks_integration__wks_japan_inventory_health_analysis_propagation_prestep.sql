with edw_vw_os_time_dim as(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
vw_edw_reg_exch_rate as(
    select * from {{ ref('aspedw_integration__vw_edw_reg_exch_rate') }}
),
edw_gch_customerhierarchy as(
    select * from {{ ref('aspedw_integration__edw_gch_customerhierarchy') }}
),
edw_customer_sales_dim as(
    select * from {{ ref('aspedw_integration__edw_customer_sales_dim') }}
),
edw_customer_base_dim as(
    select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),
edw_company_dim as(
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
edw_dstrbtn_chnl as(
    select * from {{ ref('aspedw_integration__edw_dstrbtn_chnl') }}
),
edw_sales_org_dim as(
    select * from {{ ref('aspedw_integration__edw_sales_org_dim') }}
),
edw_code_descriptions as(
    select * from {{ ref('aspedw_integration__edw_code_descriptions') }}
),
edw_subchnl_retail_env_mapping as(
    select * from {{ ref('aspedw_integration__edw_subchnl_retail_env_mapping') }}
),
itg_parameter_reg_inventory as(
    select * from {{ ref('aspitg_integration__itg_parameter_reg_inventory') }}
),
jp_inv_coverage_area_region_mapping as(
    select * from {{ ref('jpnedw_integration__jp_inv_coverage_area_region_mapping') }}
),
edi_cstm_m as(
    select * from {{ ref('jpnedw_integration__edi_cstm_m') }}
),
wks_japan_siso_propagate_final as(
    select * from {{ ref('jpnwks_integration__wks_japan_siso_propagate_final') }}
),
cstm_m as(
    select * from {{ ref('jpnedw_integration__cstm_m') }}
),
edw_material_dim as(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
edw_gch_producthierarchy as(
    select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
),
final as
(
with Regional as 
(
WITH ONSESEA AS
(

Select * from
 (
 
 WITH cal AS
(
 SELECT DISTINCT cal_year as year,cal_qrtr_no as qrtr_no,cal_MNTH_ID as mnth_id,cal_MNTH_NO as mnth_no FROM EDW_VW_OS_TIME_DIM 
),
CURRENCY AS
(
  Select * from  vw_edw_reg_exch_rate where cntry_key='JP' and jj_year >=(date_part(year, convert_timezone('UTC', current_timestamp())) -2)
),

REGONALCUST as 
(with CUSTOMER AS
(
SELECT DISTINCT 
LTRIM(ECBD.CUST_NUM,'0') AS SAP_CUST_ID,
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
REG_JAPAN.region as Region, --Added region 
REG_JAPAN.prefecture_name_eng as ZONE_OR_AREA,---Added Area 
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
      WHERE SLS_ORG IN (select parameter_value from itg_parameter_reg_inventory  where country_name='Japan')
      GROUP BY CUST_NUM) A,
	 
 (Select distinct jis_cd,region,prefecture_name_eng from jp_inv_coverage_area_region_mapping) REG_JAPAN,
 (Select distinct cstm_cd,jis_prfct_cd from edi_cstm_m )REG_CUST_JAP_REGION 

WHERE EGCH.CUSTOMER (+) = ECBD.CUST_NUM
AND   ECSD.CUST_NUM = ECBD.CUST_NUM

and    ltrim(ECBD.CUST_NUM,0)=REG_CUST_JAP_REGION.cstm_cd(+)

AND   REG_CUST_JAP_REGION.jis_prfct_cd=REG_JAPAN.jis_cd    

AND   DECODE(ECSD.CUST_DEL_FLAG,NULL,'O','','O',ECSD.CUST_DEL_FLAG) = A.CUST_DEL_FLAG
AND   A.CUST_NUM = ECSD.CUST_NUM
AND   ECSD.DSTR_CHNL = EDC.DISTR_CHAN
AND   ECSD.SLS_ORG = ESOD.SLS_ORG
AND   ESOD.SLS_ORG_CO_CD = ECD.CO_CD
AND   ECSD.SLS_ORG IN (select parameter_value from itg_parameter_reg_inventory  where country_name='Japan')
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
)
SELECT distinct 
sap_cust_id,sap_cust_nm,sap_sls_org,sap_cmp_id,sap_cntry_cd,sap_cntry_nm,sap_prnt_cust_key,sap_prnt_cust_desc,sap_cust_chnl_key,sap_cust_chnl_desc,sap_cust_sub_chnl_key,sap_sub_chnl_desc,sap_go_to_mdl_key,sap_go_to_mdl_desc,sap_bnr_key,sap_bnr_desc,sap_bnr_frmt_key,sap_bnr_frmt_desc,retail_env,
Region,ZONE_OR_AREA,
gch_region,gch_cluster,gch_subcluster,gch_market,gch_retail_banner,rank
FROM 
(Select * from customer )T1,
(select cust.cstm_cd, cust.cstm_nm,nvl(cust.rebate_rep_cd,'NA') as rebate_rep_cd,nvl(cust_grp.cstm_nm,'NA') as cstm_grp_nm
from
  (select cstm_cd, cstm_nm, rebate_rep_cd 
    from cstm_m ) cust,
  (select cstm_cd, cstm_nm 
    from cstm_m ) cust_grp
where cust.rebate_rep_cd = cust_grp.cstm_cd (+))T2 
WHERE T1.sap_cust_id(+)=nvl(T2.rebate_rep_cd,'NA'))
,

SISO as  

(
Select * from wks_japan_siso_propagate_final where left(month,4)>= (date_part(year, convert_timezone('UTC', current_timestamp())) -2) 
and sap_parent_customer_desc is not null and  sap_parent_customer_desc!=''
)
,
PRODUCT AS
(
SELECT DISTINCT 
           
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
        --  EMD.SAP_MEGA_BRND_CD AS SAP_MEGA_BRND_CD,
          EMD.MEGA_BRND_DESC AS SAP_MEGA_BRND_DESC,
        --  EMD.SAP_BRND_CD AS SAP_BRND_CD,
          EMD.BRND_DESC AS SAP_BRND_DESC,
        --  EMD.SAP_VRNT_CD AS SAP_VRNT_CD,
          EMD.VARNT_DESC AS SAP_VRNT_DESC,
        --  EMD.SAP_PUT_UP_CD AS SAP_PUT_UP_CD,
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
          EGPH.UNIT_OF_MEASURE AS GPH_PROD_SIZE_UOM
          
FROM 

(Select * from edw_material_dim )EMD,
EDW_GCH_PRODUCTHIERARCHY EGPH
WHERE LTRIM(EMD.MATL_NUM,'0') = ltrim(EGPH.MATERIALNUMBER(+),0)
AND   EMD.PROD_HIER_CD <> ''
AND   LTRIM(EMD.MATL_NUM,0)  
IN (SELECT DISTINCT MATL_NUM FROM SISO) 
)

SELECT
cal.YEAR,
cal.QRTR_NO as year_quarter,
cal.MNTH_ID as month_year,
cal.MNTH_NO as month_number,
'Japan' AS COUNTRY_NAME,
TRIM(NVL(NULLIF(T4.SAP_PRNT_CUST_KEY,''),'NA')) AS distributor_id,
'NA' as distributor_id_name,
TRIM(NVL(NULLIF(T3.GPH_PROD_FRNCHSE,''),'NA')) AS FRANCHISE,
TRIM(NVL(NULLIF(T3.GPH_PROD_BRND,''),'NA')) AS BRAND,
TRIM(NVL(NULLIF(T3.GPH_PROD_SUB_BRND,''),'NA')) AS prod_sub_brand,
TRIM(NVL(NULLIF(T3.GPH_PROD_VRNT,''),'NA')) AS VARIANT,
TRIM(NVL(NULLIF(T3.GPH_PROD_SGMNT,''),'NA')) AS SEGMENT,
TRIM(NVL(NULLIF(T3.GPH_PROD_SUBSGMNT,''),'NA')) AS prod_subsegment,
TRIM(NVL(NULLIF(T3.GPH_PROD_CTGRY,''),'NA')) AS PROD_CATEGORY,
TRIM(NVL(NULLIF(T3.GPH_PROD_SUBCTGRY,''),'NA')) AS PROD_SUBCATEGORY,
TRIM(NVL(NULLIF(T3.GPH_PROD_PUT_UP_DESC,''),'NA')) AS put_up_description,
TRIM(NVL(NULLIF(T3.SAP_MATL_NUM,''),'NA')) AS SKU_CD, 
TRIM(NVL(NULLIF(T3.SAP_MAT_DESC,''),'NA')) AS SKU_DESCRIPTION,
    --TRIM(NVL(NULLIF(T3.greenlight_sku_flag,''),'NA')) AS greenlight_sku_flag,
		TRIM(NVL(NULLIF(T3.pka_product_key,''),'NA')) AS pka_product_key,
		TRIM(NVL(NULLIF(T3.pka_product_key_description,''),'NA')) AS pka_product_key_description,
		TRIM(NVL(NULLIF(T3.product_key,''),'NA')) AS product_key,
		TRIM(NVL(NULLIF(T3.product_key_description,''),'NA')) AS product_key_description,
'JPY' AS FROM_CCY,
'USD' AS TO_CCY,
T2.EXCH_RATE,
TRIM(NVL(NULLIF(T4.SAP_PRNT_CUST_KEY,''),'Not Assigned')) as SAP_PRNT_CUST_KEY,
TRIM(NVL(NULLIF(T4.SAP_PRNT_CUST_DESC,''),'Not Assigned')) as SAP_PRNT_CUST_DESC, 
TRIM(NVL(NULLIF(T4.SAP_CUST_CHNL_KEY,''),'NA')) AS SAP_CUST_CHNL_KEY,
TRIM(NVL(NULLIF(T4.SAP_CUST_CHNL_DESC,''),'NA')) AS SAP_CUST_CHNL_DESC, 
TRIM(NVL(NULLIF(T4.SAP_CUST_SUB_CHNL_KEY,''),'NA')) AS SAP_CUST_SUB_CHNL_KEY, 
TRIM(NVL(NULLIF(T4.SAP_SUB_CHNL_DESC,''),'NA')) AS SAP_SUB_CHNL_DESC, 
TRIM(NVL(NULLIF(T4.SAP_GO_TO_MDL_KEY,''),'NA')) AS SAP_GO_TO_MDL_KEY, 
TRIM(NVL(NULLIF(T4.SAP_GO_TO_MDL_DESC,''),'NA')) AS SAP_GO_TO_MDL_DESC,
TRIM(NVL(NULLIF(T4.SAP_BNR_KEY,''),'NA')) AS SAP_BNR_KEY,
TRIM(NVL(NULLIF(T4.SAP_BNR_DESC,''),'NA')) AS SAP_BNR_DESC, 
TRIM(NVL(NULLIF(T4.SAP_BNR_FRMT_KEY,''),'NA')) AS SAP_BNR_FRMT_KEY, 
TRIM(NVL(NULLIF(T4.SAP_BNR_FRMT_DESC,''),'NA')) AS SAP_BNR_FRMT_DESC, 
TRIM(NVL(NULLIF(T4.RETAIL_ENV,''),'NA')) AS RETAIL_ENV,
TRIM(NVL(NULLIF(T4.REGION,''),'NA')) AS REGION,
TRIM(NVL(NULLIF(T4.ZONE_OR_AREA,''),'NA')) AS ZONE_OR_AREA,
sum(last_3months_so) as last_3months_so_qty,
	    sum(last_6months_so) as last_6months_so_qty,
		sum(last_12months_so) as last_12months_so_qty,
	    sum(last_3months_so_value) as last_3months_so_val,
	    sum(last_6months_so_value) as last_6months_so_val,
	    sum( last_12months_so_value) as last_12months_so_val,
	    cast(sum(last_3months_so_value * T2.Exch_rate)/100 as numeric(38,5)) as last_3months_so_val_usd,
        cast(sum(last_6months_so_value* T2.Exch_rate)/100 as numeric(38,5)) as last_6months_so_val_usd,
        cast(sum(last_12months_so_value* T2.Exch_rate)/100 as numeric(38,5)) as last_12months_so_val_usd,
        propagate_flag,
        propagate_from,
		case when propagate_flag='N' then 'Not propagate' else reason end as reason,
        replicated_flag,	   
SUM(T1.sell_in_qty) AS SI_SLS_QTY,
SUM(T1.sell_in_value) AS SI_GTS_VAL,
SUM(T1.sell_in_value*T2.EXCH_RATE)/100 AS SI_GTS_VAL_USD,
SUM(T1.inv_qty) AS INVENTORY_QUANTITY,
SUM(T1.inv_value) AS INVENTORY_VAL,
SUM(T1.inv_value*T2.EXCH_RATE)/100 AS INVENTORY_VAL_USD,
SUM(T1.SO_QTY) AS SO_SLS_QTY,
SUM(T1.SO_VALue) AS SO_grs_TRD_SLS,
round(SUM(T1.SO_VALUE * T2.EXCH_RATE)/100) AS SO_grs_TRD_SLS_USD
FROM 
SISO T1,
(SELECT * FROM CURRENCY WHERE TO_CCY='USD' AND JJ_MNTH_ID=(SELECT MAX(JJ_MNTH_ID) FROM CURRENCY)) T2,
PRODUCT T3,
(SELECT * FROM REGONALCUST where rank=1) T4,cal
WHERE
T1.month=cal.MNTH_ID and 
LTRIM(T3.SAP_MATL_NUM(+),'0') = T1.MATL_NUM
AND LTRIM(T4.SAP_CUST_ID(+),'0') =T1.sap_parent_customer_key

AND left(T1.month,4) >=(date_part(year, convert_timezone('UTC', current_timestamp())) -2)
GROUP BY
CAL.YEAR,
CAL.QRTR_NO,
CAL.MNTH_ID,
CAL.MNTH_NO,
Country_name,
T4.SAP_PRNT_CUST_KEY,
T3.GPH_PROD_FRNCHSE ,
T3.GPH_PROD_BRND ,
T3.GPH_PROD_SUB_BRND ,
T3.GPH_PROD_VRNT ,
T3.GPH_PROD_SGMNT ,
T3.GPH_PROD_SUBSGMNT ,
T3.GPH_PROD_CTGRY ,
T3.GPH_PROD_SUBCTGRY ,
T3.GPH_PROD_PUT_UP_DESC ,
T3.SAP_MATL_NUM , 
T3.SAP_MAT_DESC ,
			--greenlight_sku_flag,
            pka_product_key,
            pka_product_key_description,
            product_key,
            product_key_description,
FROM_CCY,
TO_CCY,
T2.EXCH_RATE,
T4.SAP_PRNT_CUST_KEY,
T4.SAP_PRNT_CUST_DESC, 
T4.SAP_CUST_CHNL_KEY,
T4.SAP_CUST_CHNL_DESC, 
T4.SAP_CUST_SUB_CHNL_KEY, 
T4.SAP_SUB_CHNL_DESC, 
T4.SAP_GO_TO_MDL_KEY, 
T4.SAP_GO_TO_MDL_DESC,
T4.SAP_BNR_KEY,
T4.SAP_BNR_DESC, 
T4.SAP_BNR_FRMT_KEY, 
T4.SAP_BNR_FRMT_DESC, 
T4.RETAIL_ENV,
T4.REGION,
T4.ZONE_OR_AREA,
propagate_flag,
        propagate_from,
        reason,
		replicated_flag
)
)
SELECT *
        , SUM(SI_GTS_VAL) OVER ( PARTITION BY COUNTRY_NAME,YEAR,MONTH_YEAR ) AS SI_INV_DB_VAL
        , SUM(SI_GTS_VAL_USD) OVER ( PARTITION BY COUNTRY_NAME,YEAR,MONTH_YEAR ) AS SI_INV_DB_VAL_USD
FROM 
ONSESEA     
WHERE 
COUNTRY_NAME||SAP_PRNT_CUST_DESC||MONTH_YEAR IN 
(
SELECT COUNTRY_NAME||SAP_PRNT_CUST_DESC||MONTH_YEAR AS INCLUSION
FROM
(
SELECT 
COUNTRY_NAME,
SAP_PRNT_CUST_DESC,
MONTH_YEAR,
NVL(SUM(INVENTORY_VAL),0) AS INV_VAL,
NVL(SUM(SO_grs_TRD_SLS),0) as Sellout_val
FROM ONSESEA  WHERE SAP_PRNT_CUST_DESC IS NOT NULL GROUP BY 
COUNTRY_NAME,
SAP_PRNT_CUST_DESC,
MONTH_YEAR
 HAVING  INV_VAL<>0 and Sellout_val<>0
)
)
)


Select year,year_quarter,month_year,month_number,country_name,distributor_id,distributor_id_name,
franchise,brand,prod_sub_brand,variant,segment,prod_subsegment,prod_category,prod_subcategory,put_up_description,sku_cd,sku_description,
			--greenlight_sku_flag,
            pka_product_key,
            pka_product_key_description,
            product_key,
            product_key_description,
from_ccy,to_ccy,exch_rate,sap_prnt_cust_key,sap_prnt_cust_desc,sap_cust_chnl_key,sap_cust_chnl_desc,sap_cust_sub_chnl_key,sap_sub_chnl_desc,sap_go_to_mdl_key,sap_go_to_mdl_desc,sap_bnr_key,sap_bnr_desc,sap_bnr_frmt_key,sap_bnr_frmt_desc,retail_env,region,zone_or_area
,
round(cast(si_sls_qty as numeric(38,5)),5) as si_sls_qty,round(cast(si_gts_val as numeric (38,5)),5) as si_gts_val,round(cast(si_gts_val_usd as numeric(38,5)),5) as si_gts_val_usd,round(cast (inventory_quantity as numeric(38,5)),5) as inventory_quantity,round(cast(inventory_val as numeric(38,5)),5) as inventory_val, round(cast (inventory_val_usd as numeric(38,5)),5) as inventory_val_usd,
round(cast (so_sls_qty as numeric(38,5)),5) as so_sls_qty, round(cast (so_grs_trd_sls as numeric(38,5)),5) as so_grs_trd_sls,
so_grs_trd_sls_usd as so_grs_trd_sls_usd,
 last_3months_so_qty,
		last_6months_so_qty,
		last_12months_so_qty,
		last_3months_so_val,
		last_3months_so_val_usd,
		last_6months_so_val,
		last_6months_so_val_usd,
	   last_12months_so_val,
	   last_12months_so_val_usd,
        propagate_flag,
        propagate_from,
        reason
 from Regional
)
select * from final