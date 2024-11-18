{{
    config(
        materialized='view'
    )
}}

with edw_rpt_copa_customergp_agg as
(
    select * from {{ ref('aspedw_integration__edw_rpt_copa_customergp_agg') }}
),
itg_custpl_excp_ctrl as
(
    select * from {{ source('aspitg_integration','itg_custpl_excp_ctrl') }}
),
edw_material_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
edw_gch_producthierarchy as
(
    select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
),
vw_itg_custgp_prod_hierarchy as
(
    select * from {{ ref('aspitg_integration__vw_itg_custgp_prod_hierarchy') }}
),
vw_itg_custgp_customer_hierarchy as
(
    select * from {{ ref('aspitg_integration__vw_itg_custgp_customer_hierarchy') }}
),
itg_mds_custgp_portfolio_mapping as
(
    select * from {{ ref('aspitg_integration__itg_mds_custgp_portfolio_mapping') }}
),
edw_company_dim as
(
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
wks_rpt_copa_customergp_agg2 as
(
    select * from {{ ref('aspwks_integration__wks_rpt_copa_customergp_agg2') }}
),
v_edw_customer_sales_dim as
(
    select * from {{ ref('aspedw_integration__v_edw_customer_sales_dim') }}
),
edw_custgp_cogs_automation as
(
    select * from {{ ref('aspedw_integration__edw_custgp_cogs_automation') }}
),
edw_custgp_cogs_automation as
(
    select * from {{ ref('aspedw_integration__edw_custgp_cogs_automation') }}
),
itg_mds_pre_apsc_master as
(
    select * from {{ ref('aspitg_integration__itg_mds_pre_apsc_master') }}
),

final as
(
    SELECT DISTINCT 'Incomplete Regional Customer Hierarchy' AS Error_Catgeory,
       CUSTGP.FISC_YR AS Year,		--//        custgp.Fisc_yr AS Year,
       CUSTGP.FISC_YR_PER AS Period,		--//        custgp.Fisc_yr_per AS Period,
       CUSTGP.CTRY_NM AS Market,		--//        custgp.Ctry_nm AS Market,
       CUSTGP.CUST_NUM AS "Customer Code",		--//        custgp.cust_num AS "Customer Code",
       'Not applicable' AS "Material Code",
       CUSTGP.PRFT_CTR AS Profit_Centr,		--//        custgp.Prft_Ctr AS Profit_Centr,
       EXC.ERROR_DESC||' for customer number '||custgp.cust_num AS Error_desc,		--//        exc.error_desc||' for customer number '||custgp.cust_num AS Error_desc,
       EXC.CORRECTIVE_ACTION||' for customer number '||custgp.cust_num AS Corrective_Action		--//        exc.corrective_Action||' for customer number '||custgp.cust_num AS Corrective_Action
FROM edw_rpt_copa_customergp_agg custgp, ITG_CUSTPL_EXCP_CTRL exc		--// FROM rg_edw.edw_rpt_copa_customergp_agg custgp, rg_itg.itg_custpl_excp_ctrl exc
where
((case
WHEN (COALESCE(NULLIF(CUSTGP.RETAIL_ENV,''),'NA') = 'NA' OR CUSTGP.RETAIL_ENV IS NULL OR CUSTGP.RETAIL_ENV = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP.retail_env,''),'NA') = 'NA' OR CUSTGP.retail_env IS NULL OR CUSTGP.retail_env = 'Not Available')
then 'REGCHNL1'
WHEN (COALESCE(NULLIF(CUSTGP."SUB CHANNEL",''),'NA') = 'NA' OR CUSTGP."sub channel" IS NULL OR CUSTGP."sub channel" = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP."sub channel",''),'NA') = 'NA' OR CUSTGP."sub channel" IS NULL OR CUSTGP."sub channel" = 'Not Available')
then 'REGCHNL2'
WHEN (COALESCE(NULLIF(CUSTGP."GO TO MODEL",''),'NA') = 'NA' OR CUSTGP."go to model" IS NULL OR CUSTGP."go to model" = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP."go to model",''),'NA') = 'NA' OR CUSTGP."go to model" IS NULL OR CUSTGP."go to model" = 'Not Available')
then 'REGCHNL3'
WHEN (COALESCE(NULLIF(CUSTGP."PARENT CUSTOMER",''),'NA') = 'NA' OR CUSTGP."parent customer" IS NULL OR CUSTGP."parent customer" = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP."parent customer",''),'NA') = 'NA' OR CUSTGP."parent customer" IS NULL OR CUSTGP."parent customer" = 'Not Available')
then 'REGCUST1'
WHEN (COALESCE(NULLIF(CUSTGP.BANNER,''),'NA') = 'NA' OR CUSTGP.BANNER IS NULL OR CUSTGP.BANNER = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP.banner,''),'NA') = 'NA' OR CUSTGP.banner IS NULL OR CUSTGP.banner = 'Not Available')
then 'REGCUST2'
WHEN (COALESCE(NULLIF(CUSTGP."BANNER FORMAT",''),'NA') = 'NA' OR CUSTGP."banner format" IS NULL OR CUSTGP."banner format" = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP."banner format",''),'NA') = 'NA' OR CUSTGP."banner format" IS NULL OR CUSTGP."banner format" = 'Not Available')
then 'REGCUST3'
END = EXC.PRODKEY) AND CUSTGP.CTRY_NM = EXC.CTRY_NM)		--// end = EXC.PRODKEY) AND custgp.ctry_nm = exc.ctry_nm)
and (
(COALESCE(NULLIF(CUSTGP.RETAIL_ENV,''),'NA') = 'NA' OR CUSTGP.RETAIL_ENV IS NULL OR CUSTGP.RETAIL_ENV = 'Not Available') OR		--// (COALESCE(NULLIF(CUSTGP.retail_env,''),'NA') = 'NA' OR CUSTGP.retail_env IS NULL OR CUSTGP.retail_env = 'Not Available') OR
(COALESCE(NULLIF(CUSTGP."SUB CHANNEL",''),'NA') = 'NA' OR CUSTGP."sub channel" IS NULL OR CUSTGP."sub channel" = 'Not Available') OR		--// (COALESCE(NULLIF(CUSTGP."sub channel",''),'NA') = 'NA' OR CUSTGP."sub channel" IS NULL OR CUSTGP."sub channel" = 'Not Available') OR
(COALESCE(NULLIF(CUSTGP."GO TO MODEL",''),'NA') = 'NA' OR CUSTGP."go to model" IS NULL OR CUSTGP."go to model" = 'Not Available') OR		--// (COALESCE(NULLIF(CUSTGP."go to model",''),'NA') = 'NA' OR CUSTGP."go to model" IS NULL OR CUSTGP."go to model" = 'Not Available') OR
(COALESCE(NULLIF(CUSTGP."PARENT CUSTOMER",''),'NA') = 'NA' OR CUSTGP."parent customer" IS NULL  OR CUSTGP."parent customer" = 'Not Available') OR		--// (COALESCE(NULLIF(CUSTGP."parent customer",''),'NA') = 'NA' OR CUSTGP."parent customer" IS NULL  OR CUSTGP."parent customer" = 'Not Available') OR
(COALESCE(NULLIF(CUSTGP.BANNER,''),'NA') = 'NA' OR CUSTGP.BANNER IS NULL OR CUSTGP.BANNER = 'Not Available') OR		--// (COALESCE(NULLIF(CUSTGP.banner,''),'NA') = 'NA' OR CUSTGP.banner IS NULL OR CUSTGP.banner = 'Not Available') OR
(COALESCE(NULLIF(CUSTGP."BANNER FORMAT",''),'NA') = 'NA' OR CUSTGP."banner format" IS NULL OR CUSTGP."banner format" = 'Not Available'))		--// (COALESCE(NULLIF(CUSTGP."banner format",''),'NA') = 'NA' OR CUSTGP."banner format" IS NULL OR CUSTGP."banner format" = 'Not Available'))
AND (CUSTGP.CUST_NUM != '' AND CUSTGP.CUST_NUM != 'Not Available' and CUSTGP.CUST_NUM is not null)		--// and (custgp.cust_num != '' and custgp.cust_num != 'Not Available' and custgp.cust_num is not null)
UNION ALL
SELECT DISTINCT 'Missing sold-to code in regional customer hierarchy' AS Error_Catgeory,
       CUSTGP.FISC_YR AS Year,		--//        custgp.Fisc_yr AS Year,
       CUSTGP.FISC_YR_PER AS Period,		--//        custgp.Fisc_yr_per AS Period,
       CUSTGP.CTRY_NM AS Market,		--//        custgp.Ctry_nm AS Market,
       CUSTGP.CUST_NUM AS "Customer Code",		--//        custgp.cust_num AS "Customer Code",
       'Not applicable' AS "Material Code",
       CUSTGP.PRFT_CTR AS Profit_Center,		--//        custgp.Prft_Ctr AS Profit_Center,
       'Sold-to code '||CUSTGP.CUST_NUM||' missing from SAP Customer Sales Master' AS Error_desc,		--//        'Sold-to code '||custgp.cust_num||' missing from SAP Customer Sales Master' AS Error_desc,
       'PLEASE ADD AN ENTRY FOR SOLD-TO CODE '||CUSTGP.CUST_NUM||' in SAP Customer Sales Master for sls_org ' || CUSTGP.SLS_ORG AS Corrective_Action		--//        'Please add an entry for sold-to code '||custgp.cust_num||' in SAP Customer Sales Master for sls_org ' || custgp.sls_org AS Corrective_Action
FROM WKS_RPT_COPA_CUSTOMERGP_AGG2 custgp		--// FROM rg_wks.wks_rpt_copa_customergp_agg2 custgp
left join
(select sls_org,cust_num,
							 "parent customer",
							banner,
							"banner format",
							channel,
							"go to model",
							"sub channel",
							retail_env

	from
	(select sls_org,ltrim(cust_num,'0') as cust_num,
							 "parent customer",
							banner,
							"banner format",
							channel,
							"go to model",
							"sub channel",
							retail_env,
							row_number() over( partition by sls_org,cust_num
				                               order by prnt_cust_key desc) rn
	from V_EDW_CUSTOMER_SALES_DIM )where rn = 1) cust		--// 	from rg_edw.v_edw_customer_sales_dim )where rn = 1) cust
		ON CUSTGP.SLS_ORG::TEXT = CUST.SLS_ORG::TEXT		--// 		ON custgp.sls_org::TEXT = cust.sls_org::TEXT
        AND CUSTGP.CUST_NUM::TEXT = CUST.CUST_NUM		--//         AND custgp.cust_num::TEXT = cust.cust_num
WHERE (CUST.CUST_NUM = '' OR CUST.CUST_NUM IS NULL) AND (CUSTGP.CUST_NUM != '' OR CUSTGP.CUST_NUM != 'Not Available' or CUSTGP.CUST_NUM is not null)		--// where (cust.cust_num = '' or cust.cust_num is null) and (custgp.cust_num != '' or custgp.cust_num != 'Not Available' or custgp.cust_num is not null)
UNION ALL
SELECT Distinct 'Incomplete Regional Product Hierarchy' AS Error_Catgeory,
       CUSTGP.FISC_YR AS Year,		--//        Custgp.Fisc_yr AS Year,
       CUSTGP.FISC_YR_PER AS Period,		--//        custgp.Fisc_yr_per AS Period,
       CUSTGP.CTRY_NM AS Market,		--//        custgp.Ctry_nm AS Market,
       'Not applicable' AS "Customer Code",
       CUSTGP.MATL_NUM AS "Material Code",		--//        custgp.matl_num AS "Material Code",
       CUSTGP.PRFT_CTR AS Profit_Center,		--//        custgp.Prft_Ctr AS Profit_Center,
       EXC.ERROR_DESC||' for material '||custgp.matl_num AS Error_desc,		--//        exc.error_desc||' for material '||custgp.matl_num AS Error_desc,
       EXC.CORRECTIVE_ACTION||' for material '||custgp.matl_num as corrective_action		--//        exc.corrective_action||' for material '||custgp.matl_num as corrective_action
FROM EDW_RPT_COPA_CUSTOMERGP_AGG custgp, ITG_CUSTPL_EXCP_CTRL exc		--// FROM rg_edw.edw_rpt_copa_customergp_agg custgp, rg_itg.itg_custpl_excp_ctrl exc
WHERE (CUSTGP.MATL_NUM != 'REBATE' AND CUSTGP.MATL_NUM != '' AND CUSTGP.MATL_NUM IS NOT NULL AND CUSTGP.MATL_NUM != 'Not Available' )		--// WHERE (custgp.matl_num != 'REBATE' AND custgp.matl_num != '' AND custgp.matl_num IS NOT NULL AND custgp.matl_num != 'Not Available' )
AND
((case
WHEN (COALESCE(NULLIF(CUSTGP.GCPH_FRANCHISE,''),'NA') = 'NA' OR CUSTGP.GCPH_FRANCHISE IS NULL OR CUSTGP.GCPH_FRANCHISE = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP.gcph_franchise,''),'NA') = 'NA' OR CUSTGP.gcph_franchise IS NULL OR CUSTGP.gcph_franchise = 'Not Available')
then 'REGPROD1'
WHEN (COALESCE(NULLIF(CUSTGP.SAP_FRNCHSE_DESC,''),'NA') = 'NA' OR CUSTGP.SAP_FRNCHSE_DESC IS NULL OR CUSTGP.SAP_FRNCHSE_DESC = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP.sap_frnchse_desc,''),'NA') = 'NA' OR CUSTGP.sap_frnchse_desc IS NULL OR CUSTGP.sap_frnchse_desc = 'Not Available')
then 'REGPROD2'
WHEN (COALESCE(NULLIF(CUSTGP.MEGA_BRND_DESC,''),'NA') = 'NA' OR CUSTGP.MEGA_BRND_DESC IS NULL OR CUSTGP.MEGA_BRND_DESC = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP.mega_brnd_desc,''),'NA') = 'NA' OR CUSTGP.mega_brnd_desc IS NULL OR CUSTGP.mega_brnd_desc = 'Not Available')
then 'REGPROD3'
WHEN (COALESCE(NULLIF(CUSTGP.BRND_DESC,''),'NA') = 'NA' OR CUSTGP.BRND_DESC IS NULL OR CUSTGP.BRND_DESC = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP.brnd_desc,''),'NA') = 'NA' OR CUSTGP.brnd_desc IS NULL OR CUSTGP.brnd_desc = 'Not Available')
then 'REGPROD4'
WHEN (COALESCE(NULLIF(CUSTGP.PKA_SUB_BRAND_DESC,''),'NA') = 'NA' OR CUSTGP.PKA_SUB_BRAND_DESC IS NULL OR CUSTGP.PKA_SUB_BRAND_DESC = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP.pka_sub_brand_desc,''),'NA') = 'NA' OR CUSTGP.pka_sub_brand_desc IS NULL OR CUSTGP.pka_sub_brand_desc = 'Not Available')
then 'REGPROD5'
WHEN (COALESCE(NULLIF(CUSTGP.SAP_PROD_MNR_DESC,''),'NA') = 'NA' OR CUSTGP.SAP_PROD_MNR_DESC IS NULL OR CUSTGP.SAP_PROD_MNR_DESC = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP.sap_prod_mnr_desc,''),'NA') = 'NA' OR CUSTGP.sap_prod_mnr_desc IS NULL OR CUSTGP.sap_prod_mnr_desc = 'Not Available')
then 'REGPROD6'
WHEN (COALESCE(NULLIF(CUSTGP.VARNT_DESC,''),'NA') = 'NA' OR CUSTGP.VARNT_DESC IS NULL OR CUSTGP.VARNT_DESC = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP.varnt_desc,''),'NA') = 'NA' OR CUSTGP.varnt_desc IS NULL OR CUSTGP.varnt_desc = 'Not Available')
then 'REGPROD7'
WHEN (COALESCE(NULLIF(CUSTGP.PKA_SIZE_DESC,''),'NA') = 'NA' OR CUSTGP.PKA_SIZE_DESC IS NULL OR CUSTGP.PKA_SIZE_DESC = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP.pka_size_desc,''),'NA') = 'NA' OR CUSTGP.pka_size_desc IS NULL OR CUSTGP.pka_size_desc = 'Not Available')
then 'REGPROD8'
WHEN (COALESCE(NULLIF(CUSTGP.PKA_PACKAGE_DESC,''),'NA') = 'NA' OR CUSTGP.PKA_PACKAGE_DESC IS NULL OR CUSTGP.PKA_PACKAGE_DESC = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP.pka_package_desc,''),'NA') = 'NA' OR CUSTGP.pka_package_desc IS NULL OR CUSTGP.pka_package_desc = 'Not Available')
then 'REGPROD9'
WHEN (COALESCE(NULLIF(CUSTGP.PKA_PRODUCT_KEY_DESCRIPTION,''),'NA') = 'NA' OR CUSTGP.PKA_PRODUCT_KEY_DESCRIPTION IS NULL OR CUSTGP.PKA_PRODUCT_KEY_DESCRIPTION = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP.pka_product_key_description,''),'NA') = 'NA' OR CUSTGP.pka_product_key_description IS NULL OR CUSTGP.pka_product_key_description = 'Not Available')
then 'REGPROD10'
END = EXC.PRODKEY) AND CUSTGP.CTRY_NM = EXC.CTRY_NM) and		--// end = EXC.PRODKEY) AND custgp.ctry_nm = exc.ctry_nm) and
(
(COALESCE(NULLIF(CUSTGP.GCPH_FRANCHISE,''),'NA') = 'NA' OR CUSTGP.GCPH_FRANCHISE IS NULL OR CUSTGP.GCPH_FRANCHISE = 'Not Available') OR		--// (COALESCE(NULLIF(CUSTGP.gcph_franchise,''),'NA') = 'NA' OR CUSTGP.gcph_franchise IS NULL OR CUSTGP.gcph_franchise = 'Not Available') OR
(COALESCE(NULLIF(CUSTGP.SAP_FRNCHSE_DESC,''),'NA') = 'NA' OR CUSTGP.SAP_FRNCHSE_DESC IS NULL OR CUSTGP.SAP_FRNCHSE_DESC = 'Not Available') OR		--// (COALESCE(NULLIF(CUSTGP.sap_frnchse_desc,''),'NA') = 'NA' OR CUSTGP.sap_frnchse_desc IS NULL OR CUSTGP.sap_frnchse_desc = 'Not Available') OR
(COALESCE(NULLIF(CUSTGP.MEGA_BRND_DESC,''),'NA') = 'NA' OR CUSTGP.MEGA_BRND_DESC IS NULL OR CUSTGP.MEGA_BRND_DESC = 'Not Available') OR		--// (COALESCE(NULLIF(CUSTGP.mega_brnd_desc,''),'NA') = 'NA' OR CUSTGP.mega_brnd_desc IS NULL OR CUSTGP.mega_brnd_desc = 'Not Available') OR
(COALESCE(NULLIF(CUSTGP.BRND_DESC,''),'NA') = 'NA' OR CUSTGP.BRND_DESC IS NULL  OR CUSTGP.BRND_DESC = 'Not Available') OR		--// (COALESCE(NULLIF(CUSTGP.brnd_desc,''),'NA') = 'NA' OR CUSTGP.brnd_desc IS NULL  OR CUSTGP.brnd_desc = 'Not Available') OR
(COALESCE(NULLIF(CUSTGP.PKA_SUB_BRAND_DESC,''),'NA') = 'NA' OR CUSTGP.PKA_SUB_BRAND_DESC IS NULL OR CUSTGP.PKA_SUB_BRAND_DESC = 'Not Available') OR		--// (COALESCE(NULLIF(CUSTGP.pka_sub_brand_desc,''),'NA') = 'NA' OR CUSTGP.pka_sub_brand_desc IS NULL OR CUSTGP.pka_sub_brand_desc = 'Not Available') OR
(COALESCE(NULLIF(CUSTGP.SAP_PROD_MNR_DESC,''),'NA') = 'NA' OR CUSTGP.SAP_PROD_MNR_DESC IS NULL OR CUSTGP.SAP_PROD_MNR_DESC = 'Not Available') OR		--// (COALESCE(NULLIF(CUSTGP.sap_prod_mnr_desc,''),'NA') = 'NA' OR CUSTGP.sap_prod_mnr_desc IS NULL OR CUSTGP.sap_prod_mnr_desc = 'Not Available') OR
(COALESCE(NULLIF(CUSTGP.VARNT_DESC,''),'NA') = 'NA' OR CUSTGP.VARNT_DESC IS NULL OR CUSTGP.VARNT_DESC = 'Not Available') OR		--// (COALESCE(NULLIF(CUSTGP.varnt_desc,''),'NA') = 'NA' OR CUSTGP.varnt_desc IS NULL OR CUSTGP.varnt_desc = 'Not Available') OR
(COALESCE(NULLIF(CUSTGP.PKA_SIZE_DESC,''),'NA') = 'NA' OR CUSTGP.PKA_SIZE_DESC IS NULL OR CUSTGP.PKA_SIZE_DESC = 'Not Available') OR		--// (COALESCE(NULLIF(CUSTGP.pka_size_desc,''),'NA') = 'NA' OR CUSTGP.pka_size_desc IS NULL OR CUSTGP.pka_size_desc = 'Not Available') OR
(COALESCE(NULLIF(CUSTGP.PKA_PACKAGE_DESC,''),'NA') = 'NA' OR CUSTGP.PKA_PACKAGE_DESC IS NULL OR CUSTGP.PKA_PACKAGE_DESC = 'Not Available') OR		--// (COALESCE(NULLIF(CUSTGP.pka_package_desc,''),'NA') = 'NA' OR CUSTGP.pka_package_desc IS NULL OR CUSTGP.pka_package_desc = 'Not Available') OR
(COALESCE(NULLIF(CUSTGP.PKA_PRODUCT_KEY_DESCRIPTION,''),'NA') = 'NA' OR CUSTGP.PKA_PRODUCT_KEY_DESCRIPTION IS NULL OR CUSTGP.PKA_PRODUCT_KEY_DESCRIPTION = 'Not Available')		--// (COALESCE(NULLIF(CUSTGP.pka_product_key_description,''),'NA') = 'NA' OR CUSTGP.pka_product_key_description IS NULL OR CUSTGP.pka_product_key_description = 'Not Available')
)
UNION ALL
SELECT Distinct 'Missing Regional Product Hierarchy' AS Error_Catgeory,
       CUSTGP.FISC_YR AS Year,		--//        custgp.Fisc_yr AS Year,
       CUSTGP.FISC_YR_PER AS Period,		--//        custgp.Fisc_yr_per AS Period,
       CUSTGP.CTRY_NM AS Market,		--//        custgp.Ctry_nm AS Market,
       'Not applicable' AS "Customer Code",
       CUSTGP.MATL_NUM AS "Material Code",		--//        custgp.matl_num AS "Material Code",
       CUSTGP.PRFT_CTR AS Profit_Center,		--//        custgp.Prft_Ctr AS Profit_Center,
       'Missing Material '||CUSTGP.MATL_NUM AS Error_desc,		--//        'Missing Material '||custgp.matl_num AS Error_desc,
       'Material ' ||CUSTGP.MATL_NUM|| ' needs to be added in SAP material master' AS Corrective_Action		--//        'Material ' ||custgp.matl_num|| ' needs to be added in SAP material master' AS Corrective_Action
FROM EDW_RPT_COPA_CUSTOMERGP_AGG custgp		--// FROM rg_edw.edw_rpt_copa_customergp_agg custgp
LEFT JOIN EDW_MATERIAL_DIM mat on CUSTGP.MATL_NUM = ltrim(mat.matl_num,'0')		--// left join rg_edw.edw_material_dim mat on custgp.matl_num = ltrim(mat.matl_num,'0')
WHERE (CUSTGP.MATL_NUM IS NOT NULL AND CUSTGP.MATL_NUM != '' AND CUSTGP.MATL_NUM != 'Not Available')		--// WHERE (custgp.matl_num IS NOT NULL AND custgp.matl_num != '' AND custgp.matl_num != 'Not Available')
AND (MAT.MATL_NUM is null or MAT.MATL_NUM = '')		--// and (mat.matl_num is null or mat.matl_num = '')
UNION ALL
SELECT Distinct 'Missing Regional Product Hierarchy' AS Error_Catgeory,
       CUSTGP.FISC_YR AS Year,		--//        custgp.Fisc_yr AS Year,
       CUSTGP.FISC_YR_PER AS Period,		--//        custgp.Fisc_yr_per AS Period,
       CUSTGP.CTRY_NM AS Market,		--//        custgp.Ctry_nm AS Market,
       'Not applicable' AS "Customer Code",
       CUSTGP.MATL_NUM AS "Material Code",		--//        custgp.matl_num AS "Material Code",
       CUSTGP.PRFT_CTR AS Profit_Center,		--//        custgp.Prft_Ctr AS Profit_Center,
       'Missing Material '||CUSTGP.MATL_NUM AS Error_desc,		--//        'Missing Material '||custgp.matl_num AS Error_desc,
       'Material ' ||CUSTGP.MATL_NUM|| ' needs to be added in GCPH Product Hierarchy' AS Corrective_Action		--//        'Material ' ||custgp.matl_num|| ' needs to be added in GCPH Product Hierarchy' AS Corrective_Action
FROM EDW_RPT_COPA_CUSTOMERGP_AGG custgp		--// FROM rg_edw.edw_rpt_copa_customergp_agg custgp
LEFT JOIN EDW_GCH_PRODUCTHIERARCHY gch on CUSTGP.MATL_NUM = ltrim(gch.materialnumber,'0')		--// left join rg_edw.edw_gch_producthierarchy gch on custgp.matl_num = ltrim(gch.materialnumber,'0')
WHERE (CUSTGP.MATL_NUM IS NOT NULL AND CUSTGP.MATL_NUM != '' AND CUSTGP.MATL_NUM != 'Not Available')		--// WHERE (custgp.matl_num IS NOT NULL AND custgp.matl_num != '' AND custgp.matl_num != 'Not Available')
AND (GCH.MATERIALNUMBER is null or GCH.MATERIALNUMBER = '')		--// and (gch.materialnumber is null or gch.materialnumber = '')
UNION ALL
SELECT Distinct 'Incomplete Local Product Hierarchy' AS Error_Catgeory,
       CUSTGP.FISC_YR AS Year,		--//        custgp.Fisc_yr AS Year,
       CUSTGP.FISC_YR_PER AS Period,		--//        custgp.Fisc_yr_per AS Period,
       CUSTGP.CTRY_NM AS Market,		--//        custgp.Ctry_nm AS Market,
       'Not applicable' AS "Customer Code",
       CUSTGP.MATL_NUM AS "Material Code",		--//        custgp.matl_num AS "Material Code",
       CUSTGP.PRFT_CTR AS Profit_Center,		--//        custgp.Prft_Ctr AS Profit_Center,
       EXC.ERROR_DESC||' for material '||custgp.matl_num AS Error_desc,		--//        exc.error_desc||' for material '||custgp.matl_num AS Error_desc,
       EXC.CORRECTIVE_ACTION||' for material '||custgp.matl_num as corrective_action		--//        exc.corrective_action||' for material '||custgp.matl_num as corrective_action
FROM EDW_RPT_COPA_CUSTOMERGP_AGG custgp,		--// FROM rg_edw.edw_rpt_copa_customergp_agg custgp,
VW_ITG_CUSTGP_PROD_HIERARCHY locprod,
itg_custpl_excp_ctrl exc
WHERE (CUSTGP.CTRY_NM = LOCPROD.CTRY_NM AND CUSTGP.MATL_NUM = LOCPROD.MATL_NUM) and		--// WHERE (custgp.ctry_nm = locprod.ctry_nm and custgp.matl_num = locprod.matl_num) and
(CUSTGP.MATL_NUM != 'REBATE' AND CUSTGP.MATL_NUM != '' and CUSTGP.MATL_NUM != 'Not Available') and		--// (custgp.matl_num != 'REBATE' and custgp.matl_num != '' and custgp.matl_num != 'Not Available') and
((case
WHEN (COALESCE(NULLIF(CUSTGP.LOC_PROD1,''),'NA') = 'NA' OR CUSTGP.LOC_PROD1 IS NULL OR CUSTGP.LOC_PROD1 = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP.LOC_PROD1,''),'NA') = 'NA' OR CUSTGP.LOC_PROD1 IS NULL OR CUSTGP.LOC_PROD1 = 'Not Available')
then 'LOCPROD1'
WHEN (COALESCE(NULLIF(CUSTGP.LOC_PROD2,''),'NA') = 'NA' OR CUSTGP.LOC_PROD2 IS NULL OR CUSTGP.LOC_PROD2 = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP.LOC_PROD2,''),'NA') = 'NA' OR CUSTGP.LOC_PROD2 IS NULL OR CUSTGP.LOC_PROD2 = 'Not Available')
then 'LOCPROD2'
WHEN (COALESCE(NULLIF(CUSTGP.LOC_PROD3,''),'NA') = 'NA' OR CUSTGP.LOC_PROD3 IS NULL OR CUSTGP.LOC_PROD3 = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP.LOC_PROD3,''),'NA') = 'NA' OR CUSTGP.LOC_PROD3 IS NULL OR CUSTGP.LOC_PROD3 = 'Not Available')
then 'LOCPROD3'
WHEN (COALESCE(NULLIF(CUSTGP.LOC_PROD4,''),'NA') = 'NA' OR CUSTGP.LOC_PROD4 IS NULL OR CUSTGP.LOC_PROD4 = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP.LOC_PROD4,''),'NA') = 'NA' OR CUSTGP.LOC_PROD4 IS NULL OR CUSTGP.LOC_PROD4 = 'Not Available')
then 'LOCPROD4'
WHEN (COALESCE(NULLIF(CUSTGP.LOC_PROD5,''),'NA') = 'NA' OR CUSTGP.LOC_PROD5 IS NULL OR CUSTGP.LOC_PROD5 = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP.LOC_PROD5,''),'NA') = 'NA' OR CUSTGP.LOC_PROD5 IS NULL OR CUSTGP.LOC_PROD5 = 'Not Available')
then 'LOCPROD5'
WHEN (COALESCE(NULLIF(CUSTGP.LOC_PROD6,''),'NA') = 'NA' OR CUSTGP.LOC_PROD6 IS NULL OR CUSTGP.LOC_PROD6 = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP.LOC_PROD6,''),'NA') = 'NA' OR CUSTGP.LOC_PROD6 IS NULL OR CUSTGP.LOC_PROD6 = 'Not Available')
then 'LOCPROD6'
WHEN (COALESCE(NULLIF(CUSTGP.LOC_PROD7,''),'NA') = 'NA' OR CUSTGP.LOC_PROD7 IS NULL OR CUSTGP.LOC_PROD7 = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP.LOC_PROD7,''),'NA') = 'NA' OR CUSTGP.LOC_PROD7 IS NULL OR CUSTGP.LOC_PROD7 = 'Not Available')
then 'LOCPROD7'
WHEN (COALESCE(NULLIF(CUSTGP.LOC_PROD8,''),'NA') = 'NA' OR CUSTGP.LOC_PROD8 IS NULL OR CUSTGP.LOC_PROD8 = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP.LOC_PROD8,''),'NA') = 'NA' OR CUSTGP.LOC_PROD8 IS NULL OR CUSTGP.LOC_PROD8 = 'Not Available')
then 'LOCPROD8'
WHEN (COALESCE(NULLIF(CUSTGP.LOC_PROD9,''),'NA') = 'NA' OR CUSTGP.LOC_PROD9 IS NULL OR CUSTGP.LOC_PROD9 = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP.LOC_PROD9,''),'NA') = 'NA' OR CUSTGP.LOC_PROD9 IS NULL OR CUSTGP.LOC_PROD9 = 'Not Available')
then 'LOCPROD9'
WHEN (COALESCE(NULLIF(CUSTGP.LOC_PROD10,''),'NA') = 'NA' OR CUSTGP.LOC_PROD10 IS NULL OR CUSTGP.LOC_PROD10 = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP.LOC_PROD10,''),'NA') = 'NA' OR CUSTGP.LOC_PROD10 IS NULL OR CUSTGP.LOC_PROD10 = 'Not Available')
then 'LOCPROD10'
END = EXC.PRODKEY) AND CUSTGP.CTRY_NM = EXC.CTRY_NM) and		--// end = EXC.PRODKEY) AND custgp.ctry_nm = exc.ctry_nm) and
(
(COALESCE(NULLIF(CUSTGP.LOC_PROD1,''),'NA') = 'NA' OR CUSTGP.LOC_PROD1 IS NULL OR CUSTGP.LOC_PROD1 = 'Not Available') OR		--// (COALESCE(NULLIF(CUSTGP.LOC_PROD1,''),'NA') = 'NA' OR CUSTGP.LOC_PROD1 IS NULL OR CUSTGP.LOC_PROD1 = 'Not Available') OR
(COALESCE(NULLIF(CUSTGP.LOC_PROD2,''),'NA') = 'NA' OR CUSTGP.LOC_PROD2 IS NULL OR CUSTGP.LOC_PROD2 = 'Not Available') OR		--// (COALESCE(NULLIF(CUSTGP.LOC_PROD2,''),'NA') = 'NA' OR CUSTGP.LOC_PROD2 IS NULL OR CUSTGP.LOC_PROD2 = 'Not Available') OR
(COALESCE(NULLIF(CUSTGP.LOC_PROD3,''),'NA') = 'NA' OR CUSTGP.LOC_PROD3 IS NULL OR CUSTGP.LOC_PROD3 = 'Not Available') OR		--// (COALESCE(NULLIF(CUSTGP.LOC_PROD3,''),'NA') = 'NA' OR CUSTGP.LOC_PROD3 IS NULL OR CUSTGP.LOC_PROD3 = 'Not Available') OR
(COALESCE(NULLIF(CUSTGP.LOC_PROD4,''),'NA') = 'NA' OR CUSTGP.LOC_PROD4 IS NULL  OR CUSTGP.LOC_PROD4 = 'Not Available') OR		--// (COALESCE(NULLIF(CUSTGP.LOC_PROD4,''),'NA') = 'NA' OR CUSTGP.LOC_PROD4 IS NULL  OR CUSTGP.LOC_PROD4 = 'Not Available') OR
(COALESCE(NULLIF(CUSTGP.LOC_PROD5,''),'NA') = 'NA' OR CUSTGP.LOC_PROD5 IS NULL OR CUSTGP.LOC_PROD5 = 'Not Available') OR		--// (COALESCE(NULLIF(CUSTGP.LOC_PROD5,''),'NA') = 'NA' OR CUSTGP.LOC_PROD5 IS NULL OR CUSTGP.LOC_PROD5 = 'Not Available') OR
(COALESCE(NULLIF(CUSTGP.LOC_PROD6,''),'NA') = 'NA' OR CUSTGP.LOC_PROD6 IS NULL OR CUSTGP.LOC_PROD6 = 'Not Available') OR		--// (COALESCE(NULLIF(CUSTGP.LOC_PROD6,''),'NA') = 'NA' OR CUSTGP.LOC_PROD6 IS NULL OR CUSTGP.LOC_PROD6 = 'Not Available') OR
(COALESCE(NULLIF(CUSTGP.LOC_PROD7,''),'NA') = 'NA' OR CUSTGP.LOC_PROD7 IS NULL OR CUSTGP.LOC_PROD7 = 'Not Available') OR		--// (COALESCE(NULLIF(CUSTGP.LOC_PROD7,''),'NA') = 'NA' OR CUSTGP.LOC_PROD7 IS NULL OR CUSTGP.LOC_PROD7 = 'Not Available') OR
(COALESCE(NULLIF(CUSTGP.LOC_PROD8,''),'NA') = 'NA' OR CUSTGP.LOC_PROD8 IS NULL OR CUSTGP.LOC_PROD8 = 'Not Available') OR		--// (COALESCE(NULLIF(CUSTGP.LOC_PROD8,''),'NA') = 'NA' OR CUSTGP.LOC_PROD8 IS NULL OR CUSTGP.LOC_PROD8 = 'Not Available') OR
(COALESCE(NULLIF(CUSTGP.LOC_PROD9,''),'NA') = 'NA' OR CUSTGP.LOC_PROD9 IS NULL OR CUSTGP.LOC_PROD9 = 'Not Available') OR		--// (COALESCE(NULLIF(CUSTGP.LOC_PROD9,''),'NA') = 'NA' OR CUSTGP.LOC_PROD9 IS NULL OR CUSTGP.LOC_PROD9 = 'Not Available') OR
(COALESCE(NULLIF(CUSTGP.LOC_PROD10,''),'NA') = 'NA' OR CUSTGP.LOC_PROD10 IS NULL OR CUSTGP.LOC_PROD10 = 'Not Available')		--// (COALESCE(NULLIF(CUSTGP.LOC_PROD10,''),'NA') = 'NA' OR CUSTGP.LOC_PROD10 IS NULL OR CUSTGP.LOC_PROD10 = 'Not Available')
)
UNION ALL
SELECT Distinct 'Missing Local Product Hierarchy' AS Error_Catgeory,
       CUSTGP.FISC_YR AS Year,		--//        custgp.Fisc_yr AS Year,
       CUSTGP.FISC_YR_PER AS Period,		--//        custgp.Fisc_yr_per AS Period,
       CUSTGP.CTRY_NM AS Market,		--//        custgp.Ctry_nm AS Market,
       'Not applicable' AS "Customer Code",
       CUSTGP.MATL_NUM AS "Material Code",		--//        custgp.matl_num AS "Material Code",
       CUSTGP.PRFT_CTR AS Profit_Center,		--//        custgp.Prft_Ctr AS Profit_Center,
       EXC.ERROR_DESC||' for '||custgp.matl_num AS Error_desc,		--//        exc.error_desc||' for '||custgp.matl_num AS Error_desc,
       EXC.CORRECTIVE_ACTION ||' for '||custgp.matl_num AS Corrective_Action		--//        exc.corrective_action ||' for '||custgp.matl_num AS Corrective_Action
FROM EDW_RPT_COPA_CUSTOMERGP_AGG custgp		--// FROM rg_edw.edw_rpt_copa_customergp_agg custgp
JOIN ITG_CUSTPL_EXCP_CTRL EXC ON CUSTGP.CTRY_NM = EXC.CTRY_NM and 'MISMATL' = EXC.PRODKEY		--// join rg_itg.itg_custpl_excp_ctrl exc on custgp.ctry_nm = exc.ctry_nm and 'MISMATL' = EXC.PRODKEY
left join VW_ITG_CUSTGP_PROD_HIERARCHY locprod on		--// left join rg_itg.vw_itg_custgp_prod_hierarchy locprod on
(CUSTGP.CTRY_NM = LOCPROD.CTRY_NM AND CUSTGP.CUST_NUM = LOCPROD.MATL_NUM)		--// (custgp.ctry_nm = locprod.ctry_nm and custgp.cust_num = locprod.matl_num)
WHERE (CUSTGP.MATL_NUM != 'NOT AVAILABLE' AND CUSTGP.MATL_NUM != '' AND CUSTGP.MATL_NUM IS NOT NULL) AND (LOCPROD.MATL_NUM = '' OR LOCPROD.MATL_NUM is null or LOCPROD.MATL_NUM = 'Not Available')		--// where (custgp.matl_num != 'Not Available' and custgp.matl_num != '' and custgp.matl_num is not null) and (locprod.matl_num = '' or locprod.matl_num is null or locprod.matl_num = 'Not Available')
UNION ALL
SELECT distinct 'Incomplete Local Customer Hierarchy' AS Error_Catgeory,
       CUSTGP.FISC_YR AS Year,		--//        custgp.Fisc_yr AS Year,
       CUSTGP.FISC_YR_PER AS Period,		--//        custgp.Fisc_yr_per AS Period,
       CUSTGP.CTRY_NM AS Market,		--//        custgp.Ctry_nm AS Market,
       CUSTGP.CUST_NUM AS "Customer Code",		--//        custgp.cust_num AS "Customer Code",
       'Not applicable' AS "Material Code",
       CUSTGP.PRFT_CTR AS Profit_Center,		--//        custgp.Prft_Ctr AS Profit_Center,
       EXC.ERROR_DESC||' for customer number '||custgp.cust_num AS Error_desc,		--//        exc.error_desc||' for customer number '||custgp.cust_num AS Error_desc,
       EXC.CORRECTIVE_ACTION||' for customer number '||custgp.cust_num AS Corrective_Action		--//        exc.corrective_Action||' for customer number '||custgp.cust_num AS Corrective_Action
FROM EDW_RPT_COPA_CUSTOMERGP_AGG custgp, ITG_CUSTPL_EXCP_CTRL exc,vw_itg_custgp_customer_hierarchy loccust		--// FROM rg_edw.edw_rpt_copa_customergp_agg custgp, rg_itg.itg_custpl_excp_ctrl exc,rg_itg.vw_itg_custgp_customer_hierarchy loccust
WHERE (CUSTGP.CUST_NUM != 'NOT AVAILABLE' AND CUSTGP.CUST_NUM != '' and CUSTGP.CUST_NUM is not null) and		--// WHERE (custgp.cust_num != 'Not Available' and custgp.cust_num != '' and custgp.cust_num is not null) and
(CUSTGP.CTRY_NM = LOCCUST.CTRY_NM AND CUSTGP.CUST_NUM = LOCCUST.CUST_NUM) and		--// (custgp.ctry_nm = loccust.ctry_nm and custgp.cust_num = loccust.cust_num) and
((case
WHEN (COALESCE(NULLIF(CUSTGP.LOC_CHANNEL1,''),'NA') = 'NA' OR CUSTGP.LOC_CHANNEL1 IS NULL OR CUSTGP.LOC_CHANNEL1 = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP.LOC_CHANNEL1,''),'NA') = 'NA' OR CUSTGP.LOC_CHANNEL1 IS NULL OR CUSTGP.LOC_CHANNEL1 = 'Not Available')
then 'LOCCHNL1'
WHEN (COALESCE(NULLIF(CUSTGP.LOC_CHANNEL2,''),'NA') = 'NA' OR CUSTGP.LOC_CHANNEL2 IS NULL OR CUSTGP.LOC_CHANNEL2 = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP.LOC_CHANNEL2,''),'NA') = 'NA' OR CUSTGP.LOC_CHANNEL2 IS NULL OR CUSTGP.LOC_CHANNEL2 = 'Not Available')
then 'LOCCHNL2'
WHEN (COALESCE(NULLIF(CUSTGP.LOC_CHANNEL3,''),'NA') = 'NA' OR CUSTGP.LOC_CHANNEL3 IS NULL OR CUSTGP.LOC_CHANNEL3 = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP.LOC_CHANNEL3,''),'NA') = 'NA' OR CUSTGP.LOC_CHANNEL3 IS NULL OR CUSTGP.LOC_CHANNEL3 = 'Not Available')
then 'LOCCHNL3'
WHEN (COALESCE(NULLIF(CUSTGP.LOC_CUST1,''),'NA') = 'NA' OR CUSTGP.LOC_CUST1 IS NULL OR CUSTGP.LOC_CUST1 = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP.LOC_CUST1,''),'NA') = 'NA' OR CUSTGP.LOC_CUST1 IS NULL OR CUSTGP.LOC_CUST1 = 'Not Available')
then 'LOCCUST1'
WHEN (COALESCE(NULLIF(CUSTGP.LOC_CUST2,''),'NA') = 'NA' OR CUSTGP.LOC_CUST2 IS NULL OR CUSTGP.LOC_CUST2 = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP.LOC_CUST2,''),'NA') = 'NA' OR CUSTGP.LOC_CUST2 IS NULL OR CUSTGP.LOC_CUST2 = 'Not Available')
then 'LOCCUST2'
WHEN (COALESCE(NULLIF(CUSTGP.LOC_CUST3,''),'NA') = 'NA' OR CUSTGP.LOC_CUST3 IS NULL OR CUSTGP.LOC_CUST3 = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP.LOC_CUST3,''),'NA') = 'NA' OR CUSTGP.LOC_CUST3 IS NULL OR CUSTGP.LOC_CUST3 = 'Not Available')
then 'LOCCUST3'
WHEN (COALESCE(NULLIF(CUSTGP.LOCAL_CUST_SEGMENTATION,''),'NA') = 'NA' OR CUSTGP.LOCAL_CUST_SEGMENTATION IS NULL OR CUSTGP.LOCAL_CUST_SEGMENTATION = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP.local_cust_segmentation,''),'NA') = 'NA' OR CUSTGP.local_cust_segmentation IS NULL OR CUSTGP.local_cust_segmentation = 'Not Available')
then 'LOCSEG1'
WHEN (COALESCE(NULLIF(CUSTGP.LOCAL_CUST_SEGMENTATION_2,''),'NA') = 'NA' OR CUSTGP.LOCAL_CUST_SEGMENTATION_2 IS NULL OR CUSTGP.LOCAL_CUST_SEGMENTATION_2 = 'Not Available')		--// when (COALESCE(NULLIF(CUSTGP.local_cust_segmentation_2,''),'NA') = 'NA' OR CUSTGP.local_cust_segmentation_2 IS NULL OR CUSTGP.local_cust_segmentation_2 = 'Not Available')
then 'LOCSEG2'
END = EXC.PRODKEY) AND  CUSTGP.CTRY_NM = EXC.CTRY_NM) and		--// end = EXC.PRODKEY) AND  custgp.ctry_nm = exc.ctry_nm) and
(
(COALESCE(NULLIF(CUSTGP.LOC_CHANNEL1,''),'NA') = 'NA' OR CUSTGP.LOC_CHANNEL1 IS NULL OR CUSTGP.LOC_CHANNEL1 = 'Not Available') OR		--// (COALESCE(NULLIF(CUSTGP.LOC_CHANNEL1,''),'NA') = 'NA' OR CUSTGP.LOC_CHANNEL1 IS NULL OR CUSTGP.LOC_CHANNEL1 = 'Not Available') OR
(COALESCE(NULLIF(CUSTGP.LOC_CHANNEL2,''),'NA') = 'NA' OR CUSTGP.LOC_CHANNEL2 IS NULL OR CUSTGP.LOC_CHANNEL2 = 'Not Available') OR		--// (COALESCE(NULLIF(CUSTGP.LOC_CHANNEL2,''),'NA') = 'NA' OR CUSTGP.LOC_CHANNEL2 IS NULL OR CUSTGP.LOC_CHANNEL2 = 'Not Available') OR
(COALESCE(NULLIF(CUSTGP.LOC_CHANNEL3,''),'NA') = 'NA' OR CUSTGP.LOC_CHANNEL3 IS NULL OR CUSTGP.LOC_CHANNEL3 = 'Not Available') OR		--// (COALESCE(NULLIF(CUSTGP.LOC_CHANNEL3,''),'NA') = 'NA' OR CUSTGP.LOC_CHANNEL3 IS NULL OR CUSTGP.LOC_CHANNEL3 = 'Not Available') OR
(COALESCE(NULLIF(CUSTGP.LOC_CUST1,''),'NA') = 'NA' OR CUSTGP.LOC_CUST1 IS NULL  OR CUSTGP.LOC_CUST1 = 'Not Available') OR		--// (COALESCE(NULLIF(CUSTGP.LOC_CUST1,''),'NA') = 'NA' OR CUSTGP.LOC_CUST1 IS NULL  OR CUSTGP.LOC_CUST1 = 'Not Available') OR
(COALESCE(NULLIF(CUSTGP.LOC_CUST2,''),'NA') = 'NA' OR CUSTGP.LOC_CUST2 IS NULL OR CUSTGP.LOC_CUST2 = 'Not Available') OR		--// (COALESCE(NULLIF(CUSTGP.LOC_CUST2,''),'NA') = 'NA' OR CUSTGP.LOC_CUST2 IS NULL OR CUSTGP.LOC_CUST2 = 'Not Available') OR
(COALESCE(NULLIF(CUSTGP.LOC_CUST3,''),'NA') = 'NA' OR CUSTGP.LOC_CUST3 IS NULL OR CUSTGP.LOC_CUST3 = 'Not Available') OR		--// (COALESCE(NULLIF(CUSTGP.LOC_CUST3,''),'NA') = 'NA' OR CUSTGP.LOC_CUST3 IS NULL OR CUSTGP.LOC_CUST3 = 'Not Available') OR
(COALESCE(NULLIF(CUSTGP.LOCAL_CUST_SEGMENTATION,''),'NA') = 'NA' OR CUSTGP.LOCAL_CUST_SEGMENTATION IS NULL OR CUSTGP.LOCAL_CUST_SEGMENTATION = 'Not Available') OR		--// (COALESCE(NULLIF(CUSTGP.local_cust_segmentation,''),'NA') = 'NA' OR CUSTGP.local_cust_segmentation IS NULL OR CUSTGP.local_cust_segmentation = 'Not Available') OR
(COALESCE(NULLIF(CUSTGP.LOCAL_CUST_SEGMENTATION_2,''),'NA') = 'NA' OR CUSTGP.LOCAL_CUST_SEGMENTATION_2 IS NULL OR CUSTGP.LOCAL_CUST_SEGMENTATION_2 = 'Not Available')		--// (COALESCE(NULLIF(CUSTGP.local_cust_segmentation_2,''),'NA') = 'NA' OR CUSTGP.local_cust_segmentation_2 IS NULL OR CUSTGP.local_cust_segmentation_2 = 'Not Available')
)
UNION ALL
SELECT DISTINCT 'Missing sold-to code in local customer hierarchy' AS Error_Catgeory,
       CUSTGP.FISC_YR AS Year,		--//        custgp.Fisc_yr AS Year,
       CUSTGP.FISC_YR_PER AS Period,		--//        custgp.Fisc_yr_per AS Period,
       CUSTGP.CTRY_NM AS Market,		--//        custgp.Ctry_nm AS Market,
       CUSTGP.CUST_NUM AS "Customer Code",		--//        custgp.cust_num AS "Customer Code",
       'Not applicable' AS "Material Code",
       CUSTGP.PRFT_CTR AS Profit_Center,		--//        custgp.Prft_Ctr AS Profit_Center,
       EXC.ERROR_DESC||' '||custgp.cust_num AS Error_desc,		--//        exc.error_desc||' '||custgp.cust_num AS Error_desc,
       EXC.CORRECTIVE_ACTION ||' for '||custgp.cust_num AS Corrective_Action		--//        exc.corrective_action ||' for '||custgp.cust_num AS Corrective_Action
FROM EDW_RPT_COPA_CUSTOMERGP_AGG custgp		--// FROM rg_edw.edw_rpt_copa_customergp_agg custgp
JOIN ITG_CUSTPL_EXCP_CTRL EXC ON CUSTGP.CTRY_NM = EXC.CTRY_NM and 'MISSOLDTO' = EXC.PRODKEY		--// join rg_itg.itg_custpl_excp_ctrl exc on custgp.ctry_nm = exc.ctry_nm and 'MISSOLDTO' = EXC.PRODKEY
left join VW_ITG_CUSTGP_CUSTOMER_HIERARCHY loccust on		--// left join rg_itg.vw_itg_custgp_customer_hierarchy loccust on
(CUSTGP.CTRY_NM = LOCCUST.CTRY_NM AND CUSTGP.CUST_NUM = LOCCUST.CUST_NUM)		--// (custgp.ctry_nm = loccust.ctry_nm and custgp.cust_num = loccust.cust_num)
WHERE (CUSTGP.CUST_NUM != 'NOT AVAILABLE' AND CUSTGP.CUST_NUM != '' and CUSTGP.CUST_NUM is not null) and		--// where (custgp.cust_num != 'Not Available' and custgp.cust_num != '' and custgp.cust_num is not null) and
 (LOCCUST.CUST_NUM = '' OR LOCCUST.CUST_NUM is null or LOCCUST.CUST_NUM = 'Not Available')		--//  (loccust.cust_num = '' or loccust.cust_num is null or loccust.cust_num = 'Not Available')
UNION ALL
SELECT distinct 'Incomplete Regional Product Portfolio' AS Error_Catgeory,
       CUSTGP.FISC_YR AS Year,		--//        custgp.Fisc_yr AS Year,
       CUSTGP.FISC_YR_PER AS Period,		--//        custgp.Fisc_yr_per AS Period,
       CUSTGP.CTRY_NM AS Market,		--//        custgp.Ctry_nm AS Market,
       'Not applicable' AS "Customer Code",
       'Not applicable' AS "Material Code",
       CUSTGP.PRFT_CTR AS Profit_Center,		--//        custgp.Prft_Ctr AS Profit_Center,
       EXC.ERROR_DESC||' for '||custgp.Prft_ctr AS Error_desc,		--//        exc.error_desc||' for '||custgp.Prft_ctr AS Error_desc,
       EXC.CORRECTIVE_ACTION ||' for '||custgp.Prft_ctr AS Corrective_Action		--//        exc.corrective_action ||' for '||custgp.Prft_ctr AS Corrective_Action
FROM EDW_RPT_COPA_CUSTOMERGP_AGG custgp,itg_custpl_excp_ctrl exc,itg_mds_custgp_portfolio_mapping portf		--// FROM rg_edw.edw_rpt_copa_customergp_agg custgp,rg_itg.itg_custpl_excp_ctrl exc,rg_itg.itg_mds_custgp_portfolio_mapping portf
WHERE (CUSTGP.CTRY_NM = EXC.CTRY_NM and 'REGPRODSEG' = EXC.PRODKEY) and		--// where (custgp.ctry_nm = exc.ctry_nm and 'REGPRODSEG' = EXC.PRODKEY) and
(CUSTGP.PRFT_CTR != 'NOT AVAILABLE' AND CUSTGP.PRFT_CTR != '' and CUSTGP.PRFT_CTR is not null) and		--// (custgp.prft_ctr != 'Not Available' and custgp.prft_ctr != '' and custgp.prft_ctr is not null) and
(CUSTGP.CTRY_NM = PORTF.MARKET AND CUSTGP.PRFT_CTR = PORTF.PRFT_CTR)		--// (custgp.ctry_nm = portf.market and custgp.prft_ctr = portf.prft_ctr)
UNION ALL
SELECT distinct 'Incomplete Market Product Portfolio' AS Error_Catgeory,
       CUSTGP.FISC_YR AS Year,		--//        custgp.Fisc_yr AS Year,
       CUSTGP.FISC_YR_PER AS Period,		--//        custgp.Fisc_yr_per AS Period,
       CUSTGP.CTRY_NM AS Market,		--//        custgp.Ctry_nm AS Market,
       'Not applicable' AS "Customer Code",
       'Not applicable' AS "Material Code",
       CUSTGP.PRFT_CTR AS Profit_Center,		--//        custgp.Prft_Ctr AS Profit_Center,
       EXC.ERROR_DESC||' for '||custgp.Prft_ctr AS Error_desc,		--//        exc.error_desc||' for '||custgp.Prft_ctr AS Error_desc,
       EXC.CORRECTIVE_ACTION ||' for '||custgp.Prft_ctr AS Corrective_Action		--//        exc.corrective_action ||' for '||custgp.Prft_ctr AS Corrective_Action
FROM EDW_RPT_COPA_CUSTOMERGP_AGG custgp,itg_custpl_excp_ctrl exc,itg_mds_custgp_portfolio_mapping portf		--// FROM rg_edw.edw_rpt_copa_customergp_agg custgp,rg_itg.itg_custpl_excp_ctrl exc,rg_itg.itg_mds_custgp_portfolio_mapping portf

WHERE (CUSTGP.CTRY_NM = EXC.CTRY_NM and 'LOCPRODSEG' = EXC.PRODKEY) and		--// where (custgp.ctry_nm = exc.ctry_nm and 'LOCPRODSEG' = EXC.PRODKEY) and
(CUSTGP.PRFT_CTR != 'NOT AVAILABLE' AND CUSTGP.PRFT_CTR != '' and CUSTGP.PRFT_CTR is not null) and		--// (custgp.prft_ctr != 'Not Available' and custgp.prft_ctr != '' and custgp.prft_ctr is not null) and
(CUSTGP.CTRY_NM = PORTF.MARKET AND CUSTGP.PRFT_CTR = PORTF.PRFT_CTR)		--// (custgp.ctry_nm = portf.market and custgp.prft_ctr = portf.prft_ctr)
UNION ALL
SELECT 'Missing Product Portfolio' AS Error_Catgeory,
       CUSTGP.FISC_YR AS Year,		--//        custgp.Fisc_yr AS Year,
       CUSTGP.FISC_YR_PER AS Period,		--//        custgp.Fisc_yr_per AS Period,
       CUSTGP.CTRY_NM AS Market,		--//        custgp.Ctry_nm AS Market,
       'Not applicable' AS "Customer Code",
       'Not applicable' AS "Material Code",
       CUSTGP.PRFT_CTR AS Profit_Center,		--//        custgp.Prft_Ctr AS Profit_Center,
       EXC.ERROR_DESC||' for '||custgp.Prft_ctr AS Error_desc,		--//        exc.error_desc||' for '||custgp.Prft_ctr AS Error_desc,
       EXC.CORRECTIVE_ACTION ||' for '||custgp.Prft_ctr AS Corrective_Action		--//        exc.corrective_action ||' for '||custgp.Prft_ctr AS Corrective_Action
FROM EDW_RPT_COPA_CUSTOMERGP_AGG custgp		--// FROM rg_edw.edw_rpt_copa_customergp_agg custgp
JOIN ITG_CUSTPL_EXCP_CTRL EXC ON (CUSTGP.CTRY_NM = EXC.CTRY_NM and 'MISPRODSEG' = EXC.PRODKEY)		--// join rg_itg.itg_custpl_excp_ctrl exc on (custgp.ctry_nm = exc.ctry_nm and 'MISPRODSEG' = EXC.PRODKEY)
LEFT JOIN ITG_MDS_CUSTGP_PORTFOLIO_MAPPING PORTF ON (CUSTGP.CTRY_NM = PORTF.MARKET AND CUSTGP.PRFT_CTR = PORTF.PRFT_CTR)		--// left join rg_itg.itg_mds_custgp_portfolio_mapping portf on (custgp.ctry_nm = portf.market and custgp.prft_ctr = portf.prft_ctr)
WHERE (CUSTGP.PRFT_CTR != 'NOT AVAILABLE' AND CUSTGP.PRFT_CTR != '' and CUSTGP.PRFT_CTR is not null) and		--// where (custgp.prft_ctr != 'Not Available' and custgp.prft_ctr != '' and custgp.prft_ctr is not null) and
(portf.prft_ctr = '' or PORTF.PRFT_CTR is null)		--// (portf.prft_ctr = '' or portf.prft_ctr is null)
UNION ALL
SELECT distinct 'Missing Pre-APSC Cost' AS Error_Catgeory,
       CUSTGP.FISC_YR AS Year,		--//        custgp.Fisc_yr AS Year,
       CUSTGP.PERIOD AS Period,		--//        custgp.Period AS Period,
       CUSTGP.CTRY_NM AS Market,		--//        custgp.Ctry_nm AS Market,
       'Not applicable' AS "Customer Code",
       CUSTGP.MATL_NUM AS "Material Code",		--//        custgp.matl_num AS "Material Code",
       CUSTGP.PROFIT_CNTR AS Profit_Center,		--//        custgp.profit_cntr AS Profit_Center,
       'MISSING PRE-APSC COST'||' FOR '||CUSTGP.MATL_NUM || ' for market '||CUSTGP.CTRY_NM AS Error_desc,		--//        'Missing Pre-APSC Cost'||' for '||custgp.matl_num || ' for market '||custgp.ctry_nm AS Error_desc,
       'PLEASE ADD AN ENTRY FOR MATERIAL ' ||CUSTGP.MATL_NUM || ' for market '|| CUSTGP.CTRY_NM ||' in MDS table AP_PRE_APSC_MASTER'		--//        'Please add an entry for material ' ||custgp.matl_num || ' for market '|| custgp.ctry_nm ||' in MDS table AP_PRE_APSC_MASTER'
	   AS Corrective_Action
FROM
(
SELECT
calccogs.fisc_yr,
calccogs.period,
calccogs.ctry_group as ctry_nm,
calccogs.matl_num,
profit_cntr
FROM
(SELECT
fisc_yr,
period,
cmp.ctry_group as ctry_group,
matl_num,
profit_cntr
RG_EDW.EDW_CUSTGP_COGS_AUTOMATION cogs		--// FROM rg_edw.edw_custgp_cogs_automation cogs
LEFT JOIN EDW_COMPANY_DIM CMP ON COGS.CO_CD = CMP.CO_CD		--// LEFT JOIN rg_edw.edw_company_dim cmp ON cogs.co_cd = cmp.co_cd
where pre_apsc_cper_pc is null
and nts_volume != 0
and (matl_num is not null and matl_num != ''))calccogs
left join ( select market_code,materialnumber,materialdescription,valid_from,
                nvl(valid_to,'2099012') as valid_to, pre_apsc_per_pc as pre_apsc_cper_pc
         from
        (
          select market_code,materialnumber,materialdescription, year_code||'0'||lpad(month_code,2,'0') as valid_from,
                 lead(year_code||'0'||lpad(month_code,2,'0'),1) over (partition by market_code, materialnumber order by year_code||'0'||lpad(month_code,2,'0') ) as valid_to,
                  pre_apsc_per_pc
            from RG_ITG.ITG_MDS_PRE_APSC_MASTER		--//             from rg_itg.itg_mds_pre_apsc_master
          --where -- materialnumber = '6127076' and
            --market_code ='Korea'
         )  base

        order by market_code, materialnumber, valid_from
       ) PREAPSC ON PREAPSC.MATERIALNUMBER = CALCCOGS.MATL_NUM		--//        ) Preapsc ON preapsc.materialnumber = calccogs.matl_num
        AND PREAPSC.MARKET_CODE = CALCCOGS.CTRY_GROUP		--//         AND preapsc.market_code = calccogs.ctry_group
        AND CALCCOGS.PERIOD >= PREAPSC.VALID_FROM		--//         AND calccogs.period >= preapsc.valid_from
        AND CALCCOGS.PERIOD < PREAPSC.VALID_TO		--//         AND calccogs.period < preapsc.valid_to
WHERE (PREAPSC.MATERIALNUMBER is null or PREAPSC.MATERIALNUMBER = ' '))custgp		--// where (preapsc.materialnumber is null or preapsc.materialnumber = ' '))custgp
UNION ALL
SELECT distinct 'Missing Pre-APSC Cost' AS Error_Catgeory,
       CUSTGP.FISC_YR AS Year,		--//        custgp.Fisc_yr AS Year,
       CUSTGP.PERIOD AS Period,		--//        custgp.period AS Period,
       CUSTGP.CTRY_NM AS Market,		--//        custgp.Ctry_nm AS Market,
       'Not applicable' AS "Customer Code",
       CUSTGP.MATL_NUM AS "Material Code",		--//        custgp.matl_num AS "Material Code",
       CUSTGP.PROFIT_CNTR AS Profit_Center,		--//        custgp.profit_cntr AS Profit_Center,
       'MISSING PRE-APSC COST'||' FOR '||CUSTGP.MATL_NUM || ' for market '||CUSTGP.CTRY_NM AS Error_desc,		--//        'Missing Pre-APSC Cost'||' for '||custgp.matl_num || ' for market '||custgp.ctry_nm AS Error_desc,
       'PLEASE UPDATE PRE-APSC COST FOR MATERIAL ' ||CUSTGP.MATL_NUM || ' for market '|| CUSTGP.CTRY_NM ||' in MDS table AP_PRE_APSC_MASTER'		--//        'Please update pre-apsc cost for material ' ||custgp.matl_num || ' for market '|| custgp.ctry_nm ||' in MDS table AP_PRE_APSC_MASTER'
	   AS Corrective_Action
FROM
(
SELECT
calccogs.fisc_yr,
calccogs.period,
calccogs.ctry_group as ctry_nm,
calccogs.matl_num,
profit_cntr
FROM
(SELECT
fisc_yr,
period,
cmp.ctry_group as ctry_group,
matl_num,profit_cntr
FROM EDW_CUSTGP_COGS_AUTOMATION cogs		--// FROM rg_edw.edw_custgp_cogs_automation cogs
LEFT JOIN EDW_COMPANY_DIM CMP ON COGS.CO_CD = CMP.CO_CD		--// LEFT JOIN rg_edw.edw_company_dim cmp ON cogs.co_cd = cmp.co_cd
where pre_apsc_cper_pc is null
and nts_volume != 0
and (matl_num is not null and matl_num != ''))calccogs
left join ( select market_code,materialnumber,materialdescription,valid_from,
                nvl(valid_to,'2099012') as valid_to, pre_apsc_per_pc as pre_apsc_cper_pc
         from
        (
          select market_code,materialnumber,materialdescription, year_code||'0'||lpad(month_code,2,'0') as valid_from,
                 lead(year_code||'0'||lpad(month_code,2,'0'),1) over (partition by market_code, materialnumber order by year_code||'0'||lpad(month_code,2,'0') ) as valid_to,
                  pre_apsc_per_pc
            from RG_ITG.ITG_MDS_PRE_APSC_MASTER		--//             from rg_itg.itg_mds_pre_apsc_master
          --where -- materialnumber = '6127076' and
            --market_code ='Korea'
         )  base

        order by market_code, materialnumber, valid_from
       ) PREAPSC ON PREAPSC.MATERIALNUMBER = CALCCOGS.MATL_NUM		--//        ) Preapsc ON preapsc.materialnumber = calccogs.matl_num
        AND PREAPSC.MARKET_CODE = CALCCOGS.CTRY_GROUP		--//         AND preapsc.market_code = calccogs.ctry_group
        AND CALCCOGS.PERIOD >= PREAPSC.VALID_FROM		--//         AND calccogs.period >= preapsc.valid_from
        AND CALCCOGS.PERIOD < PREAPSC.VALID_TO		--//         AND calccogs.period < preapsc.valid_to
WHERE (PREAPSC.MATERIALNUMBER is not null or PREAPSC.MATERIALNUMBER != ' ') and (pre_apsc_cper_pc is null or  pre_apsc_cper_pc = 0))custgp		--// where (preapsc.materialnumber is not null or preapsc.materialnumber != ' ') and (pre_apsc_cper_pc is null or  pre_apsc_cper_pc = 0))custgp
UNION ALL
select distinct 'Unassigned Customer Number in COPA' AS Error_Catgeory,
       CUSTGP.FISC_YR AS Year,		--//        custgp.Fisc_yr AS Year,
       CUSTGP.FISC_YR_PER AS Period,		--//        custgp.Fisc_yr_per AS Period,
       CUSTGP.CTRY_NM AS Market,		--//        custgp.Ctry_nm AS Market,
       'Not applicable' AS "Customer Code",
       CUSTGP.MATL_NUM AS "Material Code",		--//        custgp.matl_num AS "Material Code",
       CUSTGP.PRFT_CTR AS Profit_Center,		--//        custgp.prft_ctr AS Profit_Center,
       'Blank Customer Number in BW COPA Extract' as error_desc,
       'Please update correct customer number in BW COPA extract to DNA' AS Corrective_Action
FROM EDW_RPT_COPA_CUSTOMERGP_AGG custgp		--// FROM rg_edw.edw_rpt_copa_customergp_agg custgp
where cust_num is null or cust_num = '' or cust_num = 'Not Available'
UNION ALL
select distinct 'Unassigned Material Number in COPA' AS Error_Catgeory,
       CUSTGP.FISC_YR AS Year,		--//        custgp.Fisc_yr AS Year,
       CUSTGP.FISC_YR_PER AS Period,		--//        custgp.Fisc_yr_per AS Period,
       CUSTGP.CTRY_NM AS Market,		--//        custgp.Ctry_nm AS Market,
       CUSTGP.CUST_NUM AS "Customer Code",		--//        custgp.cust_num AS "Customer Code",
       'Not Applicable' AS "Material Code",
       CUSTGP.PRFT_CTR AS Profit_Center,		--//        custgp.prft_ctr AS Profit_Center,
       'Blank Material Number in BW COPA Extract' as error_desc,
       'Please update correct customer number in BW COPA extract to DNA' AS Corrective_Action
FROM EDW_RPT_COPA_CUSTOMERGP_AGG custgp		--// FROM rg_edw.edw_rpt_copa_customergp_agg custgp
where matl_num is null or matl_num = '' or matl_num = 'Not Available'
UNION ALL
select distinct 'Unassigned Profit Center in COPA' AS Error_Catgeory,
       CUSTGP.FISC_YR AS Year,		--//        custgp.Fisc_yr AS Year,
       CUSTGP.FISC_YR_PER AS Period,		--//        custgp.Fisc_yr_per AS Period,
       CUSTGP.CTRY_NM AS Market,		--//        custgp.Ctry_nm AS Market,
       CUSTGP.CUST_NUM AS "Customer Code",		--//        custgp.cust_num AS "Customer Code",
       CUSTGP.MATL_NUM AS "Material Code",		--//        custgp.matl_num AS "Material Code",
       'Not Applicable' AS Profit_Center,
       'Blank Profit Center in BW COPA Extract' as error_desc,
       'Please update correct Profit Center in BW COPA extract to DNA' AS Corrective_Action
FROM EDW_RPT_COPA_CUSTOMERGP_AGG custgp		--// FROM rg_edw.edw_rpt_copa_customergp_agg custgp
where prft_ctr is null or prft_ctr = '' or prft_ctr = 'Not Available'
UNION ALL
select distinct 'Incomplete COPA Record in DNA' AS Error_Catgeory,
       CUSTGP.FISC_YR AS Year,		--//        custgp.Fisc_yr AS Year,
       CUSTGP.FISC_YR_PER AS Period,		--//        custgp.Fisc_yr_per AS Period,
       CMP.CTRY_GROUP AS Market,		--//        cmp.Ctry_group AS Market,
       CUSTGP.CUST_NUM AS "Customer Code",		--//        custgp.cust_num AS "Customer Code",
       CUSTGP.MATL_NUM AS "Material Code",		--//        custgp.matl_num AS "Material Code",
       CUSTGP.PRFT_CTR AS Profit_Center,		--//        custgp.Prft_ctr AS Profit_Center,
       'Blank/empty Sales Org in COPA record' as error_desc,
       'Please update the necessary Sales Org in BW COPA extract to DNA' AS Corrective_Action
FROM EDW_COPA_TRANS_FACT custgp,edw_company_dim cmp,itg_custgp_cogs_fg_control fgctl		--// FROM rg_edw.edw_copa_trans_fact custgp,rg_edw.edw_company_dim cmp,rg_itg.itg_custgp_cogs_fg_control fgctl
where CUSTGP.ACCT_HIER_SHRT_DESC in ('NTS','FG') and		--// where custgp.acct_hier_shrt_desc in ('NTS','FG') and
(custgp.co_cd = CMP.CO_CD) and		--// (custgp.co_cd = cmp.co_cd) and
(custgp.acct_hier_shrt_desc	= FGCTL.ACCT_HIER_SHRT_DESC and		--// (custgp.acct_hier_shrt_desc	= fgctl.acct_hier_shrt_desc and
													   CUSTGP.CO_CD = FGCTL.CO_CD and		--// 													   custgp.co_cd = fgctl.co_cd and
													   CUSTGP.FISC_YR_PER >= FGCTL.VALID_FROM and		--// 													   custgp.fisc_yr_per >= fgctl.valid_from and
													   CUSTGP.FISC_YR_PER < FGCTL.VALID_TO and		--// 													   custgp.fisc_yr_per < fgctl.valid_to and
													   case when CUSTGP.ACCT_HIER_SHRT_DESC = 'FG'		--// 													   case when custgp.acct_hier_shrt_desc = 'FG'
													        THEN LTRIM(CUSTGP.ACCT_NUM,'0') = FGCTL.GL_ACCT_NUM		--// 													        then ltrim(custgp.acct_num,'0') = fgctl.gl_acct_num
															when CUSTGP.ACCT_HIER_SHRT_DESC = 'NTS'		--// 															when custgp.acct_hier_shrt_desc = 'NTS'
													        then '0' = FGCTL.GL_ACCT_NUM end and		--// 													        then '0' = fgctl.gl_acct_num end and
													   FGCTL.ACTIVE = 'Y'	)	and		--// 													   fgctl.active = 'Y'	)	and
(sls_org is null or sls_org = '' or sls_org = 'Not Available')  
)
select * from final