--import cte
with itg_re_msl_input_definition as (
    select * from {{ source('aspitg_integration', 'itg_re_msl_input_definition') }}
),
edw_calendar_dim as (
    select * from {{ source('aspedw_integration', 'edw_calendar_dim') }}
),
itg_mds_master_mother_code_mapping as (
    select * from {{ source('aspitg_integration', 'itg_mds_master_mother_code_mapping') }}
),
edw_vw_cal_retail_excellence_dim as (
    select * from {{ source('aspedw_integration', 'edw_vw_cal_retail_excellence_dim') }}
),
wks_singapore_base_retail_excellence as (
    select * from {{ ref('chnwks_integration__wks_china_personal_care_base_retail_excellence') }}
),

cn_pc_re_msl_list
as
(
   SELECT distinct final.fisc_yr,
       final.JJ_MNTH_ID AS fisc_per,
       final.market AS Market,
       nvl(final.store_type,final.retail_environment) AS Retail_Environment,
	   final.soldto_code as soldto_code,
       final.Distributor_Code AS Distributor_Code,
       FINAL.Distributor_Name AS Distributor_Name,
       final.STORE_Code AS Store_Code,
       FINAL.STORE_Name AS STORE_NAME,
       final.store_type AS Store_Type,
       --final.pka_product_key_description AS sku_description,
	   --final.ean_code as product_code,
	   --replace by ean and use mother code as PRODUCT_CODE
	   final.ean as ean,
	   nvl(final.mother_code,'NA') as product_code,
	   final.msl_product_desc as product_name,
	   final.mapped_sku_code as mapped_sku_code,
       final.region AS Region,
       final.Zone,
       final.City,
       --final.province,
       final.Data_Src,
   --    final.pka_product_key_description AS PRODUCT_CODE,
       'China Personal Care' AS prod_hier_l1,
       SYSDATE AS Created_date
FROM (SELECT distinct base.start_date,
             base.end_date,
             base.market,
             base.retail_environment,
             base.jj_mnth_id,
             base.fisc_yr,
--             base.product_key,
       --base.ean_code,
	         noo.ean,
			 base.mother_code,
             noo.Data_Src,
             noo.CNTRY_CD,
             noo.CNTRY_NM,
			 noo.soldto_code,
             noo.distributor_code,
             noo.distributor_name,
             noo.store_code,
             noo.store_name,
             noo.store_type,
             noo.region,
             noo.zone,
             --noo.province,
             noo.city,
			 nvl(noo.msl_product_desc,pd.msl_product_desc) as msl_product_desc,
			 nvl(noo.mapped_sku_code,pd.sku_code) as mapped_sku_code
			 --,
--             noo.pka_product_key_description
       FROM (SELECT DISTINCT start_date,
                      END_DATE,
                      msl.market,
                      retail_environment,
                      --product_key,
					   ltrim(ean_code,'0') as ean_Code,
					   -----Include mother code as per new changes - June'2024
					   mother_code,
                      jj_mnth_id,
                      fisc_yr
               FROM (SELECT DISTINCT 
                            TO_CHAR (TO_DATE ( start_date,'DD/MM/YYYY'),'YYYYMM')::NUMERIC as start_date,
                            TO_CHAR (TO_DATE ( END_DATE,'DD/MM/YYYY'),'YYYYMM')::NUMERIC as END_DATE,
                            market,
                            upper(retail_environment) as retail_environment,
							ltrim(sku_unique_identifier,'0') as ean_code
                            --upper(sku_unique_identifier) as product_key 
                     FROM rg_itg.Itg_re_msl_input_definition
                     WHERE market = 'China Personal Care') msl
                    LEFT JOIN 
                       (SELECT DISTINCT fisc_yr, cal_mo_1,
                                        (SUBSTRING(FISC_PER,1,4) ||SUBSTRING(FISC_PER,6,7))::NUMERIC AS JJ_MNTH_ID
                         FROM rg_edw.edw_calendar_dim
                         WHERE jj_mnth_id >= (select last_17mnths from rg_edw.edw_vw_cal_Retail_excellence_Dim)::numeric
						   and jj_mnth_id <= (select last_2mnths from rg_edw.edw_vw_cal_Retail_excellence_Dim)::numeric  
                        ) cal
                     ON start_date <= cal.JJ_MNTH_ID
                    AND END_DATE >= cal.JJ_MNTH_ID
					LEFT JOIN  rg_itg.itg_mds_master_mother_code_mapping mc on ltrim(msl.ean_code,'0') = ltrim(mc.sku_unique_identifier,'0')
             ) base
        LEFT JOIN (SELECT distinct Data_Src,
                          'CN' as CNTRY_CD,
                          'China Personal Care' as CNTRY_NM,
                          Region ,
                          city,
                          zone,
                          Distributor_Name,
                          Distributor_code,
						  soldto_code,
                          store_code,
                          store_name,
                          store_type,
                          --PKA_PRODUCT_KEY_DESC as pka_product_key_description
						  EAN,
						  --use mother code as per latest changes - June'2024
						  --Mother_code,
						  product_code,
						  MSL_PRODUCT_DESC,
						  MAPPED_SKU_CODE
                   FROM CN_WKS.WKS_CNPC_ALL_RETAIL_EXCELLENCE
                  ) NOO
               ON upper(noo.store_type) = upper(base.Retail_Environment)
              --AND noo.pka_product_key_description = base.product_key
			   --AND noo.EAN = base.EAN_code
			     AND noo.product_code = base.mother_code
		LEFT JOIN CN_WKS.WKS_CNPC_REGIONAL_SELLOUT_MAPPED_SKU_CD pd on base.EAN_code = pd.ean 
		LEFT JOIN CN_WKS.WKS_CNPC_REGIONAL_SELLOUT_EAN ed
ON --upper(main.PKA_PRODUCT_KEY_DESC)=PD.PKA_PRODUCT_KEY_DESCRIPTION
upper(base.mother_code) = ed.mother_code  
              ) final 
)
