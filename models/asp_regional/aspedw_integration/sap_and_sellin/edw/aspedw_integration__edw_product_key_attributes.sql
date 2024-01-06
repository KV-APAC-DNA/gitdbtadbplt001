{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized= "table"
    )
}}

--Import CTE
with 
edw_material_dim as (
   select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
 
edw_gch_producthierarchy as (
   select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
),
 
edw_material_plant_dim as (
   select * from {{ ref('aspedw_integration__edw_material_plant_dim') }}
),
 
edw_material_dim as (
   select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
 
edw_company_dim as (
   select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
 
itg_mds_ap_greenlight_skus as (
   select * from {{ ref('aspitg_integration__itg_mds_ap_greenlight_skus') }}
),
 
edw_copa_trans_fact as (
   select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }}
),

--Logical CTE

-- Final CTE
final as (
  SELECT MAT.MATL_NUM,
       MAT.MATL_DESC,
       MAT.CRT_ON,
       CMP.CTRY_NM,
       --CNTY.CLUSTER,
       MAT.MATL_TYPE_CD,
       MAT.MATL_TYPE_DESC,
       MAT.MEGA_BRND_CD,
       CASE
            WHEN UPPER(MAT.MEGA_BRND_DESC) LIKE 'JOHNSONS%'
            THEN MAT.BRND_DESC
          ELSE MAT.MEGA_BRND_DESC
       END MEGA_BRND_DESC,
       MAT.BRND_CD,
       MAT.BRND_DESC,
       MAT.VARNT_DESC,
       MAT.BASE_PROD_DESC,
       MAT.PUT_UP_DESC,
       MAT.PRODH1,
       MAT.PRODH1_TXTMD,
       MAT.PRODH2,
       MAT.PRODH2_TXTMD,
       MAT.PRODH3,
       MAT.PRODH3_TXTMD,
       MAT.PRODH4,
       MAT.PRODH4_TXTMD,
       MAT.PRODH5,
       MAT.PRODH5_TXTMD,
       MAT.PRODH6,
       MAT.PRODH6_TXTMD,
       MAT.PROD_HIER_CD,
       GCH.GCPH_FRANCHISE,
       GCH.GCPH_BRAND,
       GCH.GCPH_SUBBRAND,
       GCH.GCPH_VARIANT,
       GCH.GCPH_NEEDSTATE,
       GCH.GCPH_CATEGORY,
       GCH.GCPH_SUBCATEGORY,
       GCH.GCPH_SEGMENT,
       GCH.GCPH_SUBSEGMENT,
       GCH.EAN_UPC,
       GCH.APAC_VARIANT,
       GCH.DATA_TYPE,
       GCH.DESCRIPTION,
       GCH.BASE_UNIT,
       GCH."region" ,
       GCH.REGIONAL_BRAND,
       GCH.REGIONAL_SUBBRAND,
       GCH.REGIONAL_MEGABRAND,
       GCH.REGIONAL_FRANCHISE,
       GCH.REGIONAL_FRANCHISE_GROUP,
       MAT.pka_franchise_cd AS PKA_FRANCHISE,
       MAT.pka_franchise_desc AS PKA_FRANCHISE_DESCRIPTION,
       MAT.pka_brand_cd AS PKA_BRAND,
       MAT.pka_brand_desc AS PKA_BRAND_DESCRIPTION,
       MAT.pka_sub_brand_cd AS PKA_SUBBRAND,
       MAT.pka_sub_brand_desc AS PKA_SUBBRANDDESC,
       MAT.pka_variant_cd AS PKA_VARIANT,
       MAT.pka_variant_desc AS PKA_VARIANTDESC,
       MAT.pka_sub_variant_cd AS PKA_SUBVARIANT,
       MAT.pka_sub_variant_desc AS PKA_SUBVARIANTDESC,
       MAT.pka_flavor_cd AS PKA_FLAVOR,
       MAT.pka_flavor_desc AS PKA_FLAVORDESC,
       MAT.pka_ingredient_cd AS PKA_INGREDIENT,
       MAT.pka_ingredient_desc AS PKA_INGREDIENTDESC,
       MAT.pka_application_cd AS PKA_APPLICATION,
       MAT.pka_application_desc AS PKA_APPLICATIONDESC,
       MAT.pka_length_cd AS PKA_STRENGTH,
       MAT.pka_length_desc AS PKA_STRENGTHDESC,
       MAT.pka_shape_cd AS PKA_SHAPE,
       MAT.pka_shape_desc AS PKA_SHAPEDESC,
       MAT.pka_spf_cd AS PKA_SPF,
       MAT.pka_spf_desc AS PKA_SPFDESC,
       MAT.pka_cover_cd AS PKA_COVER,
       MAT.pka_cover_desc AS PKA_COVERDESC,
       MAT.pka_form_cd AS PKA_FORM,
       MAT.pka_form_desc AS PKA_FORMDESC,
       MAT.pka_size_cd AS PKA_SIZE,
       MAT.pka_size_desc AS PKA_SIZEDESC,
       MAT.pka_character_cd AS PKA_CHARACTER,
       MAT.pka_character_desc AS PKA_CHARATERDESC,
       MAT.pka_package_cd AS PKA_PACKAGE,
       MAT.pka_package_desc AS PKA_PACKAGEDESC,
       MAT.pka_attribute_13_cd AS PKA_ATTRIBUTE13,
       MAT.pka_attribute_13_desc AS PKA_ATTRIBUTE13DESC,
       MAT.pka_attribute_14_cd AS PKA_ATTRIBUTE14,
       MAT.pka_attribute_14_desc AS PKA_ATTRIBUTE14DESC,
       MAT.pka_sku_identification_cd AS PKA_SKUIDENTIFICATION,
       MAT.pka_sku_identification_desc AS PKA_SKUIDDESC,
       MAT.pka_one_time_relabeling_cd AS PKA_ONETIMEREL,
       MAT.pka_one_time_relabeling_desc AS PKA_ONETIMERELDESC,
       MAT.pka_product_key AS PKA_PRODUCTKEY,
       MAT.pka_product_key_description AS PKA_PRODUCTDESC,
       MAT.pka_root_code AS PKA_ROOTCODE,
       MAT.pka_root_code_desc_1 AS PKA_ROOTCODEDES,
       CASE WHEN NTS.MATL_NUM IS NULL THEN 'N' ELSE 'Y' END AS NTS_FLAG,
       NTS.FISC_YR_PER AS LST_NTS,
       current_timestamp()::timestamp_ntz(9) as LOAD_DATE,
       CASE WHEN GREENLIGHT.greenlight_sku_flag = 'Y' THEN 'Y' ELSE 'N' END AS GREENLIGHT_SKU_FLAG,
        --start Add new column total_size = size*package AEBU-10288
        CASE WHEN MAT.TS = '11111.00' THEN 'PACK'
           WHEN MAT.TS = '99999.00' THEN 'BULK'
           ELSE MAT.TS
          END AS TOTAL_SIZE
		--End Add new column total_size = size*package AEBU-10288
FROM (SELECT *
 -- Start Add new column total_size = size*package AEBU-10288
		,CASE
         WHEN SUBSTRING(pka_package_desc,1,1) = 'x' THEN SUBSTRING(pka_package_desc,2,LENGTH(pka_package_desc))
         WHEN UPPER(pka_package_desc) = 'ASSORTED PACK' THEN '1' --'PACK'
         WHEN UPPER(pka_package_desc) = 'MIX PACK' THEN '1'
         WHEN UPPER(pka_package_desc) = 'REFILL PACK' THEN '1'
         WHEN UPPER(pka_package_desc) = 'REFILL TRIPLE PACK' THEN '3'
         WHEN UPPER(pka_package_desc) = 'REFILL TWIN PACK' THEN '2'
         WHEN UPPER(pka_package_desc) = 'REFILL X12' THEN '12'
         WHEN UPPER(pka_package_desc) = 'REFILL X13' THEN '13'
         WHEN UPPER(pka_package_desc) = 'REFILL X6' THEN '6'
         WHEN UPPER(pka_package_desc) = 'REFILL X8' THEN '8'
         WHEN UPPER(pka_package_desc) = 'REGULAR' THEN '1'
         WHEN UPPER(pka_package_desc) = 'STANDIPOUCH' THEN '1'
         WHEN UPPER(pka_package_desc) = 'TRIPLE PACK' THEN '3'
         WHEN UPPER(pka_package_desc) = 'TWIN PACK' THEN '2'
         ELSE pka_package_desc
       END AS pka_package_desc_v1,
       CASE WHEN UPPER(pka_size_desc) = 'PACK' THEN '11111'
             WHEN UPPER(pka_size_desc) = 'BULK' THEN '99999'
          ELSE replace(substring(pka_size_desc,1,regexp_instr(pka_size_desc,'[a-z]|[A-Z]') -1),',','.')
          -- ELSE REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(pka_size_desc,'millilitre',''),'gram',''),'piece',''),'meter',''),'sheet',''),'spray',''),',','')
       END  AS pka_size_desc_v1,
	   CAST ( (CAST(TRIM(pka_package_desc_v1) AS INTEGER) * CAST(TRIM(pka_size_desc_v1) AS NUMERIC(38,2))) AS VARCHAR(30)) AS TS
	   -- End Add new column total_size = size*package AEBU-10288
FROM EDW_MATERIAL_DIM
WHERE MATL_NUM IN (SELECT DISTINCT MATL_NUM
                   FROM (SELECT MATL_NUM,
                                FISC_YR_PER,
                                SUM(AMT_OBJ_CRNCY) AMT_OBJ_CRNCY
                         FROM EDW_COPA_TRANS_FACT
                         WHERE ACCT_HIER_SHRT_DESC = 'NTS'
                         AND   TRIM(MATL_NUM) IS NOT NULL
                         AND   TRIM(MATL_NUM) != ''
                         GROUP BY MATL_NUM,
                                  FISC_YR_PER)
                   WHERE FISC_YR_PER >= '2017001'
                   GROUP BY MATL_NUM
                   UNION
                   SELECT MATL_NUM
                   FROM EDW_MATERIAL_DIM MAT
                   WHERE MAT.MATL_TYPE_CD IN ('FERT')
                  AND   crt_on >= '20190101')
) MAT
LEFT JOIN EDW_GCH_PRODUCTHIERARCHY GCH
ON MAT.MATL_NUM = GCH.MATERIALNUMBER
LEFT JOIN (SELECT DISTINCT EMPD.MATL_NUM, --EMPD.PLNT, EPD.CO_CD,
            CASE
			--WHEN ECD.CTRY_GROUP = 'OTC' THEN 'China OTC'
              --   WHEN ECD.CTRY_GROUP = 'China' THEN 'China Skincare'
				         WHEN ECD.CTRY_GROUP = 'Dabao' THEN 'China Personal Care'
            ELSE CTRY_GROUP
            END  CTRY_NM
      FROM EDW_MATERIAL_PLANT_DIM EMPD,
           EDW_PLANT_DIM EPD,
           EDW_COMPANY_DIM ECD
      WHERE EMPD.PLNT = EPD.PLNT(+)
      AND   EPD.CO_CD = ECD.CO_CD(+)) CMP
ON MAT.MATL_NUM = CMP.MATL_NUM
LEFT JOIN (SELECT market, material_number, GREENLIGHT_SKU_FLAG
           FROM itg_mds_ap_greenlight_skus GROUP BY 1,2,3) GREENLIGHT
ON LTRIM(MAT.MATL_NUM,'0') = LTRIM(GREENLIGHT.material_number,'0') AND CMP.CTRY_NM = GREENLIGHT.market
LEFT JOIN
     --ITG_MATERIAL_PKA_MARA_EXTRACT IMPA,
     (SELECT MATL_NUM, MAX(FISC_YR_PER) FISC_YR_PER
		FROM (SELECT MATL_NUM,FISC_YR_PER,SUM(AMT_OBJ_CRNCY) AMT_OBJ_CRNCY
			  FROM EDW_COPA_TRANS_FACT
			  WHERE ACCT_HIER_SHRT_DESC = 'NTS' AND TRIM(MATL_NUM) IS NOT NULL AND TRIM(MATL_NUM) != ''
			  GROUP BY MATL_NUM,FISC_YR_PER)
		WHERE AMT_OBJ_CRNCY != '0.0'
		GROUP BY MATL_NUM) NTS
ON MAT.MATL_NUM = NTS.MATL_NUM 
--   current_timestamp()::timestamp_ntz(9) as crt_dttm,
--   current_timestamp()::timestamp_ntz(9) as updt_dttm

)


--Final select
select * from final 