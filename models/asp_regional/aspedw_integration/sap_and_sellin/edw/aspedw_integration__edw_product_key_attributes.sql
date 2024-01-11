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
  SELECT 
       mat.matl_num,
       mat.matl_desc,
       mat.crt_on,
       cmp.ctry_nm,
       --cnty.cluster,
       mat.matl_type_cd,
       mat.matl_type_desc,
       mat.mega_brnd_cd,
       case
            when upper(mat.mega_brnd_desc) like 'JOHNSONS%'
            then mat.brnd_desc
          else mat.mega_brnd_desc
       end mega_brnd_desc,
       mat.brnd_cd,
       mat.brnd_desc,
       mat.varnt_desc,
       mat.base_prod_desc,
       mat.put_up_desc,
       mat.prodh1,
       mat.prodh1_txtmd,
       mat.prodh2,
       mat.prodh2_txtmd,
       mat.prodh3,
       mat.prodh3_txtmd,
       mat.prodh4,
       mat.prodh4_txtmd,
       mat.prodh5,
       mat.prodh5_txtmd,
       mat.prodh6,
       mat.prodh6_txtmd,
       mat.prod_hier_cd,
       gch.gcph_franchise,
       gch.gcph_brand,
       gch.gcph_subbrand,
       gch.gcph_variant,
       gch.gcph_needstate,
       gch.gcph_category,
       gch.gcph_subcategory,
       gch.gcph_segment,
       gch.gcph_subsegment,
       gch.ean_upc,
       gch.apac_variant,
       gch.data_type,
       gch.description,
       gch.base_unit,
       gch.region,
       gch.regional_brand,
       gch.regional_subbrand,
       gch.regional_megabrand,
       gch.regional_franchise,
       gch.regional_franchise_group,
       mat.pka_franchise_cd as pka_franchise,
       mat.pka_franchise_desc as pka_franchise_description,
       mat.pka_brand_cd as pka_brand,
       mat.pka_brand_desc as pka_brand_description,
       mat.pka_sub_brand_cd as pka_subbrand,
       mat.pka_sub_brand_desc as pka_subbranddesc,
       mat.pka_variant_cd as pka_variant,
       mat.pka_variant_desc as pka_variantdesc,
       mat.pka_sub_variant_cd as pka_subvariant,
       mat.pka_sub_variant_desc as pka_subvariantdesc,
       mat.pka_flavor_cd as pka_flavor,
       mat.pka_flavor_desc as pka_flavordesc,
       mat.pka_ingredient_cd as pka_ingredient,
       mat.pka_ingredient_desc as pka_ingredientdesc,
       mat.pka_application_cd as pka_application,
       mat.pka_application_desc as pka_applicationdesc,
       mat.pka_length_cd as pka_strength,
       mat.pka_length_desc as pka_strengthdesc,
       mat.pka_shape_cd as pka_shape,
       mat.pka_shape_desc as pka_shapedesc,
       mat.pka_spf_cd as pka_spf,
       mat.pka_spf_desc as pka_spfdesc,
       mat.pka_cover_cd as pka_cover,
       mat.pka_cover_desc as pka_coverdesc,
       mat.pka_form_cd as pka_form,
       mat.pka_form_desc as pka_formdesc,
       mat.pka_size_cd as pka_size,
       mat.pka_size_desc as pka_sizedesc,
       mat.pka_character_cd as pka_character,
       mat.pka_character_desc as pka_charaterdesc,
       mat.pka_package_cd as pka_package,
       mat.pka_package_desc as pka_packagedesc,
       mat.pka_attribute_13_cd as pka_attribute13,
       mat.pka_attribute_13_desc as pka_attribute13desc,
       mat.pka_attribute_14_cd as pka_attribute14,
       mat.pka_attribute_14_desc as pka_attribute14desc,
       mat.pka_sku_identification_cd as pka_skuidentification,
       mat.pka_sku_identification_desc as pka_skuiddesc,
       mat.pka_one_time_relabeling_cd as pka_onetimerel,
       mat.pka_one_time_relabeling_desc as pka_onetimereldesc,
       mat.pka_product_key as pka_productkey,
       mat.pka_product_key_description as pka_productdesc,
       mat.pka_root_code as pka_rootcode,
       mat.pka_root_code_desc_1 as pka_rootcodedes,
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