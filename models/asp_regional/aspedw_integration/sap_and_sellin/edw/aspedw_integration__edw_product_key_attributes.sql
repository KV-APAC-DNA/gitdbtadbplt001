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

edw_plant_dim as (
    select * from {{ ref('aspedw_integration__edw_plant_dim') }}
),
--Logical CTE

-- Final CTE
final as (
  SELECT 
        mat.matl_num::varchar(40) as matl_num,
        mat.matl_desc::varchar(100) as matl_desc,
        mat.crt_on::date as crt_on,
        cmp.ctry_nm::varchar(40) as ctry_nm,
        --cnty.cluster,
        mat.matl_type_cd::varchar(10) as matl_type_cd,
        mat.matl_type_desc::varchar(40) as matl_type_desc,
        mat.mega_brnd_cd::varchar(10) as mega_brnd_cd,
        case
            when upper(mat.mega_brnd_desc) like 'JOHNSON\'S%'
            then mat.brnd_desc
          else mat.mega_brnd_desc
       end::varchar(100) as mega_brnd_desc,
       mat.brnd_cd::varchar(10) as brnd_cd,
       mat.brnd_desc::varchar(100) as brnd_desc,
mat.varnt_desc::varchar(100) as varnt_desc,
mat.base_prod_desc::varchar(100) as base_prod_desc,
mat.put_up_desc::varchar(100) as put_up_desc,
mat.prodh1::varchar(18) as prodh1,
mat.prodh1_txtmd::varchar(100) as prodh1_txtmd,
mat.prodh2::varchar(18) as prodh2,
mat.prodh2_txtmd::varchar(100) as prodh2_txtmd,
mat.prodh3::varchar(18) as prodh3,
mat.prodh3_txtmd::varchar(100) as prodh3_txtmd,
mat.prodh4::varchar(18) as prodh4,
mat.prodh4_txtmd::varchar(100) as prodh4_txtmd,
mat.prodh5::varchar(18) as prodh5,
mat.prodh5_txtmd::varchar(100) as prodh5_txtmd,
mat.prodh6::varchar(18) as prodh6,
mat.prodh6_txtmd::varchar(100) as prodh6_txtmd,
mat.prod_hier_cd::varchar(40) as prod_hier_cd,
gch.gcph_franchise::varchar(30) as gcph_franchise,
gch.gcph_brand::varchar(80) as gcph_brand,
gch.gcph_subbrand::varchar(100) as gcph_subbrand,
gch.gcph_variant::varchar(100) as gcph_variant,
gch.gcph_needstate::varchar(50) as gcph_needstate,
gch.gcph_category::varchar(50) as gcph_category,
gch.gcph_subcategory::varchar(50) as gcph_subcategory,
gch.gcph_segment::varchar(50) as gcph_segment,
gch.gcph_subsegment::varchar(100) as gcph_subsegment,
gch.ean_upc::varchar(30) as ean_upc,
gch.apac_variant::varchar(256) as apac_variant,
gch.data_type::varchar(30) as data_type,
gch.description::varchar(256) as description,
gch.base_unit::varchar(30) as base_unit,
cast(gch."region" as varchar(50)) as "region",
gch.regional_brand::varchar(100) as regional_brand,
gch.regional_subbrand::varchar(256) as regional_subbrand,
gch.regional_megabrand::varchar(100) as regional_megabrand,
gch.regional_franchise::varchar(100) as regional_franchise,
gch.regional_franchise_group::varchar(50) as regional_franchise_group,
mat.pka_franchise_cd::varchar(2) as pka_franchise,
mat.pka_franchise_desc::varchar(30) as pka_franchise_description,
mat.pka_brand_cd::varchar(2) as pka_brand,
mat.pka_brand_desc::varchar(30) as pka_brand_description,
mat.pka_sub_brand_cd::varchar(4) as pka_subbrand,
mat.pka_sub_brand_desc::varchar(30) as pka_subbranddesc,
mat.pka_variant_cd::varchar(4) as pka_variant,
mat.pka_variant_desc::varchar(30) as pka_variantdesc,
mat.pka_sub_variant_cd::varchar(4) as pka_subvariant,
mat.pka_sub_variant_desc::varchar(30) as pka_subvariantdesc,
mat.pka_flavor_cd::varchar(4) as pka_flavor,
mat.pka_flavor_desc::varchar(30) as pka_flavordesc,
mat.pka_ingredient_cd::varchar(4) as pka_ingredient,
mat.pka_ingredient_desc::varchar(30) as pka_ingredientdesc,
mat.pka_application_cd::varchar(4) as pka_application,
mat.pka_application_desc::varchar(30) as pka_applicationdesc,
mat.pka_length_cd::varchar(4) as pka_strength,
mat.pka_length_desc::varchar(30) as pka_strengthdesc,
mat.pka_shape_cd::varchar(4) as pka_shape,
mat.pka_shape_desc::varchar(30) as pka_shapedesc,
mat.pka_spf_cd::varchar(4) as pka_spf,
mat.pka_spf_desc::varchar(30) as pka_spfdesc,
mat.pka_cover_cd::varchar(4) as pka_cover,
mat.pka_cover_desc::varchar(30) as pka_coverdesc,
mat.pka_form_cd::varchar(4) as pka_form,
mat.pka_form_desc::varchar(30) as pka_formdesc,
mat.pka_size_cd::varchar(4) as pka_size,
mat.pka_size_desc::varchar(30) as pka_sizedesc,
mat.pka_character_cd::varchar(4) as pka_character,
mat.pka_character_desc::varchar(30) as pka_charaterdesc,
mat.pka_package_cd::varchar(4) as pka_package,
mat.pka_package_desc::varchar(30) as pka_packagedesc,
mat.pka_attribute_13_cd::varchar(4) as pka_attribute13,
mat.pka_attribute_13_desc::varchar(30) as pka_attribute13desc,
mat.pka_attribute_14_cd::varchar(4) as pka_attribute14,
mat.pka_attribute_14_desc::varchar(30) as pka_attribute14desc,
mat.pka_sku_identification_cd::varchar(2) as pka_skuidentification,
mat.pka_sku_identification_desc::varchar(30) as pka_skuiddesc,
mat.pka_one_time_relabeling_cd::varchar(2) as pka_onetimerel,
mat.pka_one_time_relabeling_desc::varchar(30) as pka_onetimereldesc,
mat.pka_product_key::varchar(68) as pka_productkey,
mat.pka_product_key_description::varchar(255) as pka_productdesc,
mat.pka_root_code::varchar(68) as pka_rootcode,
MAT.pka_root_code_desc_1::varchar(255) as PKA_ROOTCODEDES,
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
) mat
LEFT JOIN EDW_GCH_PRODUCTHIERARCHY gch
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

