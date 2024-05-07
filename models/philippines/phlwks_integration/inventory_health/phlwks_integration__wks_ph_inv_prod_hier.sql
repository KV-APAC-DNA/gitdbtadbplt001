with edw_ph_siso_analysis as (
select * from {{ ref('phledw_integration__edw_ph_siso_analysis') }}
),
edw_vw_greenlight_skus as (
select * from {{ ref('aspedw_integration__edw_vw_greenlight_skus') }}
),
final as (
Select * from (Select a.*,
greenlight_sku_flag ,
pka_product_key ,
pka_product_key_description,
product_key ,
product_key_description,
row_number () over( partition by SKU_CD order by SKU_CD) as rnk
 from (
SELECT *
FROM (SELECT DISTINCT GLOBAL_PROD_FRANCHISE,
             GLOBAL_PROD_BRAND,
             GLOBAL_PROD_SUB_BRAND,
             GLOBAL_PROD_VARIANT,
             GLOBAL_PROD_SEGMENT,
             GLOBAL_PROD_SUBSEGMENT,
             GLOBAL_PROD_CATEGORY,
             GLOBAL_PROD_SUBCATEGORY,
             GLOBAL_PUT_UP_DESC,
             nvl(NULLIF(sku,''),'NA') AS SKU_CD,
             SKU_DESC AS SKU_DESC
      FROM EDW_PH_SISO_ANALYSIS)  where sku_cd!='NA')a

    left join 
              ( Select 
            MATL_NUM,  
      greenlight_sku_flag ,
		  pka_product_key ,
		  pka_product_key_description,
		  product_key ,
		  product_key_description
 from edw_vw_greenlight_skus  where sls_org='2300')EMD
               on ltrim(a.sku_cd,'0')=ltrim(EMD.MATL_NUM,'0')
               
               ) 
             where rnk=1 
     )
select * from final 