with source as 
(
  
  select distinct sap_matl_num,GMC_BRAND_NAME
from (

select 
        ltrim(replace(a.gmc_sku_code, 'AP', ''),'0') as sap_matl_num,
        a.*,
        b.GMC_DESCRIPTION,  
        b.GMC_SUBCATEGORY_CODE,  
        b.GMC_SUBCATEGORY_NAME,  
        b.GMC_BRAND_CODE,  
        b.GMC_BRAND_NAME,  
        b.GMC_SUBBRAND_CODE,  
        b.GMC_SUBBRAND_NAME,  
        b.GMC_VARIANT_CODE,  
        b.GMC_VARIANT_NAME,  
        b.GMC_SUBVARIANT_CODE,  
        b.GMC_SUBVARIANT_NAME,  
        b.GMC_FLAVOR_CODE,  
        b.GMC_FLAVOR_NAME,  
        b.GMC_INGREDIENT_CODE,  
        b.GMC_INGREDIENT_NAME,  
        b.GMC_APPLICATION_CODE,  
        b.GMC_APPLICATION_NAME,  
        b.GMC_LENGTH_STRENGTH_CODE,  
        b.GMC_LENGTH_STRENGTH_NAME,  
        b.GMC_SHAPE_CODE,  
        b.GMC_SHAPE_NAME,  
        b.GMC_SPF_CONCENTRATION_CODE,  
        b.GMC_SPF_CONCENTRATION_NAME,  
        b.GMC_COVER_PRODUCT_TYPE_CODE,  
        b.GMC_COVER_PRODUCT_TYPE_NAME,  
        b.GMC_FORM_CODE,  
        b.GMC_FORM_NAME,  
        b.GMC_SIZE_CODE,  
        b.GMC_SIZE_NAME,  
        b.GMC_CHARACTERS_SPECIAL_EDITION_CODE,  
        b.GMC_CHARACTERS_SPECIAL_EDITION_NAME,  
        b.GMC_PACKAGE_CODE,  
        b.GMC_PACKAGE_NAME,  
        b.GMC_UOM_CODE,  
        b.GMC_UOM_NAME,  
        b.P4_BRAND_CATEGORY_CODE,  
        b.P4_BRAND_CATEGORY,  
        b.B2_SUBBRAND_CODE,  
        b.B2_SUBBRAND,  
        b.C5_SUBCATEGORY_CODE,  
        b.C5_SUBCATEGORY
    from (
        select GMC_SKU_CODE, GMC_SKU_NAME, UNIQUE_ID, GMC_REGION, GMC_CODE, MATERIAL_TYPE 
        from PROD_CUSTOMER360_APACOBS.GLOBALMASTER_ACCESS.VW_DIM_GMC_SKU_MAPPINGS
        
         where gmc_region='APAC'
        -- and gmc_sku_code in (select distinct code from dev_dna_load.phlsdl_raw.SDL_MDS_PH_HCE_Product_Master )
         
        ) a
    join 
        (
        Select * 
        from PROD_CUSTOMER360_APACOBS.GLOBALMASTER_ACCESS.VW_DIM_GMC_ATTRIBUTE_MAPPINGS
        ) b 
    on a.gmc_code=b.gmc_code 
    ) a
    
),
final as 
(
    select * from source 
)
select * from final 