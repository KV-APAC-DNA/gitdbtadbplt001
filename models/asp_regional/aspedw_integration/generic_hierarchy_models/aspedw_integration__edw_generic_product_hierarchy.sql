with EDW_MATERIAL_DIM as (
    select * from {{ source('aspedw_integration','edw_material_dim') }}
),
EDW_GCH_PRODUCTHIERARCHY as (
   select * from {{ source('aspedw_integration','edw_gch_producthierarchy') }}
),

TRANSFORMED as (select distinct
EMD.MATL_NUM as SAP_MATL_NUM,
--EMD.MATL_ID as SAP_MATL_NUM,
                                EMD.MATL_DESC as SAP_MAT_DESC,
                                EMD.MATL_TYPE_CD as SAP_MAT_TYPE_CD,
                                EMD.MATL_TYPE_DESC as SAP_MAT_TYPE_DESC,
                                EMD.PRODH1 as SAP_PROD_SGMT_CD,
                                EMD.PRODH1_TXTMD as SAP_PROD_SGMT_DESC,
                                EMD.BASE_PROD_DESC as SAP_BASE_PROD_DESC,
                                EMD.MEGA_BRND_DESC as SAP_MEGA_BRND_DESC,
                                EMD.BRND_DESC as SAP_BRND_DESC,
                                EMD.VARNT_DESC as SAP_VRNT_DESC,
                                EMD.PUT_UP_DESC as SAP_PUT_UP_DESC,
                                EMD.PRODH2 as SAP_GRP_FRNCHSE_CD,
                                EMD.PRODH2_TXTMD as SAP_GRP_FRNCHSE_DESC,
                                EMD.PRODH3 as SAP_FRNCHSE_CD,
                                EMD.PRODH3_TXTMD as SAP_FRNCHSE_DESC,
                                EMD.PRODH4 as SAP_PROD_FRNCHSE_CD,
                                EMD.PRODH4_TXTMD as SAP_PROD_FRNCHSE_DESC,
                                EMD.PRODH5 as SAP_PROD_MJR_CD,
                                EMD.PRODH5_TXTMD as SAP_PROD_MJR_DESC,
                                EMD.PRODH5 as SAP_PROD_MNR_CD,
                                EMD.PRODH5_TXTMD as SAP_PROD_MNR_DESC,
                                EMD.PRODH6 as SAP_PROD_HIER_CD,
                                EMD.PRODH6_TXTMD as SAP_PROD_HIER_DESC,
                                EMD.PKA_PRODUCT_KEY,
                                EMD.PKA_PRODUCT_KEY_DESCRIPTION,
                                EMD.PKA_FRANCHISE_DESC,
                                EMD.PKA_BRAND_DESC,
                                EMD.PKA_SUB_BRAND_DESC,
                                EMD.PKA_VARIANT_DESC,
                                EMD.PKA_SUB_VARIANT_DESC,
                                EMD.PRMRY_UPC_CD as EAN,
                                --REGION COLUMN DEFINED IN SMALL CASE IN SNOWFLAKE TABLES
                                EGPH."region" as GPH_REGION,
                                EGPH.REGIONAL_FRANCHISE as GPH_REG_FRNCHSE,
                                EGPH.REGIONAL_FRANCHISE_GROUP as GPH_REG_FRNCHSE_GRP,
                                EGPH.GCPH_FRANCHISE as GPH_PROD_FRNCHSE,
                                EGPH.GCPH_BRAND as GPH_PROD_BRND,
                                EGPH.GCPH_SUBBRAND as GPH_PROD_SUB_BRND,
                                EGPH.GCPH_VARIANT as GPH_PROD_VRNT,
                                EGPH.GCPH_NEEDSTATE as GPH_PROD_NEEDSTATE,
                                EGPH.GCPH_CATEGORY as GPH_PROD_CTGRY,
                                EGPH.GCPH_SUBCATEGORY as GPH_PROD_SUBCTGRY,
                                EGPH.GCPH_SEGMENT as GPH_PROD_SGMNT,
                                EGPH.GCPH_SUBSEGMENT as GPH_PROD_SUBSGMNT,
                                EGPH.PUT_UP_CODE as GPH_PROD_PUT_UP_CD,
                                EGPH.PUT_UP_DESCRIPTION as GPH_PROD_PUT_UP_DESC,
                                EGPH.SIZE as GPH_PROD_SIZE,
                                EGPH.UNIT_OF_MEASURE as GPH_PROD_SIZE_UOM,
                                ROW_NUMBER() over (partition by SAP_MATL_NUM order by SAP_MATL_NUM) as RANK
                         from EDW_MATERIAL_DIM as EMD,
                              EDW_GCH_PRODUCTHIERARCHY as EGPH
                         where LTRIM(EMD.MATL_NUM, 0) = LTRIM(EGPH.MATERIALNUMBER(+), 0)
                         and EMD.PROD_HIER_CD <> ''
)

select * from TRANSFORMED