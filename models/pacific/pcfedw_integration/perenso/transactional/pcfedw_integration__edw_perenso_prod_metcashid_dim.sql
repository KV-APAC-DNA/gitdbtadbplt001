with edw_perenso_prod_dim as (
    select * from {{ ref('pcfedw_integration__edw_perenso_prod_dim') }}
),
itg_perenso_product_reln_id as (
    select * from {{ ref('pcfitg_integration__itg_perenso_product_reln_id') }}
),
itg_perenso_product_fields as (
   select * from {{ ref('pcfitg_integration__itg_perenso_product_fields') }}
),

main as (
    SELECT EDW.prod_key,
       EDW.PROD_DESC,
       EDW.PROD_ID,
       EDW.PROD_EAN,
       EDW.PROD_JJ_FRANCHISE,
       EDW.PROD_JJ_CATEGORY,
       EDW.PROD_JJ_BRAND,
       EDW.PROD_SAP_FRANCHISE,
       EDW.PROD_SAP_PROFIT_CENTRE,
       EDW.PROD_SAP_PRODUCT_MAJOR,
       EDW.PROD_GROCERY_FRANCHISE,
       EDW.PROD_GROCERY_CATEGORY,
       EDW.PROD_GROCERY_BRAND,
       EDW.PROD_ACTIVE_NZ_PHARMA,
       EDW.PROD_ACTIVE_AU_GROCERY,
       EDW.PROD_ACTIVE_METCASH,
       EDW.PROD_ACTIVE_NZ_GROCERY,
       EDW.PROD_ACTIVE_AU_PHARMA,
       EDW.PROD_PBS,
       EDW.PROD_IMS_BRAND,
       EDW.PROD_NZ_CODE,
       EDW.PROD_METCASH_CODE,
       RELN.ID AS PRODUCT_PROBE_ID,
       EDW.PROD_OLD_ID,
       EDW.PROD_OLD_EAN,
       EDW.PROD_TAX,
       EDW.PROD_BWP_AUD,
       EDW.PROD_BWP_NZD
FROM edw_perenso_prod_dim EDW,
     itg_perenso_product_reln_id RELN,
     itg_perenso_product_fields FIELDS
WHERE EDW.PROD_KEY = RELN.PROD_KEY
AND   RELN.FIELD_KEY = FIELDS.FIELD_KEY
AND   UPPER(FIELDS.FIELD_DESC) = 'METCASH PDE'
),

final as (
SELECT
  PROD_KEY,
  PROD_DESC,
  PROD_ID,
  PROD_EAN,
  PROD_JJ_FRANCHISE,
  PROD_JJ_CATEGORY,
  PROD_JJ_BRAND,
  PROD_SAP_FRANCHISE,
  PROD_SAP_PROFIT_CENTRE,
  PROD_SAP_PRODUCT_MAJOR,
  PROD_GROCERY_FRANCHISE,
  PROD_GROCERY_CATEGORY,
  PROD_GROCERY_BRAND,
  PROD_ACTIVE_NZ_PHARMA,
  PROD_ACTIVE_AU_GROCERY,
  PROD_ACTIVE_METCASH,
  PROD_ACTIVE_NZ_GROCERY,
  PROD_ACTIVE_AU_PHARMA,
  PROD_PBS,
  PROD_IMS_BRAND,
  PROD_NZ_CODE,
  PROD_METCASH_CODE,
  PRODUCT_PROBE_ID,
  PROD_OLD_ID,
  PROD_OLD_EAN,
  PROD_TAX,
  PROD_BWP_AUD,
  PROD_BWP_NZD from 
  (
    SELECT
        PROD_KEY,
        PROD_DESC,
        PROD_ID,
        PROD_EAN,
        PROD_JJ_FRANCHISE,
        PROD_JJ_CATEGORY,
        PROD_JJ_BRAND,
        PROD_SAP_FRANCHISE,
        PROD_SAP_PROFIT_CENTRE,
        PROD_SAP_PRODUCT_MAJOR,
        PROD_GROCERY_FRANCHISE,
        PROD_GROCERY_CATEGORY,
        PROD_GROCERY_BRAND,
        PROD_ACTIVE_NZ_PHARMA,
        PROD_ACTIVE_AU_GROCERY,
        PROD_ACTIVE_METCASH,
        PROD_ACTIVE_NZ_GROCERY,
        PROD_ACTIVE_AU_PHARMA,
        PROD_PBS,
        PROD_IMS_BRAND,
        PROD_NZ_CODE,
        PROD_METCASH_CODE,
        PRODUCT_PROBE_ID,
        PROD_OLD_ID,
        PROD_OLD_EAN,
        PROD_TAX,
        PROD_BWP_AUD,
        PROD_BWP_NZD,
        ROW_NUMBER() OVER (PARTITION BY main.PRODUCT_PROBE_ID ORDER BY main.prod_key DESC) AS rno
    from main
)
WHERE rno = 1
)

select * from final