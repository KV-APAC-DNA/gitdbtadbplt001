WITH itg_product
AS (
    SELECT *
    FROM hcposeitg_integration.itg_product
    ),
itg_recordtype
AS (
    SELECT *
    FROM hcposeitg_integration.itg_recordtype
    ),
itg_common_prod_hier
AS (
    SELECT *
    FROM hcposeitg_integration.itg_common_prod_hier
    ),
T1 AS
 (
    SELECT DISTINCT PRODUCT_INDICATION_KEY,
        ITG_PROD.COUNTRY_CODE,
        NVL(ITG_PROD.PRODUCT_SOURCE_ID, 'Not Applicable') AS PRODUCT_SOURCE_ID,
        ITG_PROD.LAST_MODIFIED_DATE AS MODIFY_DT,
        NVL(ITG_PROD.LAST_MODIFIED_BY_ID, 'Not Applicable') AS MODIFY_ID,
        ITG_PROD.PRODUCT_NAME,
        ITG_PROD.PRODUCT_TYPE,
        THERAPEUTIC_AREA,
        PARENT_PRODUCT_KEY,
        COMPANY_PRODUCT,
        BUSINESS_UNIT,
        CONSUMER_SITE,
        PRODUCT_INFO,
        THERAPEUTIC_CLASS,
        MANUFACTURER,
        ITG_PROD.DESCRIPTION,
        FRANCHISE,
        REQUIRE_KEY_MESSAGE,
        CONTROLLED_SUBSTANCE,
        SAMPLE_QUANTITY_PICKLIST,
        ITG_PROD.DISPLAY_ORDER,
        NO_METRICS,
        DISTRIBUTOR,
        SAMPLE_QUANTITY_BOUND,
        SAMPLE_U_M,
        NO_DETAILS,
        RESTRICTED,
        NO_CYCLE_PLANS,
        SKU_ID,
        BIZ_SUB_UNIT,
        BIZ_UNIT,
        PRODUCT_SECTOR,
        NVL(ITG_PROD.EXTERNAL_ID, 'Not Applicable') AS EXTERNAL_ID,
        NVL(ITG_PROD.IS_DELETED, 0) AS IS_DELETED,
        ITG_REC.RECORD_TYPE_NAME,
        ITG_COM_PROD.BRAND_NAME AS COMMON_BRAND_NAME,
        ITG_COM_PROD.DA_NAME AS COMMON_DISEASE_AREA_NAME,
        ITG_COM_PROD.TA_NAME AS COMMON_THERAPEUTIC_AREA,
        ITG_PROD.SHC_Sector,
        ITG_PROD.SHC_Strategic_Group,
        ITG_PROD.SHC_Franchise,
        ITG_PROD.SHC_Brand,
        SYSDATE() AS INSERTED_DATE,
        SYSDATE() AS UPDATED_DATE
    FROM itg_product ITG_PROD
    LEFT OUTER JOIN itg_recordtype ITG_REC ON ITG_PROD.RECORD_TYPE_SOURCE_ID = ITG_REC.RECORD_TYPE_SOURCE_ID
        AND ITG_PROD.COUNTRY_CODE = ITG_REC.COUNTRY_CODE
    LEFT OUTER JOIN itg_common_prod_hier ITG_COM_PROD ON ITG_PROD.PRODUCT_SOURCE_ID = ITG_COM_PROD.PRODUCT_SFID
        AND ITG_PROD.COUNTRY_CODE = ITG_COM_PROD.COUNTRY_CODE
),   
T2 AS
(   ---DUMMY RECORD
    SELECT 'Not Applicable',
        'ZZ',
        'Not Applicable',
        SYSDATE(),
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        0,
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        0,
        0,
        'Not Applicable',
        0,
        0,
        'Not Applicable',
        0,
        'Not Applicable',
        0,
        0,
        0,
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        0,
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        SYSDATE(),
        SYSDATE()
    ),
JOINED AS
(
    SELECT * FROM T1
    UNION ALL
    SELECT * FROM T2
),
FINAL AS 
(
    SELECT PRODUCT_INDICATION_KEY::VARCHAR(32) AS PRODUCT_INDICATION_KEY,
        COUNTRY_CODE::VARCHAR(8) AS COUNTRY_CODE,
        PRODUCT_SOURCE_ID::VARCHAR(18) AS PRODUCT_SOURCE_ID,
        MODIFY_DT::TIMESTAMP_NTZ(9) AS MODIFY_DT,
        MODIFY_ID::VARCHAR(18) AS MODIFY_ID,
        PRODUCT_NAME::VARCHAR(80) AS PRODUCT_NAME,
        PRODUCT_TYPE::VARCHAR(255) AS PRODUCT_TYPE,
        THERAPEUTIC_AREA::VARCHAR(255) AS THERAPEUTIC_AREA_NAME,
        PARENT_PRODUCT_KEY::VARCHAR(80) AS PARENT_PRODUCT_KEY,
        COMPANY_PRODUCT::NUMBER(38,0) AS COMPANY_PRODUCT_FLAG,
        BUSINESS_UNIT::VARCHAR(4099) AS BUSINESS_UNIT,
        CONSUMER_SITE::VARCHAR(255) AS CONSUMER_SITE,
        PRODUCT_INFO::VARCHAR(255) AS PRODUCT_INFO,
        THERAPEUTIC_CLASS::VARCHAR(255) AS THERAPEUTIC_CLASS,
        MANUFACTURER::VARCHAR(255) AS MANUFACTURER,
        DESCRIPTION::VARCHAR(255) AS DESCRIPTION,
        FRANCHISE::VARCHAR(4099) AS FRANCHISE,
        REQUIRE_KEY_MESSAGE::NUMBER(38,0) AS REQUIRE_KEY_MESSAGE,
        CONTROLLED_SUBSTANCE::NUMBER(38,0) AS CONTROLLED_SUBSTANCE,
        SAMPLE_QUANTITY_PICKLIST::VARCHAR(1030) AS SAMPLE_QUANTITY_PICKLIST,
        DISPLAY_ORDER::NUMBER(5,0) AS DISPLAY_ORDER,
        NO_METRICS::NUMBER(38,0) AS NO_METRICS,
        DISTRIBUTOR::VARCHAR(255) AS DISTRIBUTOR,
        SAMPLE_QUANTITY_BOUND::NUMBER(38,0) AS SAMPLE_QUANTITY_BOUND,
        SAMPLE_U_M::VARCHAR(255) AS SAMPLE_U_M,
        NO_DETAILS::NUMBER(38,0) AS NO_DETAILS,
        RESTRICTED::NUMBER(38,0) AS RESTRICTED,
        NO_CYCLE_PLANS::NUMBER(38,0) AS NO_CYCLE_PLANS,
        SKU_ID::VARCHAR(25) AS SKU_ID,
        BIZ_SUB_UNIT::VARCHAR(255) AS BIZ_SUB_UNIT,
        BIZ_UNIT::VARCHAR(255) AS BIZ_UNIT,
        PRODUCT_SECTOR::VARCHAR(1030) AS PRODUCT_SECTOR,
        EXTERNAL_ID::VARCHAR(25) AS EXTERNAL_ID,
        RECORD_TYPE_NAME::VARCHAR(80) AS RECORD_TYPE_NAME,
        IS_DELETED::NUMBER(38,0) AS DELETED_FLAG,
        COMMON_BRAND_NAME::VARCHAR(255) AS COMMON_BRAND_NAME,
        COMMON_DISEASE_AREA_NAME::VARCHAR(255) AS COMMON_DISEASE_AREA_NAME,
        COMMON_THERAPEUTIC_AREA::VARCHAR(255) AS COMMON_THERAPEUTIC_AREA,
        SHC_SECTOR::VARCHAR(255) AS SHC_SECTOR,
        SHC_STRATEGIC_GROUP::VARCHAR(255) AS SHC_STRATEGIC_GROUP,
        SHC_FRANCHISE::VARCHAR(255) AS SHC_FRANCHISE,
        INSERTED_DATE::TIMESTAMP_NTZ(9) AS INSERTED_DATE,
        UPDATED_DATE::TIMESTAMP_NTZ(9) AS UPDATED_DATE,
        SHC_BRAND::VARCHAR(255) AS SHC_BRAND
    FROM JOINED
)
SELECT *
FROM FINAL
