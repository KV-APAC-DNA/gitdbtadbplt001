with WKS_INDIA_REGIONAL_SELLOUT as
(
    select * from {{ ref('indwks_integration__wks_india_regional_sellout') }}
),
transformed as
(
    SELECT *,
        MIN(CAL_DATE) OVER (PARTITION BY SAP_PARENT_CUSTOMER_KEY) AS CUSTOMER_MIN_DATE,
        MIN(CAL_DATE) OVER (PARTITION BY COUNTRY_NAME) AS MARKET_MIN_DATE,
        RANK() OVER (
            PARTITION BY SAP_PARENT_CUSTOMER_KEY,
            PKA_PRODUCT_KEY ORDER BY CAL_DATE
            ) AS RN_CUS,
        RANK() OVER (
            PARTITION BY COUNTRY_NAME,
            PKA_PRODUCT_KEY ORDER BY CAL_DATE
            ) AS RN_MKT,
        MIN(CAL_DATE) OVER (
            PARTITION BY SAP_PARENT_CUSTOMER_KEY,
            PKA_PRODUCT_KEY
            ) AS CUSTOMER_PRODUCT_MIN_DATE,
        MIN(CAL_DATE) OVER (
            PARTITION BY COUNTRY_NAME,
            PKA_PRODUCT_KEY
            ) AS MARKET_PRODUCT_MIN_DATE
    FROM WKS_INDIA_REGIONAL_SELLOUT
)
select * from transformed
