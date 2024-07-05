with itg_sku_recom_flag_msl_spike as
(
    select * from {{ ref('inditg_integration__itg_sku_recom_flag_msl_spike') }}
),
itg_query_parameters as 
(
    select * from {{ source('inditg_integration', 'itg_query_parameters') }}
),
final as
(
    SELECT sku.mnth_id,
       sku.cust_cd,
       sku.retailer_cd,
       sku.mother_sku_cd,
       AVG(sku.oos_flag) AS oos_flag,
       AVG(sku.ms_flag) AS ms_flag,
       AVG(sku.cs_flag) AS cs_flag,
       AVG(sku.soq) AS soq,
       TO_DATE((sku.mnth_id)::CHARACTER VARYING::TEXT,'YYYYMM'::CHARACTER VARYING::TEXT) AS reco_date
    FROM itg_sku_recom_flag_msl_spike sku
    WHERE NOT (sku.cust_cd IS NULL OR sku.retailer_cd IS NULL)
      AND sku.mnth_id >= (SELECT PARAMETER_VALUE AS PARAMETER_VALUE
                          FROM  itg_query_parameters
                          WHERE UPPER(COUNTRY_CODE) = 'IN'
                          AND   UPPER(PARAMETER_TYPE) = 'START_PERIOD'
                          AND   UPPER(PARAMETER_NAME) = 'SKU_RECOM_2024_GT_SSS_START_PERIOD')
    GROUP BY sku.mnth_id,
         sku.cust_cd,
         sku.retailer_cd,
         sku.mother_sku_cd,
         TO_DATE((sku.mnth_id)::CHARACTER VARYING::TEXT,'YYYYMM'::CHARACTER VARYING::TEXT)
)
select * from final