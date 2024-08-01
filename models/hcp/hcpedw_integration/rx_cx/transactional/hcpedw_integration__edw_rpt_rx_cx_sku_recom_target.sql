with wks_rx_cx_sku_recom_target as(
    select * from {{ ref('hcpwks_integration__wks_rx_cx_sku_recom_target') }}
),
final as(
    SELECT
        URC::VARCHAR(50) as URC,
        MOTHER_SKU_CD::VARCHAR(100) as MOTHER_SKU_CD,
        YEAR::NUMBER(18,0) as YEAR,
        QUARTER::VARCHAR(2) as QUARTER,
        MONTH::NUMBER(18,0) as MONTH,
        QUARTERLY_SOQ::NUMBER(18,2) as QUARTERLY_SOQ,
        MONTHLY_SOQ::NUMBER(18,2) as MONTHLY_SOQ,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm
    FROM  
    wks_rx_cx_sku_recom_target
)
select * from final