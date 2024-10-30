{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key = ["business_unit","recording_date","stock_type","product_code","product_name","batch","plant_code "]
        )
}}
--Import CTE
with source as 
(
    select * from {{ source('vnmsdl_raw', 'sdl_vn_dksh_sdl_otc') }}
),  

final as 
(
Select      BUSINESS_UNIT::VARCHAR(100),
            RECORDING_DATE::VARCHAR(100),
            STOCK_TYPE::VARCHAR(100),
            PRODUCT_CODE::VARCHAR(100),
            PRODUCT_NAME::VARCHAR(100),
            BATCH::VARCHAR(100),
            EXPIRY_DATE::VARCHAR(100),
            BASE_UOM::VARCHAR(100),
            SHELF_LIFE::VARCHAR(100),
            COGS::VARCHAR(100),
            REGION::VARCHAR(100),
            QUANTITY::NUMBER(20,0),
            VALUE::VARCHAR(100),
            PLANT_CODE::VARCHAR(100),
            PLANT_NAME::VARCHAR(100),
            SYSLOT::VARCHAR(100),
            convert_timezone('Asia/Singapore',current_timestamp)::timestamp_ntz(9) AS crt_dttm,
            convert_timezone('Asia/Singapore',current_timestamp)::timestamp_ntz(9) AS upd_dttm
from source
)
select * from final            