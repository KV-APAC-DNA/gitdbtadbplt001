{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key = ["business_unit","recording_date","stock_type","product_code","product_name","batch","plant_code"]
        )
}}
--Import CTE
with source as 
(
    select * from {{ source('vnmsdl_raw', 'sdl_vn_dksh_sdl_otc') }}
      where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dksh_otc__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dksh_otc__duplicate_test')}}
        union all 
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dksh_otc__date_format_test')}}
        )
),  

final as 
(
Select      BUSINESS_UNIT::VARCHAR(100) as business_unit,
            DATE::VARCHAR(100) as recording_date ,
            STOCK_TYPE::VARCHAR(100) as stock_type,
            PRODUCT_CODE::VARCHAR(100) as product_code,
            PRODUCT_NAME::VARCHAR(100) as product_name,
            BATCH::VARCHAR(100) as batch,
            EXPIRY_DATE::VARCHAR(100) as expiry_date,
            BASE_UOM::VARCHAR(100) as base_uom,
            SHELF_LIFE::VARCHAR(100) as shelf_life,
            COGS::VARCHAR(100) as cogs,
            REGION::VARCHAR(100) as region,
            QUANTITY::NUMBER(20,0) as quantity,
            VALUE::VARCHAR(100) as value,
            PLANT_CODE::VARCHAR(100) as plant_code,
            PLANT_NAME::VARCHAR(100) as plant_name,
            SYSLOT::VARCHAR(100) as syslot,
            convert_timezone('Asia/Singapore',current_timestamp)::timestamp_ntz(9) AS crt_dttm,
            convert_timezone('Asia/Singapore',current_timestamp)::timestamp_ntz(9) AS upd_dttm
from source
)
select * from final            