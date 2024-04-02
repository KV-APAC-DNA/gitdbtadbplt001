{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('vnmsdl_raw','sdl_spiral_mti_offtake') }}
),
final as(
    select 
            stt,
            area,
            channelname,
            customername,
            shopcode,
            shopname,
            address,
            supcode,
            supname,
            year,
            month,
            barcode,
            productname,
            franchise,
            brand,
            cate,
            sub_cat,
            sub_brand,
            size,
            quantity,
            amount,
            amountusd,
            file_name,
            current_timestamp() as crtd_name 
from source
 
)

select * from final