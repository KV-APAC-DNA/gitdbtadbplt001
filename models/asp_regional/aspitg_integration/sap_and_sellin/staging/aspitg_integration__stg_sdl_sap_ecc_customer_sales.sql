{{
    config(
        alias= "sdl_sap_ecc_customer_sales",
        materialized="view",
        tags=[""]
    )
}}


with sources as(
    select * from {{ source('aspsdl_raw', 'sdl_sap_ecc_customer_sales') }}
),

final as(
    select * from sources
)

select * from final

