{{
    config(
        alias= "vw_stg_sdl_sap_ecc_sales_office_text",
        materialized="view",
        tags=[""]
    )
}}

--import CTE
with sources as(
    select * from {{ source('aspsdl_raw', 'sdl_sap_ecc_sales_office_text') }}
),
--logical CTE
final as(
    select * from sources
)
--final select
select * from final
