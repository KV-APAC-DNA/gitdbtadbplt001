{{
    config(
        alias= "vw_stg_sdl_sap_ecc_sales_group_text",
        materialized="view",
        tags=["sap_ecc"]
    )
}}

--import CTE
with sources as(
    select * from {{ source('aspsdl_raw', 'sdl_sap_ecc_sales_group_text') }}
),
--logical CTE
final as(
    select * from sources
)
--final select
select * from final
