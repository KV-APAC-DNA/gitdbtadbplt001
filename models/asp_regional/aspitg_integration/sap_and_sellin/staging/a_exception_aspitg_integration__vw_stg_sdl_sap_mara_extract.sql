{{
    config(
        alias= "vw_stg_sdl_sap_mara_extract",
        materialized= "view",
        tags= ["daily","sap_ecc"]
    )
}}

--Import CTE
with source as (
    select * from aspwks_integration.wks_sap_mara_extract
),

--Logical CTE

--Final CTE
final as (
    select * from source
)

--Final select
select * from final