{{
    config(
        alias= "vw_stg_sdl_sap_bw_needstates_text",
        materialized= "view",
        tags= ["daily","sap_bw"]
    )
}}
--Import CTE
with source as (
    select * from {{ source('aspsdl_raw', 'sdl_sap_bw_needstates_text') }}
),

--Logical CTE

--Final CTE
final as (
    select * from source
)

--Final select
select * from final
