{{
    config(
        alias= "vw_stg_sdl_prod_hier",
        materialized= "view",
        tags= ["daily","sap_bw"]
    )
}}

--Import CTE
with source as (
    select * from {{ source('aspsdl_raw', 'sdl_prod_hier') }}
),

--Logical CTE

--Final CTE
final as (
    select * from source
)

--Final select
select * from final