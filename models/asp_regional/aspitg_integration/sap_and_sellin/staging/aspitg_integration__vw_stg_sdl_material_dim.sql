{{
    config(
        alias= "vw_stg_sdl_material_dim",
        materialized= "view",
        tags= ["daily","sap_bw"]
    )
}}

--Import CTE
with source as (
    select * from {{ source('aspsdl_raw', 'sdl_material_dim') }}
),

--Logical CTE

--Final CTE
final as (
    select * from source
    where matl_num !='|'
)

--Final select
select * from final