
--Import CTE
with source as (
    select * from {{ source('aspsdl_raw', 'sdl_sap_ecc_material_base') }}
),

--Logical CTE

--Final CTE
final as (
    select * from source
)

--Final select
select * from final