--Import CTE
with source as (
    select * from {{ source('aspsdl_raw', 'sdl_material_pka_mara_extract') }}
),

--Logical CTE

--Final CTE
final as (
select 
* from source
)

--Final select
select * from final
