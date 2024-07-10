
--Import CTE
with edw_material_sales_dim as (
    select * from {{ ref( 'mysitg_integration__itg_my_material_dim') }}
)

select * from edw_material_sales_dim