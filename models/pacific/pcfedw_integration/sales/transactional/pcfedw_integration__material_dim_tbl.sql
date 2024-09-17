with vw_material_dim as(
    select * from {{ ref('pcfedw_integration__vw_material_dim') }}
),
transformed as 
(
    select * from vw_material_dim
)
select * from transformed