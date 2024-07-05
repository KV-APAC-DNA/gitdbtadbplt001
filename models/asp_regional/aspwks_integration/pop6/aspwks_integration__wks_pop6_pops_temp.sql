with 
source as
(
    select * from {{ source('aspitg_integration', 'itg_pop6_pops_temp') }}
),
final as
(
    select * from source
)
select * from final