with 
source as
(
    select * from {{ source('aspitg_integration','itg_pop6_pop_lists_temp') }}
),
final as
(
    select * from source
)
select * from final