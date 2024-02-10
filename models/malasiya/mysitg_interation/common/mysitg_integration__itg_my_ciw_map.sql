with source as(
    select * from {{ source('mysitg_integration', 'itg_my_ciw_map') }}
),
final as (
    select * from source
)
select * from final