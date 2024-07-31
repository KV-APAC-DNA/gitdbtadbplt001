with source as
(
    select * from {{ source('jpnedw_integration', 'mt_constant_range') }}
),

final as
(
    select * from source
)

select * from final