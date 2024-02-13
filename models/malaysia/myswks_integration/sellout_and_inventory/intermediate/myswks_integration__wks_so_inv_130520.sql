with source as
(
    select  * from {{ source('snaposewks_integration','wks_so_inv_130520') }}
),

final as (
    select * from source
)

select * from final