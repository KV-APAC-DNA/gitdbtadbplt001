with source as
(
    select  * from {{ source('snaposewks_integration','wks_so_inv_131167') }}
),

final as (
    select * from source
)

select * from final