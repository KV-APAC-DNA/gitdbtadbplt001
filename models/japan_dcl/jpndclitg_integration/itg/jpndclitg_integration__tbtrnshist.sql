with source as(
    select * from {{ source('jpdclsdl_raw', 'tbtrnshist') }} 
),
final as(
    select * from source
)
select * from final