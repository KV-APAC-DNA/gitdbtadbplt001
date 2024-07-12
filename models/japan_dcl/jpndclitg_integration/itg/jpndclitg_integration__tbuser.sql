with source as(
    select * from {{ source('jpdclsdl_raw', 'tbuser') }} 
),
final as(
    select * from source
)
select * from final