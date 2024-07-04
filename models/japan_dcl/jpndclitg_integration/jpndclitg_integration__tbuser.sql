with source as(
    select * from {{ source('jpndclsdl_raw', 'tbuser') }} 
),
final as(
    select * from source
)
select * from final