with source as(
    select * from {{ source('jpndclsdl_raw', 'tbpromotioncate') }} 
),
final as(
    select * from source
)
select * from final