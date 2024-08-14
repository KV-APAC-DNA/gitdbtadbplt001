with source as(
    select * from {{ source('jpdclsdl_raw', 'tbpromotioncate') }} 
),
final as(
    select * from source
)
select * from final