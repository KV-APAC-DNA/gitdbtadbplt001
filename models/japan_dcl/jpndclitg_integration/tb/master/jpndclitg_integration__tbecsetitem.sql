with source as
(
    select * from {{ source('jpndclsdl_raw', 'tbecsetitem') }}
),


final as
(
    select * from source
)

select * from final