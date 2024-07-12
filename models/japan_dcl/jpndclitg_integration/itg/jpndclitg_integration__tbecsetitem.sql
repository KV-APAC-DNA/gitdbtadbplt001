with source as
(
    select * from {{ source('jpdclsdl_raw', 'tbecsetitem') }}
),


final as
(
    select * from source
)

select * from final