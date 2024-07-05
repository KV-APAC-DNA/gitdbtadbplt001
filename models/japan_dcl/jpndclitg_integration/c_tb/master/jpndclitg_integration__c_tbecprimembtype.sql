with source as
(
    select * from {{ source('jpndclsdl_raw', 'c_tbecprimembtype') }}
),


final as
(
    select * from source
)

select * from final