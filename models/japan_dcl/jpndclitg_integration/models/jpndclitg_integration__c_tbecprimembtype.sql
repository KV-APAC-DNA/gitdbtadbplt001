with source as
(
    select * from {{ source('jpdclsdl_raw', 'c_tbecprimembtype') }}
),


final as
(
    select * from source
)

select * from final