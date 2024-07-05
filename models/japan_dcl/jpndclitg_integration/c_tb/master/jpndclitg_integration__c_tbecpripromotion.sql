with source as
(
     select * from {{ source('jpndclsdl_raw', 'c_tbecpripromotion') }}
),


final as
(
    select * from source
)

select * from final