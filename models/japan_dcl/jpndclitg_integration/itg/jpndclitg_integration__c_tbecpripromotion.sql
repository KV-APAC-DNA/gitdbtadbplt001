with source as
(
     select * from {{ source('jpdclsdl_raw', 'c_tbecpripromotion') }}
),


final as
(
    select * from source
)

select * from final