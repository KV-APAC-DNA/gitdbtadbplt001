with source as
(
    select * from {{ source('jpdclsdl_raw', 'contact_hist') }}
),


final as
(
    select * from source
)

select * from final