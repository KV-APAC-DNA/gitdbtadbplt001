with source as(
    select * from {{ source('jpdclsdl_raw', 'c_tbecpriroute') }}
),
final as(
    select * from source
)
select * from final