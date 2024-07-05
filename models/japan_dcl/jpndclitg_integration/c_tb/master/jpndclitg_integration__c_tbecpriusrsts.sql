with source as(
    select * from {{ source('jpndclsdl_raw', 'c_tbecpriusrsts') }}
),
final as(
    select * from source
)
select * from final