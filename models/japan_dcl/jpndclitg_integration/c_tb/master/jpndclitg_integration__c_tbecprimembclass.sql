with source as(
    select * from {{ source('jpndclsdl_raw', 'c_tbecprimembclass') }}
),
final as(
    select * from source
)
select * from final