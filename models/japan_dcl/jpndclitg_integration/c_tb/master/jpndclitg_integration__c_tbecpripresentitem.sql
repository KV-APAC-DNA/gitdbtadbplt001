with source as(
    select * from {{ source('jpndclsdl_raw', 'c_tbecpripresentitem') }}
),
final as(
    select * from source
)
select * from final