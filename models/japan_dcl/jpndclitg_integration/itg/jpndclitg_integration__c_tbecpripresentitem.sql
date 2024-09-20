with source as(
    select * from {{ source('jpdclsdl_raw', 'c_tbecpripresentitem') }}
),
final as(
    select * from source
)
select * from final