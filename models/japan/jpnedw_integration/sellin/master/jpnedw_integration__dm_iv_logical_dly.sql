with source as
(
    select * from {{ source('jpnedw_integration', 'dm_iv_logical_dly') }}
),

final as
(
    select * from source
)

select * from final