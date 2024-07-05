with source as
(
    select * from {{ source('jpndcledw_integration', 'dm_user_attr') }}
),

final as
(
    SELECT  
        a.*,
        DATEADD('day', -1, CONVERT_TIMEZONE('Asia/Tokyo', 'UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)) AS prev_insertdate
    FROM source a
)

select * from final

