with source as
(
    select * from {{ source('jpdcledw_integration', 'dm_user_attr_sfcc_v_prev') }}
),

final as
(
    SELECT  
        a.*,
        DATEADD('day', - 2, CONVERT_TIMEZONE('Asia/Tokyo', 'UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)) AS prev1_insertdate
    FROM source a
)

select * from final