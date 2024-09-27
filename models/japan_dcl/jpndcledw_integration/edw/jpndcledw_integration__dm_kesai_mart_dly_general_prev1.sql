with source as
(
    select * from {{ source('jpdcledw_integration', 'dm_kesai_mart_dly_general_prev') }}
),

final as
(
    SELECT  
        a.*,
        DATEADD('day', - 2, CONVERT_TIMEZONE('Asia/Tokyo', 'UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)) AS prev1_insertdate
    FROM source a
    WHERE order_dt >= '1-JAN-2019'
)

select * from final