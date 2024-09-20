with source as
(
    select * from  {{ ref('jpndcledw_integration__teikikeiyaku_data_mart_uni') }}
),

final as
(
    SELECT  
        a.*,
        DATEADD('day', - 1, CONVERT_TIMEZONE('Asia/Tokyo', 'UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)) AS prev_insertdate
    FROM source a
)

select * from final