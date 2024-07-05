with source as
(
    select * from {{ ref('jpndcledw_integration__teikikeiyaku_data_mart_uni_prev') }}
),

final as
(
    SELECT  
        a.*,
        DATEADD('day', - 2, CONVERT_TIMEZONE('Asia/Tokyo', 'UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)) AS prev1_insertdate
    FROM source a
)

select * from final