with source as
(
    select * from {{ source('jpndcledw_integration', 'kr_new_stage_point') }}
),

final as
(
    SELECT 
        yyyymm::VARCHAR(9) AS yyyymm,
        kokyano::VARCHAR(30) AS kokyano,
        stage::VARCHAR(18) AS stage,
        DATEADD('day', -1, CONVERT_TIMEZONE('Asia/Tokyo', 'UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)) AS prev_insertdate
    FROM source
)

select * from final