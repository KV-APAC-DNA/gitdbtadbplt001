with source as
(
    -- select * from dev_dna_core.snapjpdcledw_integration.DM_USER_ATTR
    select * from {{ source('jpndcledw', 'jpndcledw_integration__DM_USER_ATTR') }}
),

final as
(
    SELECT  
        a.*,
        DATEADD('day', -1, CONVERT_TIMEZONE('Asia/Tokyo', 'UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)) AS prev_insertdate
    FROM source a
)

select * from final

