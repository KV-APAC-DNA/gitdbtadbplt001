with source as
(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.dm_user_attr_sfcc_v
),

transformed as
(
    SELECT  
        a.*,
        DATEADD('day', - 1, CONVERT_TIMEZONE('Asia/Tokyo', 'UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)) AS prev_insertdate
    FROM source a
)

select * from final