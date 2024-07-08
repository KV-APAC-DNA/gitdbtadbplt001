WITH sources
AS (
    SELECT *
    FROM {{source('myssdl_raw', 'sdl_mds_my_ps_weights') }}
    ),
final
AS (
    SELECT kpi::VARCHAR(100) AS kpi,
        name::VARCHAR(255) AS retail_env,
        weight::NUMBER(20,4) AS weight,
        channel::VARCHAR(50) AS channel,
        convert_timezone('Asia/Singapore',current_timestamp()::timestamp_ntz(9)) as crtd_dttm
    FROM sources
    )
SELECT *
FROM final