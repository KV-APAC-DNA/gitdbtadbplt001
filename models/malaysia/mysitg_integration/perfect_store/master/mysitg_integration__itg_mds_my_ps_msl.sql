WITH sources
AS (
    SELECT *
    FROM {{source('myssdl_raw', 'sdl_mds_my_ps_msl') }}
    ),
final
AS (
    SELECT name::VARCHAR(255) as name,
        ean::VARCHAR(100) as ean,
        product_name::VARCHAR(255) as product_name,
        convert_timezone('Asia/Singapore',current_timestamp()::timestamp_ntz(9)) as crtd_dttm,
        valid_from::timestamp_ntz(9) as valid_from,
        valid_to::timestamp_ntz(9) as valid_to
    FROM sources
    )
SELECT *
FROM final