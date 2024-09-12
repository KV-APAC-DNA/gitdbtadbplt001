WITH itg_isight_licenses
AS (
    SELECT *
    FROM {{ ref('hcposeitg_integration__itg_isight_licenses') }}
    ),
trns
AS (
    SELECT year,
        country,
        sector,
        qty,
        licensetype,
        current_timestamp() AS inserted_date,
        current_timestamp() AS updated_date
    FROM itg_isight_licenses
    ),
final
AS (
    SELECT year::number(18, 0) AS year,
        country::VARCHAR(255) AS country,
        sector::VARCHAR(255) AS sector,
        qty::number(18, 0) AS qty,
        licensetype::VARCHAR(255) AS licensetype,
        inserted_date::timestamp_ntz(9) AS inserted_date,
        updated_date::timestamp_ntz(9) AS updated_date
    FROM trns
    )
SELECT *
FROM final