WITH itg_isight_sector_mapping
AS (
    SELECT *
    FROM {{ ref('hcposeitg_integration__isight_sector_mapping') }}
    ),
trns
AS (
    SELECT country,
        company,
        division,
        sector,
        sysdate() AS inserted_date,
        sysdate() AS updated_date
    FROM itg_isight_sector_mapping
    ),
final
AS (
    SELECT country::VARCHAR(256) AS country,
        company::VARCHAR(256) AS company,
        division::VARCHAR(256) AS division,
        sector::VARCHAR(256) AS sector,
        inserted_date::timestamp_ntz(9) AS inserted_date,
        updated_date::timestamp_ntz(9) AS updated_date
    FROM trns
    )
SELECT *
FROM final