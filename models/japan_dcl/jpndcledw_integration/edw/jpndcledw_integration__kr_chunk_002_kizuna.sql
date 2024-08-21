WITH kesai_h_data_mart_mv_kizuna
AS (
  SELECT *
  FROM  {{ ref('jpndcledw_integration__kesai_h_data_mart_mv') }}
  ),
transformed
AS (
    SELECT
        KOKYANO
        ,MIN(JUCHDATE) MINJUCHDATE
        ,NULL INSERTED_BY
        ,NULL UPDATED_BY
    FROM   kesai_h_data_mart_mv_kizuna
    WHERE 1 = 1
    AND   SOGOKEI = 0
    AND   MAKER <> '3'    --3:調整行DUMMY は除外
    AND   JUCHKBN NOT IN ('90','91','92')    --90,91,92:返品データ は除外
    GROUP BY KOKYANO
  ),
final
AS (
  SELECT 
        kokyano::varchar(30) as kokyano,
        minjuchdate::number(38,18) as minjuchdate,
        current_timestamp()::timestamp_ntz(9) AS inserted_date,
        inserted_by::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) AS updated_date,
        updated_by::varchar(100) as updated_by
FROM transformed
)
SELECT *
FROM final