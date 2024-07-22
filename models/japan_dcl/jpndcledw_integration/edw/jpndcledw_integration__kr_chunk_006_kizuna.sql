WITH teikikeiyaku_data_mart_uni
AS (
  SELECT *
  FROM dev_dna_core.snapjpdcledw_integration.teikikeiyaku_data_mart_uni
  ),
transformed
AS (
  SELECT
         C_DIUSRID KOKYANO
        ,MIN(SHOKAI_YM) CONTRACT_FSTYM
        ,NULL INSERTED_BY
        ,NULL UPDATED_BY
    FROM   teikikeiyaku_data_mart_uni
    WHERE  1 = 1
    GROUP BY C_DIUSRID
  ),
final
AS (
  SELECT 
        kokyano::varchar(30) as kokyano,
        contract_fstym::varchar(9) as contract_fstym,
        current_timestamp()::timestamp_ntz(9) AS inserted_date,
        inserted_by::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) AS updated_date,
        updated_by::varchar(100) as updated_by
FROM transformed
)
SELECT *
FROM final