WITH teikikeiyaku_data_mart_uni
AS (
    SELECT *
    FROM dev_dna_core.jpdcledw_integration.teikikeiyaku_data_mart_uni
  ),
transformed
AS (
    SELECT
        C_DIUSRID KOKYANO
        ,'Y' CONTRACT
        ,NULL INSERTED_BY
        ,NULL UPDATED_BY
    FROM   teikikeiyaku_data_mart_uni
    WHERE  --C_DSDELEVERYYM = TO_CHAR(SYSDATE,'YYYYMM')  	--Timezone conversion from UTC 20220211
        C_DSDELEVERYYM = TO_CHAR(CURRENT_TIMESTAMP()::DATE, 'YYYYMM')
        AND    KAIYAKU_KBN    = '有効'
    GROUP BY C_DIUSRID
  ),
final
AS (
    SELECT 
        kokyano::varchar(30) as kokyano,
        contract::varchar(1) as contract,
        current_timestamp()::timestamp_ntz(9) AS inserted_date,
        inserted_by::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) AS updated_date,
        updated_by::varchar(100) as updated_by
    FROM transformed
)
SELECT *
FROM final