WITH kesai_h_data_mart_mv_kizuna
AS (
  SELECT *
  FROM {{ ref('jpndcledw_integration__kesai_h_data_mart_mv_kizuna') }}
  ),
transformed
AS (
    SELECT KOKYANO
      ,SUM(SOGOKEI) AMOUNTSUMALL
      ,SUM(CASE WHEN SHUKADATE BETWEEN TO_NUMBER( TO_CHAR(DATEADD('day', -365*3-1, CURRENT_TIMESTAMP())::DATE, 'YYYYMMDD') ,'99999999') AND TO_NUMBER( TO_CHAR(DATEADD('day', -1, CURRENT_TIMESTAMP())::DATE, 'YYYYMMDD'),'99999999') THEN SOGOKEI
                ELSE 0
           END )    AMOUNTSUM30
      ,SUM(CASE WHEN SHUKADATE BETWEEN TO_NUMBER( TO_CHAR(DATEADD('day', -83*3-1, CURRENT_TIMESTAMP())::DATE, 'YYYYMMDD'),'99999999') AND TO_NUMBER( TO_CHAR(DATEADD('day', -1, CURRENT_TIMESTAMP())::DATE, 'YYYYMMDD'),'99999999') THEN SOGOKEI
                ELSE 0
           END )    AMOUNTSUM25
      ,SUM(CASE WHEN SHUKADATE BETWEEN TO_NUMBER( TO_CHAR(DATEADD('day', -365*2-1, CURRENT_TIMESTAMP())::DATE, 'YYYYMMDD'),'99999999') AND TO_NUMBER( TO_CHAR(DATEADD('day', -1, CURRENT_TIMESTAMP())::DATE, 'YYYYMMDD'),'99999999') THEN SOGOKEI
                ELSE 0
           END )    AMOUNTSUM20
      ,SUM(CASE WHEN SHUKADATE BETWEEN TO_NUMBER( TO_CHAR(DATEADD('day', -83*2-1, CURRENT_TIMESTAMP())::DATE, 'YYYYMMDD'),'99999999') AND TO_NUMBER( TO_CHAR(DATEADD('day', -1, CURRENT_TIMESTAMP())::DATE, 'YYYYMMDD'),'99999999') THEN SOGOKEI
                ELSE 0
           END )    AMOUNTSUM15
      ,SUM(CASE WHEN SHUKADATE BETWEEN TO_NUMBER( TO_CHAR(DATEADD('day', -365*1-1, CURRENT_TIMESTAMP())::DATE, 'YYYYMMDD'),'99999999') AND TO_NUMBER( TO_CHAR(DATEADD('day', -1, CURRENT_TIMESTAMP())::DATE, 'YYYYMMDD'),'99999999') THEN SOGOKEI
                ELSE 0
           END )    AMOUNTSUM10
      ,SUM(CASE WHEN SHUKADATE BETWEEN TO_NUMBER( TO_CHAR(DATEADD('day', -83*1-1, CURRENT_TIMESTAMP())::DATE, 'YYYYMMDD') ,'99999999') AND TO_NUMBER(TO_CHAR(DATEADD('day', -1, CURRENT_TIMESTAMP())::DATE, 'YYYYMMDD'),'99999999') THEN SOGOKEI
                ELSE 0
           END )    AMOUNTSUM05
      ,SUM(CASE WHEN SORYO > 0 THEN 1
                ELSE 0
           END )    SHIPPINGFEECNT
      ,NULL INSERTED_BY
      ,NULL UPDATED_BY
    FROM   kesai_h_data_mart_mv_kizuna
    WHERE 1 = 1
    AND   SOGOKEI > 0
    GROUP BY KOKYANO
),
final
AS (
  SELECT 
        kokyano::varchar(30) as kokyano,
        amountsumall::number(38,18) as amountsumall,
        amountsum30::number(38,18) as amountsum30,
        amountsum25::number(38,18) as amountsum25,
        amountsum20::number(38,18) as amountsum20,
        amountsum15::number(38,18) as amountsum15,
        amountsum10::number(38,18) as amountsum10,
        amountsum05::number(38,18) as amountsum05,
        shippingfeecnt::number(38,18) as shippingfeecnt,
        current_timestamp()::timestamp_ntz(9) AS inserted_date,
        inserted_by::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) AS updated_date,
        updated_by::varchar(100) as updated_by
FROM transformed
)
SELECT *
FROM final