WITH kesai_h_data_mart_mv_kizuna
AS (
  SELECT *
  FROM {{ ref('jpndcledw_integration__kesai_h_data_mart_mv_kizuna') }}
  ),
transformed
AS (
    SELECT
        KOKYANO
        ,MIN(JUCHDATE) MINJUCHDATE1
        ,MAX(JUCHDATE) MAXJUCHDATE1
        ,SUM(CASE WHEN DAIHANROBUNNAME = '通販'         THEN 1       ELSE 0 
            END
            ) JUCHCNTTSUHAN
        ,SUM(CASE WHEN DAIHANROBUNNAME = 'Web'          THEN 1       ELSE 0 
            END
            ) JUCHCNTWEB
        ,SUM(CASE WHEN DAIHANROBUNNAME = '直営・百貨店' THEN 1       ELSE 0 
            END
            ) JUCHCNTSTORE
        ,SUM(CASE WHEN DAIHANROBUNNAME = '通販'         THEN SOGOKEI ELSE 0 
            END
            ) JUCHSUMTSUHAN
        ,SUM(CASE WHEN DAIHANROBUNNAME = 'Web'          THEN SOGOKEI ELSE 0 
            END
            ) JUCHSUMWEB
        ,SUM(CASE WHEN DAIHANROBUNNAME = '直営・百貨店' THEN SOGOKEI ELSE 0 
            END
            ) JUCHSUMSTORE
        ,TRUNC(AVG(SOGOKEI)) AMOUNTAVG
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
        minjuchdate1::number(38,18) as minjuchdate1,
        maxjuchdate1::number(38,18) as maxjuchdate1,
        juchcnttsuhan::number(38,18) as juchcnttsuhan,
        juchcntweb::number(38,18) as juchcntweb,
        juchcntstore::number(38,18) as juchcntstore,
        juchsumtsuhan::number(38,18) as juchsumtsuhan,
        juchsumweb::number(38,18) as juchsumweb,
        juchsumstore::number(38,18) as juchsumstore,
        amountavg::number(38,18) as amountavg,
        current_timestamp()::timestamp_ntz(9) AS inserted_date,
        inserted_by::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) AS updated_date,
        updated_by::varchar(100) as updated_by
FROM transformed
)
SELECT *
FROM final