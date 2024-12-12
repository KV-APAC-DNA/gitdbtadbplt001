with d2c_data AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__dm_dcl_mon_net_sales_comp') }}  -- Reference to the previous model
),
net_sales_monthly AS (
SELECT 
    month_id AS "month_id",
    "channel" AS "channel",
    nts_base AS "nts_base",
    "saleno" AS "saleno",
    "kokyano" AS "kokyano",
    subscription_count AS "subscription_count",
    regular_count AS "regular_count"
FROM d2c_data 
)
SELECT *,
current_timestamp()::timestamp_ntz(9) as inserted_date,
null::varchar(100) as inserted_by ,
current_timestamp()::timestamp_ntz(9) as updated_date,
null::varchar(100) as updated_by
FROM net_sales_monthly