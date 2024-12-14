with d2c_data AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__dm_d2c_mon_net_sales_comp') }}  -- Reference to the previous model
),
net_sales_monthly AS (
SELECT 
    month_id AS "month_id",
    channel AS "channel",
    nts_base AS "nts_base",
    teikikeiyaku AS "teikikeiyaku",
    total_order AS "total_order",
    total_items AS "total_items",
    total_customer AS "total_customer"
FROM d2c_data 
)
SELECT *,
current_timestamp()::timestamp_ntz(9) as inserted_date,
null::varchar(100) as inserted_by ,
current_timestamp()::timestamp_ntz(9) as updated_date,
null::varchar(100) as updated_by
FROM net_sales_monthly