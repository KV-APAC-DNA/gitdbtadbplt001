{{
    config(
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_SMALL_WH")+ ";",
    )
}}
with d2c_data AS (
    SELECT *,
        CASE
            WHEN channel = '通販' THEN 'E-Commerce'
            WHEN channel = '直営・百貨店' THEN 'Direct Store & Department Store'
            ELSE channel  -- Keep original value if no match
        END AS channel_english
    FROM {{ ref('jpndcledw_integration__dm_dcl_sales_mon_w_target') }}
),
FINAL as (
SELECT 
    month_id AS "month_id",
    gts_base AS "gts_base",
    gts_mom AS "gts_mom",
    gts_yoy AS "gts_yoy",
    nts_base AS "nts_base",
    nts_mom AS "nts_mom",
    nts_yoy AS "nts_yoy",
    channel_english AS "channel",
    gts_target AS "gts_target",
    nts_target AS "nts_target",
    inserted_date AS "inserted_date" ,
    inserted_by AS "inserted_by" ,
    updated_date AS "updated_date",
    updated_by AS "updated_by"
FROM d2c_data
)

SELECT * ,
current_timestamp()::timestamp_ntz(9) as inserted_date,
null::varchar(100) as inserted_by ,
current_timestamp()::timestamp_ntz(9) as updated_date,
null::varchar(100) as updated_by
FROM FINAL
