{{
    config(
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_SMALL_WH")+ ";",
    )
}}
with d2c_data AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__dm_dcl_sales_mon_w_target') }}  -- Reference to the previous model
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
    "channel" AS "channel",
    gts_target AS "gts_target",
    nts_target AS "nts_target",
    inserted_date AS "inserted_date" ,
    inserted_by AS "inserted_by" ,
    updated_date AS "updated_date",
    updated_by AS "updated_by"
FROM d2c_data
)

SELECT * FROM FINAL

