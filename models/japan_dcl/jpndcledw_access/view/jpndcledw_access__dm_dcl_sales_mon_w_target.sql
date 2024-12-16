with d2c_data AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__dm_dcl_sales_mon_w_target') }}
),
FINAL as (
SELECT 
    month_id AS "month id",
    gts_base AS "gts base",
    gts_mom AS "gts mom",
    gts_yoy AS "gts yoy",
    nts_base AS "nts base",
    nts_mom AS "nts mom",
    nts_yoy AS "nts yoy",
    channel AS "channel",
    target_gts AS "target gts",
    target_nts AS "target nts",
    inserted_date AS "inserted_date" ,
    inserted_by AS "inserted_by" ,
    updated_date AS "updated_date",
    updated_by AS "updated_by"
FROM d2c_data
)

SELECT * FROM FINAL