with key_cohort_met AS (
    SELECT * 
    FROM {{ ref('jpndcledw_integration__dm_d2c_mon_key_cohort_met') }}
),
final AS (
    SELECT kokyano as "kokyano",
    month_id as "month_id",
    sales as "sales",
    birthday as "birthday",
    inserted_date as "inserted_date",
    inserted_by as "inserted_by",
    updated_date as "updated_date",
    updated_by as "updated_by"
    FROM key_cohort_met
)

select * from final
