with purchase_fnl AS (
    SELECT * 
    FROM {{ ref('jpndcledw_integration__dm_d2c_mon_purchase_fnl') }}
),
final AS (
SELECT month_id as "month_id", 
birthday_yearmonth as "birthday_yearmonth", 
f1 as "f1", 
f2 as "f2", 
f3 as "f3",
f4 as "f4", 
f5 as "f5", 
"f6+" as "f6+",    
inserted_date as "inserted_date",
inserted_by as "inserted_by",
updated_date as "updated_date",
updated_by as "updated_by"
FROM purchase_fnl
)

select * from final