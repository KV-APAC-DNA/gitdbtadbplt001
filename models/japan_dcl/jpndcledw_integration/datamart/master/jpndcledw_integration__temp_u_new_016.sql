with temp_kesai_016 as(
    select * from {{ ref('jpndcledw_integration__temp_kesai_016') }} 
),
cld_m as(
    select * from {{ source('jpndcledw_integration', 'cld_m') }}
),
transformed as(
SELECT kokyano,
	x.year AS first_order_year,
	y.year_445 AS first_ship_year,
	first_order_dt,
	first_ship_dt
FROM (
	SELECT DISTINCT kokyano,
		min(order_dt) AS first_order_dt,
		min(ship_dt) AS first_ship_dt
	FROM temp_kesai_016
	GROUP BY kokyano
	)
LEFT JOIN cld_m x ON first_order_dt = x.ymd_dt
LEFT JOIN cld_m y ON first_ship_dt = y.ymd_dt
),
final as(
    select
        kokyano::varchar(60) as kokyano,
        first_order_year::varchar(4) as first_order_year,
        first_ship_year::varchar(256) as first_ship_year,
        to_date(first_order_dt) as first_order_dt,
        to_date(first_ship_dt) as first_ship_dt
    from transformed
)
select * from final
