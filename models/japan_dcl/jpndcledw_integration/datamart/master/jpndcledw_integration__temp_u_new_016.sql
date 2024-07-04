with temp_kesai_016 as(
    select * from SNAPJPDCLEDW_INTEGRATION.temp_kesai_016
),
cld_m as(
    select * from SNAPJPDCLEDW_INTEGRATION.cld_m
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
)
select * from transformed
