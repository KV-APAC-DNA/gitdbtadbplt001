WITH item_jizen_bunkai_w3
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w3') }}
	),
transformed
AS (
	SELECT tm14.itemcode AS itemcode,
		count(*) AS cnt
	FROM item_jizen_bunkai_w3 tm14
	GROUP BY tm14.itemcode
	),
final
AS (
	SELECT itemcode::VARCHAR(40) AS itemcode,
		cnt::number(38, 0) AS cnt
	FROM transformed
	)
SELECT *
FROM final