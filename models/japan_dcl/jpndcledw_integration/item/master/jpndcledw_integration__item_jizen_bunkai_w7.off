WITH item_jizen_bunkai_w3
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w3') }}
	),
item_jizen_bunkai_wz
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_wz') }}
	),
transformed
AS (
	SELECT t.itemcode AS itemcode,
		sum(zaiko.tanka * t.suryo) AS kotankasum
	FROM item_jizen_bunkai_w3 t
	LEFT OUTER JOIN item_jizen_bunkai_wz zaiko ON t.kosecode = zaiko.itemcode
	GROUP BY t.itemcode
	),
final
AS (
	SELECT itemcode::VARCHAR(20) AS itemcode,
		kotankasum::DECIMAL(16, 8) AS kotankasum
	FROM transformed
	)
SELECT *
FROM final