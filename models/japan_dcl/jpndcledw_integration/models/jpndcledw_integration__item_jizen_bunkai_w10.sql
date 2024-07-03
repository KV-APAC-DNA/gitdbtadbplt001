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
item_jizen_bunkai_w11
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w11') }}
	),
transformed
AS (
	SELECT bom.itemcode AS itemcode,
		bom.kosecode AS kosecode,
		bom.suryo AS suryo
	FROM item_jizen_bunkai_w3 bom
	LEFT JOIN item_jizen_bunkai_wz zaiko2 ON bom.kosecode = zaiko2.itemcode
	WHERE (bom.suryo * zaiko2.tanka) = 0
		AND bom.itemcode NOT IN (
			SELECT itemcode
			FROM item_jizen_bunkai_w11
			WHERE sa < 0
			)
	),
final
AS (
	SELECT itemcode::VARCHAR(40) AS itemcode,
		kosecode::VARCHAR(40) AS kosecode,
		suryo::number(38, 4) AS suryo
	FROM transformed
	)
SELECT *
FROM final