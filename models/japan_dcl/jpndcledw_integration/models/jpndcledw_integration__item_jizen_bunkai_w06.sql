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

item_jizen_bunkai_w7
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w7') }}
	),

transformed
AS (
	SELECT bom.itemcode AS itemcode,
		bom.kosecode AS kosecode,
		(
			CASE 
				WHEN zaiko.tanka = 0
					THEN CASE 
							WHEN w7.kotankasum = 0
								THEN 0
							ELSE round(bom.suryo * zaiko2.tanka / w7.kotankasum::DECIMAL, 8)
							END
				ELSE round(bom.suryo * zaiko2.tanka / zaiko.tanka::DECIMAL, 8)
				END
			) AS koseritsu
	FROM item_jizen_bunkai_w3 bom
	LEFT JOIN item_jizen_bunkai_wz zaiko ON bom.itemcode = zaiko.itemcode
	LEFT JOIN item_jizen_bunkai_wz zaiko2 ON bom.kosecode = zaiko2.itemcode
	LEFT JOIN item_jizen_bunkai_w7 w7 ON bom.itemcode = w7.itemcode
	),

final
AS (
	SELECT itemcode::VARCHAR(20) AS itemcode,
		kosecode::VARCHAR(20) AS kosecode,
		koseritsu::number(16, 8) AS koseritsu
	FROM transformed
	)

SELECT *
FROM final