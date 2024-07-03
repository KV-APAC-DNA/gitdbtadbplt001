WITH item_jizen_bunkai_w06
AS 
(
	SELECT *
	FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w06') }}
),

item_jizen_bunkai_w3
AS 
(
	SELECT *
	FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w3') }}
),

transformed
AS 
(
	SELECT 
		a.itemcode AS itemcode,
		a.kosecode AS kosecode,
		a.koseritsu AS koseritsu,
		b.suryo AS suryo
	FROM item_jizen_bunkai_w06 a,
		item_jizen_bunkai_w3 b
	WHERE a.itemcode = b.itemcode
		AND a.kosecode = b.kosecode
),

final
AS 
(
	SELECT 
		itemcode::VARCHAR(20) AS itemcode,
		kosecode::VARCHAR(20) AS kosecode,
		koseritsu::DECIMAL(16, 8) AS koseritsu,
		suryo::DECIMAL(16, 8) AS suryo
	FROM transformed
)
SELECT *
FROM final