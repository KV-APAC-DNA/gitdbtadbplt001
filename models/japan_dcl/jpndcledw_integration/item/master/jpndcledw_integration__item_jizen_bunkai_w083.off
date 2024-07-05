WITH item_jizen_bunkai_wend
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_wend') }}
	),

transformed
AS (
	SELECT 
		t.itemcode AS itemcode,
		sum(t.koseritsu) AS koseritsukei,
		1 - sum(t.koseritsu) AS sa
	FROM item_jizen_bunkai_wend t
	WHERE t.koseritsu <> 0
	GROUP BY t.itemcode
	HAVING sum(t.koseritsu) <> 1
	),

final
AS (
	SELECT 
		itemcode::VARCHAR(20) AS itemcode,
		koseritsukei::DECIMAL(16, 8) AS koseritsukei,
		sa::DECIMAL(16, 8) AS sa
	FROM transformed
	)

SELECT *
FROM final