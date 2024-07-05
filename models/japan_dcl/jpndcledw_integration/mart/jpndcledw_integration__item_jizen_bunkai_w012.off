WITH item_jizen_bunkai_wend
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_wend') }}
	),

transformed
AS (
	SELECT 
		itemcode AS itemcode,
		sum(suryo) AS suryo
	FROM item_jizen_bunkai_wend
	GROUP BY itemcode
	HAVING sum(koseritsu) = 0
	),

final
AS (
	SELECT 
        itemcode::VARCHAR(20) AS itemcode,
		suryo::number(38, 8) AS suryo
	FROM transformed
	)

SELECT *
FROM final