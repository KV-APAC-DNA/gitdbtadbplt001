WITH item_jizen_bunkai_w91
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w91') }}
	),

item_jizen_bunkai_w92
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w92') }}
	),

transformed
AS (
	SELECT 
		t1.itemcode AS itemcode
	FROM 
		item_jizen_bunkai_w91 t1,
		item_jizen_bunkai_w92 t2
	WHERE 
		t1.itemcode = t2.itemcode
		AND t1.cnt <> t2.cnt
	),
final
AS (
	SELECT 
		itemcode::VARCHAR(40) AS itemcode,
	FROM transformed
	)
SELECT *
FROM final