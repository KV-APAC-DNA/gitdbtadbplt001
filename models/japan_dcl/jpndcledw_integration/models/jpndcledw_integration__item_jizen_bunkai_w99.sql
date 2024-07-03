WITH item_jizen_bunkai_w3
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w3') }}
	),

item_jizen_bunkai_w93
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w3') }}
	),

transformed
AS (
	SELECT 
		tm14.itemcode AS itemcode,
		tm14.kosecode AS kosecode,
		tm14.suryo AS suryo
	FROM item_jizen_bunkai_w3 tm14,
		item_jizen_bunkai_w93 t1
	WHERE tm14.itemcode = t1.itemcode
		AND (
			tm14.kosecode LIKE '0083%'
			OR tm14.kosecode LIKE '0084%'
			OR tm14.kosecode LIKE '0085%'
			)
	),

final
AS (
	SELECT 
		itemcode::VARCHAR(40) AS itemcode,
		kosecode::VARCHAR(40) AS kosecode,
		suryo::number(38, 4) AS suryo
	FROM transformed
	)

SELECT *
FROM final