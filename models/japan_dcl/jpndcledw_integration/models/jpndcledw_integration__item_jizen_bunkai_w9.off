WITH item_jizen_bunkai_w16
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w16') }}
	),

item_jizen_bunkai_w10
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w10') }}
	),

item_jizen_bunkai_w8
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w8') }}
	),

transformed
AS (
	SELECT 
		bom.itemcode AS itemcode,
		w10.kosecode AS kosecode,
		w10.suryo AS suryo,
		bom.koseritsukei AS koseritsukei,
		bom.sa AS sa,
		w8.kosecode_cnt AS kosecode_cnt
	FROM item_jizen_bunkai_w16 bom
	INNER JOIN item_jizen_bunkai_w10 w10 ON bom.itemcode = w10.itemcode
	INNER JOIN item_jizen_bunkai_w8 w8 ON bom.itemcode = w8.itemcode
	),

final
AS (
	SELECT 
		itemcode::VARCHAR(20) AS itemcode,
		kosecode::VARCHAR(20) AS kosecode,
		suryo::DECIMAL(16, 8) AS suryo,
		koseritsukei::DECIMAL(16, 8) AS koseritsukei,
		sa::DECIMAL(16, 8) AS sa,
		kosecode_cnt::DECIMAL(16, 8) AS kosecode_cnt
	FROM transformed
	)
	
SELECT *
FROM final