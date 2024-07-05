WITH item_jizen_bunkai_w1
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w1') }}
	),
transformed
AS (
	SELECT 2 AS kaisou,
		t1.itemcode AS itemcode,
		t1.kosecode AS kosecode,
		t1.suryo AS suryo
	FROM item_jizen_bunkai_w1 t1
	
	UNION ALL
	
	SELECT 3 AS kaisou,
		t1.itemcode AS itemcode,
		t2.kosecode AS kosecode,
		round(t1.suryo * t2.suryo, 4) AS suryo
	FROM item_jizen_bunkai_w1 t1
	JOIN item_jizen_bunkai_w1 t2 ON t1.kosecode = t2.itemcode
	
	UNION ALL
	
	SELECT 4 AS kaisou,
		t1.itemcode AS itemcode,
		t3.kosecode AS kosecode,
		round(t1.suryo * t2.suryo * t3.suryo, 4) AS suryo
	FROM item_jizen_bunkai_w1 t1
	JOIN item_jizen_bunkai_w1 t2 ON t1.kosecode = t2.itemcode
	JOIN item_jizen_bunkai_w1 t3 ON t2.kosecode = t3.itemcode
	
	UNION ALL
	
	SELECT 5 AS kaisou,
		t1.itemcode AS itemcode,
		t4.kosecode AS kosecode,
		round(t1.suryo * t2.suryo * t3.suryo * t4.suryo, 4) AS suryo
	FROM item_jizen_bunkai_w1 t1
	JOIN item_jizen_bunkai_w1 t2 ON t1.kosecode = t2.itemcode
	JOIN item_jizen_bunkai_w1 t3 ON t2.kosecode = t3.itemcode
	JOIN item_jizen_bunkai_w1 t4 ON t3.kosecode = t4.itemcode
	
	UNION ALL
	
	SELECT 6 AS kaisou,
		t1.itemcode AS itemcode,
		t5.kosecode AS kosecode,
		round(t1.suryo * t2.suryo * t3.suryo * t4.suryo * t5.suryo, 4) AS suryo
	FROM item_jizen_bunkai_w1 t1
	JOIN item_jizen_bunkai_w1 t2 ON t1.kosecode = t2.itemcode
	JOIN item_jizen_bunkai_w1 t3 ON t2.kosecode = t3.itemcode
	JOIN item_jizen_bunkai_w1 t4 ON t3.kosecode = t4.itemcode
	JOIN item_jizen_bunkai_w1 t5 ON t4.kosecode = t5.itemcode
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