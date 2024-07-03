WITH item_jizen_bunkai_w06
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w06') }}
	),

transformed
AS (
	SELECT 
		t.itemcode AS itemcode,
		s.kosecodemax AS kosecode,
		t.koseritsumax AS koseritsu
	FROM (
		SELECT 
			t1.itemcode AS itemcode,
			max(t1.koseritsu) AS koseritsumax
		FROM item_jizen_bunkai_w06 t1
		GROUP BY t1.itemcode
		) t
	JOIN (
		SELECT 
			s1.itemcode AS itemcode,
			s1.koseritsu AS koseritsu,
			max(s1.kosecode) AS kosecodemax
		FROM item_jizen_bunkai_w06 s1
		GROUP BY s1.itemcode,
			s1.koseritsu
		) s ON t.itemcode = s.itemcode
		AND t.koseritsumax = s.koseritsu
	),

final
AS (
	SELECT 
		itemcode::VARCHAR(20) AS itemcode,
		kosecode::VARCHAR(20) AS kosecode,
		koseritsu::DECIMAL(16, 8) AS koseritsu
	FROM transformed
	)
    
SELECT *
FROM final