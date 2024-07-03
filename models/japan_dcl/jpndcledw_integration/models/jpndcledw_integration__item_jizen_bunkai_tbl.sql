WITH item_jizen_bunkai_wend1
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_wend1') }}
	),

transformed
AS (
	SELECT 
		w14.itemcode as itemcode,
		w14.kosecode as kosecode,
		w14.suryo as suryo,
		w14.koseritsu as koseritsu,
		cast(to_char(current_timestamp(), 'YYYYMMDD') as numeric) as insertdate,
		cast(to_char(current_timestamp(), 'HH24MISS') as numeric) as inserttime,
		'DWH401' as insertid,
		bunkaikbn as bunkaikbn,
		1 as marker
	FROM item_jizen_bunkai_wend1 w14
	),

final
AS (
	SELECT 
        itemcode::VARCHAR(45) AS item_cd,
		kosecode::VARCHAR(45) AS kosei_cd,
		suryo::number(13, 4) AS suryo,
		koseritsu::number(16, 8) AS koseritsu,
		insertdate::number(18, 0) AS insertdate,
		inserttime::number(18, 0) AS inserttime,
		insertid::VARCHAR(9) AS insertid,
		bunkaikbn::VARCHAR(1) AS bunkaikbn,
		current_timestamp()::timestamp_ntz(9) AS inserted_date,
		null::VARCHAR(100) AS inserted_by,
		current_timestamp()::timestamp_ntz(9) AS updated_date,
		null::VARCHAR(100) AS updated_by,
		marker::number(38, 0) AS marker
	FROM transformed
	)
    
SELECT *
FROM final
