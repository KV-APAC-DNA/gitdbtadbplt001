WITH tbecitem
AS (
	SELECT *
	FROM  {{ ref('jpndclitg_integration__tbecitem') }}
	),
tbecsetitem
AS (
	SELECT *
	FROM  {{ ref('jpndclitg_integration__tbecsetitem') }}
	),
c_tbecprivilegemst
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__c_tbecprivilegemst') }}
	),
cim03item_zaiko_ikou_kizuna
AS (
	SELECT *
	FROM {{ source('jpdcledw_integration', 'cim03item_zaiko_ikou_kizuna') }}
	),
ct1
AS (
	SELECT item_hanbai.dsitemid AS h_itemcode,
		item_zaiko.dsitemid AS z_itemcode,
		1 AS num
	FROM tbecitem item_hanbai
	INNER JOIN tbecsetitem seti ON item_hanbai.diid = seti.diid
		AND item_hanbai.dsoption001::TEXT = '販売商品'::CHARACTER VARYING::TEXT
		AND item_hanbai.dielimflg::TEXT = '0'::CHARACTER VARYING::TEXT
	INNER JOIN tbecitem item_zaiko ON seti.disetitemid = item_zaiko.diid
		AND item_zaiko.dsoption001::TEXT = '在庫商品'::CHARACTER VARYING::TEXT
	),
ct2
AS (
	SELECT COALESCE(TO_VARCHAR(TRY_TO_NUMBER(c_diprivilegeid)), '') AS h_itemcode,
		COALESCE(TO_VARCHAR(TRY_TO_NUMBER(c_diprivilegeid)), '') AS z_itemcode,
		2 AS num
	FROM c_tbecprivilegemst
	),
ct3
AS (
	SELECT 'X000000001' AS h_itemcode,
		'X000000001' AS z_itemcode,
		3
	),
ct4
AS (
	SELECT 'X000000002' AS h_itemcode,
		'X000000002' AS z_itemcode,
		3
	),
ct5
AS (
	SELECT ciik.bar_cd2 AS h_itemcode,
		max(ciik.itemcode::TEXT)::CHARACTER VARYING AS z_itemcode,
		99 AS maker
	FROM cim03item_zaiko_ikou_kizuna ciik
	WHERE ciik.syutoku_kbn = 'PORT'
		AND ciik.bar_cd2 <> ' '
		AND NOT EXISTS (
			SELECT item_hanbai.dsitemid AS itemcode,
				item_zaiko.dsitemid AS kosei_cd
			FROM tbecitem item_hanbai
			INNER JOIN tbecsetitem seti ON item_hanbai.diid = seti.diid
				AND item_hanbai.dsoption001::TEXT = '販売商品'::CHARACTER VARYING::TEXT
				AND item_hanbai.dielimflg::TEXT = '0'::CHARACTER VARYING::TEXT
			INNER JOIN tbecitem item_zaiko ON seti.disetitemid = item_zaiko.diid
				AND item_zaiko.dsoption001::TEXT = '在庫商品'::CHARACTER VARYING::TEXT
			WHERE item_hanbai.dsitemid = ciik.bar_cd2
			)
	GROUP BY ciik.bar_cd2
	),
transformed
AS (
	SELECT *
	FROM ct1
	
	UNION ALL
	
	SELECT *
	FROM ct2
	
	UNION ALL
	
	SELECT *
	FROM ct3
	
	UNION ALL
	
	SELECT *
	FROM ct4
	
	UNION ALL
	
	SELECT *
	FROM ct5
	),
final
AS (
    SELECT h_itemcode::VARCHAR(45) AS h_itemcode,
        z_itemcode::VARCHAR(45) AS z_itemcode,
        num::NUMBER(38,0) AS marker,
        current_timestamp()::timestamp_ntz(9) AS inserted_date,
        null::VARCHAR(100) AS inserted_by,
        current_timestamp()::timestamp_ntz(9) AS updated_date,
        null::VARCHAR(100) AS updated_by
    FROM transformed
)
SELECT *
FROM final