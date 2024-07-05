WITH item_jizen_bunkai_w10
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w10') }}
	),
transformed
AS (
	SELECT bom.itemcode AS itemcode,
		sum(bom.suryo) AS kosecode_cnt
	FROM item_jizen_bunkai_w10 bom
	GROUP BY bom.itemcode
	),
final
AS (
	SELECT itemcode::VARCHAR(40) AS itemcode,
		kosecode_cnt::number(38, 4) AS kosecode_cnt
	FROM transformed
	)
SELECT *
FROM final