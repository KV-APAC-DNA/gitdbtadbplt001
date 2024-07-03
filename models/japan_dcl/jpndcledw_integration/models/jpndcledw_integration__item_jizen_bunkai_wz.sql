WITH item_zaiko_tbl
AS (
	SELECT *
	FROM dev_dna_core.snapjpdcledw_integration.item_zaiko_tbl
	),
item_hanbai_tbl
AS (
	SELECT *
	FROM dev_dna_core.snapjpdcledw_integration.item_hanbai_tbl
	),
transformed
AS (
	SELECT z_itemcode AS itemcode,
		tanka
	FROM item_zaiko_tbl
	WHERE marker IN (1, 2)
	GROUP BY z_itemcode,
		tanka
	
	UNION ALL
	
	SELECT h_itemcode AS itemcode,
		tanka
	FROM item_hanbai_tbl
	WHERE marker IN (1, 2, 3)
	GROUP BY h_itemcode,
		tanka
	),
final
AS (
	SELECT itemcode::VARCHAR(40) AS itemcode,
		tanka::DECIMAL(16, 8) AS tanka
	FROM transformed
	)
SELECT *
FROM final