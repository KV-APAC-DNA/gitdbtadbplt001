WITH bom_sap_v
AS (
	SELECT *
	FROM dev_dna_core.snapjpdcledw_integration.bom_sap_v
	),
item_bom_ikou_kizuna
AS (
	SELECT *
	FROM dev_dna_core.snapjpdcledw_integration.item_bom_ikou_kizuna
	),
transformed
AS (
	SELECT bom_sap.oya_hin_cd AS itemcode,
		bom_sap.kos_hin_cd AS kosecode,
		nvl(bom_sap.kos_qut, 0)::NUMERIC(28, 4) AS suryo
	FROM bom_sap_v bom_sap
	WHERE bom_sap.kos_hin_cd NOT LIKE '009%'
		AND bom_sap.kos_hin_cd NOT LIKE '0082%'
		AND bom_sap.kos_hin_cd NOT LIKE '019%'
		AND bom_sap.kos_hin_cd NOT LIKE '0182%'
	
	UNION
	
	SELECT bom_ikou.oya_hin_cd AS itemcode,
		bom_ikou.kos_hin_cd AS kosecode,
		nvl(bom_ikou.kos_qut, 0)::NUMERIC(28, 4) AS suryo
	FROM item_bom_ikou_kizuna bom_ikou
	WHERE NOT EXISTS (
			SELECT 1
			FROM bom_sap_v bom_sap
			WHERE bom_sap.oya_hin_cd = bom_ikou.oya_hin_cd
			)
	),
final
AS (
	SELECT itemcode::VARCHAR(40) AS itemcode,
		kosecode::VARCHAR(40) AS kosecode,
		suryo::number(28, 4) AS suryo
	FROM transformed
	)
SELECT *
FROM final