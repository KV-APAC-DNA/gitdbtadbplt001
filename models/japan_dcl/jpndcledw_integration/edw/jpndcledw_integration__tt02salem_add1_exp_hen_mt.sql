
WITH tt02salem_add3_epi_hen_mt
AS (
	SELECT *
	FROM dev_dna_core.snapjpdcledw_integration.tt02salem_add3_epi_hen_mt
	)

	,transformed
AS (
	SELECT tt02salem_add3_epi_hen_mt.saleno
		,9000000002::BIGINT AS gyono
		,'利用ポイント数(交換)' AS meisaikbn
		,'X000000002' AS itemcode
		,1 AS suryo
		,(- sum(tt02salem_add3_epi_hen_mt.kingaku)) AS tanka
		,(- sum(tt02salem_add3_epi_hen_mt.kingaku)) AS kingaku
		,(- sum(tt02salem_add3_epi_hen_mt.meisainukikingaku)) AS meisainukikingaku
		,0 AS wariritu
		,(- sum(tt02salem_add3_epi_hen_mt.warimaekomitanka)) AS warimaekomitanka
		,(- sum(tt02salem_add3_epi_hen_mt.warimaenukikingaku)) AS warimaenukikingaku
		,(- sum(tt02salem_add3_epi_hen_mt.warimaekomikingaku)) AS warimaekomikingaku
		,tt02salem_add3_epi_hen_mt.dispsaleno
		,tt02salem_add3_epi_hen_mt.kesaiid
		,tt02salem_add3_epi_hen_mt.henpinsts
	FROM tt02salem_add3_epi_hen_mt
	GROUP BY tt02salem_add3_epi_hen_mt.saleno
		,tt02salem_add3_epi_hen_mt.dispsaleno
		,tt02salem_add3_epi_hen_mt.kesaiid
		,tt02salem_add3_epi_hen_mt.henpinsts
	)
    
SELECT *
FROM transformed
