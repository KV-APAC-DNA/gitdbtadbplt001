WITH tt02salem_mv_tbl
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__tt02salem_mv_tbl') }}
	)

	,transformed
AS (
	SELECT tt02salem_mv_tb1.saleno
		,tt02salem_mv_tb1.gyono
		,tt02salem_mv_tb1.itemcode
		,tt02salem_mv_tb1.wariritu
		,tt02salem_mv_tb1.tanka
		,tt02salem_mv_tb1.warimaekomitanka
		,tt02salem_mv_tb1.suryo
		,tt02salem_mv_tb1.kingaku
		,tt02salem_mv_tb1.warimaekomikingaku
		,tt02salem_mv_tb1.meisainukikingaku
		,tt02salem_mv_tb1.warimaenukikingaku
		,tt02salem_mv_tb1.meisaitax
		,tt02salem_mv_tb1.dispsaleno
		,tt02salem_mv_tb1.kesaiid
		,tt02salem_mv_tb1.saleno_trm
		,tt02salem_mv_tb1.maker
		,tt02salem_mv_tb1.salemrowid
	FROM tt02salem_mv_tbl tt02salem_mv_tb1
	)

SELECT  *
FROM transformed