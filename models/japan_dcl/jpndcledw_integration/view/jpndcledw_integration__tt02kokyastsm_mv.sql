WITH tt02kokyastsm_mv_tbl
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__tt02kokyastsm_mv_tbl') }}
	)
	
	,transformed
AS (
	SELECT tt02kokyastsm_mv_tbl.saleno
		,tt02kokyastsm_mv_tbl.gyono
		,tt02kokyastsm_mv_tbl.itemcode
		,tt02kokyastsm_mv_tbl.meisainukikingaku
		,tt02kokyastsm_mv_tbl.warimaenukikingaku
		,tt02kokyastsm_mv_tbl.saleno_trm
		,tt02kokyastsm_mv_tbl.maker
		,tt02kokyastsm_mv_tbl.salemrowid
	FROM tt02kokyastsm_mv_tbl
	)
	
SELECT *
FROM transformed
