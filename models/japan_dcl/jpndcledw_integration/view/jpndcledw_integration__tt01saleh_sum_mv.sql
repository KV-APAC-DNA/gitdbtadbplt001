WITH tt01saleh_sum_mv_tbl
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__tt01saleh_sum_mv_tbl') }}
	)
	
	,transformed
AS (
	SELECT tt01saleh_sum_mv_tbl.saleno
		,tt01saleh_sum_mv_tbl.juchkbn
		,tt01saleh_sum_mv_tbl.juchdate
		,tt01saleh_sum_mv_tbl.kokyano
		,tt01saleh_sum_mv_tbl.hanrocode
		,tt01saleh_sum_mv_tbl.syohanrobunname
		,tt01saleh_sum_mv_tbl.chuhanrobunname
		,tt01saleh_sum_mv_tbl.daihanrobunname
		,tt01saleh_sum_mv_tbl.mediacode
		,tt01saleh_sum_mv_tbl.kessaikbn
		,tt01saleh_sum_mv_tbl.soryo
		,tt01saleh_sum_mv_tbl.tax
		,tt01saleh_sum_mv_tbl.sogokei
		,tt01saleh_sum_mv_tbl.cardcorpcode
		,tt01saleh_sum_mv_tbl.henreasoncode
		,tt01saleh_sum_mv_tbl.cancelflg
		,tt01saleh_sum_mv_tbl.insertdate
		,tt01saleh_sum_mv_tbl.inserttime
		,tt01saleh_sum_mv_tbl.insertid
		,tt01saleh_sum_mv_tbl.updatedate
		,tt01saleh_sum_mv_tbl.updatetime
		,tt01saleh_sum_mv_tbl.zipcode
		,tt01saleh_sum_mv_tbl.todofukencode
		,tt01saleh_sum_mv_tbl.happenpoint
		,tt01saleh_sum_mv_tbl.riyopoint
		,tt01saleh_sum_mv_tbl.shukkasts
		,tt01saleh_sum_mv_tbl.torikeikbn
		,tt01saleh_sum_mv_tbl.tenpocode
		,tt01saleh_sum_mv_tbl.shukadate
		,tt01saleh_sum_mv_tbl.rank
		,tt01saleh_sum_mv_tbl.dispsaleno
		,tt01saleh_sum_mv_tbl.kesaiid
		,tt01saleh_sum_mv_tbl.henreasonname
		,tt01saleh_sum_mv_tbl.uketsukeusrid
		,tt01saleh_sum_mv_tbl.uketsuketelcompanycd
		,tt01saleh_sum_mv_tbl.smkeiroid
		,tt01saleh_sum_mv_tbl.dipromid
		,tt01saleh_sum_mv_tbl.maker
		,tt01saleh_sum_mv_tbl.salehrowid
	FROM tt01saleh_sum_mv_tbl
	)
	
SELECT *
FROM transformed
