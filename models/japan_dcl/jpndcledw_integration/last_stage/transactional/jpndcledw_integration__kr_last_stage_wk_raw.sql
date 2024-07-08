with kr_this_stage_point_monthly as(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.kr_this_stage_point_monthly
),
dcl_calendar_sysdate as(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.dcl_calendar_sysdate
),
transformed as(
    SELECT ST.YYYYMM::varchar(9) as yyyymm,
	ST.USRID::number(38,0) as usrid,
	CASE 
		WHEN RIGHT(CAL.TARGET_MONTH, 2) = '12'
			THEN NULL
		ELSE ST.STAGE
		END::varchar(18) AS STAGE,
	CASE 
		WHEN RIGHT(CAL.TARGET_MONTH, 2) = '12'
			THEN 0
		ELSE NVL(ST.THISTOTALPRC, 0)
		END::number(38,18) AS THISTOTALPRC, -- 前年最終時点の年間累計購入額
	ST.INSERTDATE::varchar(25) as insertdate
    FROM kr_this_stage_point_monthly ST, -- ■前年度ステージ計算結果テーブル
        dcl_calendar_sysdate CAL
    WHERE ST.YYYYMM = TO_CHAR(DATEADD('YEAR', - 1, TO_DATE(CAL.TARGET_MONTH, 'YYYYMM')), 'YYYY') || '12'
        AND CAL.IS_ACTIVE = TRUE
)
select * from transformed