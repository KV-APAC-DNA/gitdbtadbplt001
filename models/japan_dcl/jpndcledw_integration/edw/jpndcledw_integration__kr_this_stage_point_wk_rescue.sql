with KR_THIS_STAGE_POINT_RESCUE as(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.KR_THIS_STAGE_POINT_RESCUE
),
C_TBMEMBUNITREL as(
    select * from {{ ref('jpndclitg_integration__c_tbmembunitrel') }}
),
RESCUE_NAYOSE AS (
	SELECT RSC.YYYYMM,
		NVL2(TBMEMBUNITREL.C_DIPARENTUSRID, TBMEMBUNITREL.C_DIPARENTUSRID, RSC.USRID) usrid,
		RSC.AMOUNT amount,
		RSC.POINT_GRANTED point_granted
	FROM KR_THIS_STAGE_POINT_RESCUE RSC
	LEFT JOIN C_TBMEMBUNITREL TBMEMBUNITREL
		-- ■名寄せテーブル
		ON RSC.USRID = TBMEMBUNITREL.C_DICHILDUSRID
		AND TBMEMBUNITREL.DIELIMFLG = '0'
),
transformed as(
    SELECT YYYYMM::varchar(6) as yyyymm,
        USRID::number(38,0) as usrid,
        SUM(AMOUNT)::number(38,0) as diff_amout,
        SUM(POINT_GRANTED)::number(18,0) as point_granted
    FROM RESCUE_NAYOSE
    GROUP BY YYYYMM,
        USRID
)
select * from transformed
