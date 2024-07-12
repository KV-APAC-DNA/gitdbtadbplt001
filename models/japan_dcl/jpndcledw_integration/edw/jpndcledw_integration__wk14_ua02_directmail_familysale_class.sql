with cim01kokya as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIM01KOKYA
),
c_tbecusrcomment as (
select * from {{ ref('jpndclitg_integration__c_tbecusrcomment') }}
),
transformed as (
select 
distinct ck.kokyano as Customer_No, case when ck.kokyano=a.kokyano then 1 else 0 end as familysale_class
from cim01kokya ck
left join ( 
SELECT DISTINCT NVL(LPAD(DIUSRID,10,'0'),'0000000000') AS kokyano
FROM (
	/*
	SELECT DIUSRID FROM TBUSRPRAM WHERE DSDAT13 = '対象'
	union all
	*/
	SELECT DIECUSRID AS DIUSRID FROM C_TBECUSRCOMMENT COMME
	WHERE 1 = 1
	AND COMME.C_DSUSRCOMMENT like '%FS会員番号%'
	AND COMME.C_DSUSRCOMMENTCLASSKBN = '33'
	AND COMME.DIELIMFLG = 0
	union all
	SELECT DIECUSRID AS DIUSRID FROM C_TBECUSRCOMMENT COMME
	WHERE 1 = 1
	AND COMME.C_DSUSRCOMMENT like '%FS会員番号%'
	AND COMME.C_DSUSRCOMMENTCLASSKBN = '52'
	AND COMME.DIELIMFLG = 0
    )
) a
on ck.kokyano=a.kokyano
where ck.testusrflg = '通常ユーザ'
),
final as (
select
	CUSTOMER_NO::VARCHAR(68) as CUSTOMER_NO,
	FAMILYSALE_CLASS::NUMBER(18,0) as FAMILYSALE_CLASS
from transformed
)
select * from final