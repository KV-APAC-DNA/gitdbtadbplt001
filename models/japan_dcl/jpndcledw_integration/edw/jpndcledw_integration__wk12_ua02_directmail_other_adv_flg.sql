with cim01kokya as (
select * from {{ ref('jpndcledw_integration__cim01kokya') }}
),
c_tbecusrcomment as (
select * from {{ ref('jpndclitg_integration__c_tbecusrcomment') }}
),
transformed as (
select 
distinct ck.kokyano as Customer_No, case when ck.kokyano=a.kokyano then 1 else 0 end as other_adv_flg
from cim01kokya ck
left join ( 
SELECT distinct NVL(LPAD(DIECUSRID,10,'0'),'0000000000') AS kokyano
FROM C_TBECUSRCOMMENT
WHERE 
C_DSUSRCOMMENTCLASSKBN = '99' AND
C_DSUSRCOMMENT LIKE '%他社チラシ%' AND
DIELIMFLG = 0
) a
on ck.kokyano=a.kokyano
where ck.testusrflg = '通常ユーザ'
),
final as (
select
CUSTOMER_NO::VARCHAR(68) as CUSTOMER_NO,
OTHER_ADV_FLG::NUMBER(18,0) as OTHER_ADV_FLG
from transformed
)
select * from final
