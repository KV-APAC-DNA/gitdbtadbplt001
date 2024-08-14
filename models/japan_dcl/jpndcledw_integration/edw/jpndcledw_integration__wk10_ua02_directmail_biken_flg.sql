with cim01kokya as (
select * from {{ ref('jpndcledw_integration__cim01kokya') }}
),
c_tbecusrcomment as (
select * from {{ ref('jpndclitg_integration__c_tbecusrcomment') }}
),
transformed as (
select 
distinct ck.kokyano as Customer_No, case when ck.kokyano=a.kokyano then 1 else 0 end as biken_flg
from cim01kokya ck
left join (   
SELECT distinct NVL(LPAD(DIECUSRID,10,'0'),'0000000000') AS kokyano 
FROM   C_TBECUSRCOMMENT
WHERE  C_DSUSRCOMMENTCLASSKBN = '99'
AND    C_DSUSRCOMMENT LIKE 'シーラボ美研%送付不要%'
AND    DIELIMFLG = '0'
) a
on ck.kokyano=a.kokyano
where ck.testusrflg = '通常ユーザ'
) ,
final as (
select 
customer_no::varchar(68) as customer_no,
BIKEN_FLG::NUMBER(18,0) as BIKEN_FLG ,
from transformed
)
select * from final