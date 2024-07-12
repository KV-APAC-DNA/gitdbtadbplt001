with cim01kokya as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIM01KOKYA
),
c_tbecusercard as (
select * from {{ ref('jpndclitg_integration__c_tbecusercard') }}
),
TBECORDER as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.TBECORDER
),
C_TBECKESAI as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.C_TBECKESAI
),
C_TBECUSRCOMMENT as (
select * from {{ ref('jpndclitg_integration__c_tbecusrcomment') }}
),
transformed as (
select 
distinct ck.kokyano as Customer_No, case when ck.kokyano=a.kokyano then 1 else 0 end as register_card_flg
from cim01kokya ck
left join ( 
SELECT distinct NVL(LPAD(DIECUSRID,10,'0'),'0000000000') AS kokyano
FROM C_TBECUSERCARD
WHERE DIELIMFLG = '0'
AND C_DSGMOMEMBERID IS NOT NULL
AND DIECUSRID IN (SELECT OD.DIECUSRID
                    FROM TBECORDER OD
                   INNER JOIN C_TBECKESAI KS
                      ON KS.DIORDERID = OD.DIORDERID
                   WHERE TO_CHAR(OD.DSORDERDT,'YYYYMMDD') >= '20180604' --2018年6月4日以降に受注あり
                     AND KS.DSKESSAIHOHO = 2 --カード決済
                     AND OD.DISHUKKASTS = '1060' --出荷済
                     AND OD.DIELIMFLG = 0 --削除なし
                     AND OD.DICANCEL = 0 --キャンセルなし
                     AND KS.DIELIMFLG = 0 --削除なし
                     AND KS.DICANCEL = 0 --キャンセルなし
                 )
AND DIECUSRID NOT IN (SELECT DIECUSRID
                      FROM C_TBECUSRCOMMENT
                     WHERE C_DSUSRCOMMENTCLASSKBN = '99'
                       AND C_DSUSRCOMMENT like '注文用紙カード記入欄あり毎回希望%'
                       AND DIELIMFLG = '0'
                      )
) a
on ck.kokyano=a.kokyano
where ck.testusrflg = '通常ユーザ'
),
final as (
select
CUSTOMER_NO::VARCHAR(68) as CUSTOMER_NO ,
REGISTER_CARD_FLG::NUMBER(18,0) as REGISTER_CARD_FLG
from transformed
)
select * from final