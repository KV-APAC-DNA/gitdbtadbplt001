with cim01kokya as (
select * from {{ ref('jpndcledw_integration__cim01kokya') }}
),
teikikeiyaku_data_mart_uni as (
select * from {{ ref('jpndcledw_integration__teikikeiyaku_data_mart_uni_prev') }}
),
b AS (
SELECT 
 ck.kokyano AS customer_no
,un.c_diregularcontractid 
,un.keiyakubi
,un.kaiyakumoushidebi
,un.shokai_ym
,CASE WHEN (un.kaiyakubi IS NULL OR un.kaiyakumoushidebi IS NULL) 
      AND un.shokai_ym IS NOT NULL 
      THEN '1' ELSE '0' END AS subscription_flg
FROM cim01kokya ck
LEFT JOIN teikikeiyaku_data_mart_uni un
ON ck.kokyano=NVL(LPAD(un.c_diusrid,10,'0'),'0000000000')
WHERE ck.testusrflg = '通常ユーザ' 
),
final as (SELECT
   customer_no::varchar(68) as customer_no
  ,MAX(subscription_flg)::varchar(1) AS subscription_flg
FROM b
GROUP BY 1
)
select * from final