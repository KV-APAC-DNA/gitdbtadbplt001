{{
    config(
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";",
    )
}}
with c_tbdmsndhist as (
select * from {{ ref('jpndclitg_integration__c_tbdmsndhist') }}
),
cl_mst as (
select * from {{ source('jpdcledw_integration','cl_mst') }} 
),
hanyo_attr as (
select * from {{source('jpdcledw_integration', 'hanyo_attr')}} --using as source as cycle is created
),
transformed as (
SELECT
    S.NUM as DMPRNNO,
    H.C_DSDMNAME as COMMENT1,
    S.CNT as KENSU,
    H.DSPREP as INSERTDATE,
    H.DIPREPUSR as INSERTID,
    H.DIELIMFLG as DELETEFLG
 FROM
    C_TBDMSNDHIST H
    INNER JOIN 
    (
		SELECT
			T.C_DSDMNUMBER as NUM,
			COUNT(T.C_DSDMNUMBER) as CNT,
			MAX(T.C_DISENDID) as T_ROWID,
			'DUMMY' as C_DSDMNAME,
			'0' as FLG
		FROM
			C_TBDMSNDHIST T
		WHERE 1=1
		AND NOT EXISTS (
			SELECT
				'X'
			FROM
				CL_MST CL_MST
			WHERE 
				DMNUMBER = T.C_DSDMNUMBER
		) 
		AND	  (		  
				T.C_DSDMNAME NOT LIKE '%CLﾌﾟﾚﾐｱﾑ%発送%' AND T.C_DSDMNAME NOT LIKE '%ｼｰﾗﾊﾞｰ%発送%'
			AND T.C_DSDMNAME NOT LIKE '%CLプレミアム%発送%'
            AND T.C_DSDMNAME NOT LIKE '%シーラバー%発送%'
            AND T.C_DSDMNAME NOT LIKE '%美研%発送%'
            AND T.C_DSDMNAME NOT LIKE '%CLライト版%発送%' 
		)
        AND T.DIELIMFLG = 0 
	    AND T.C_DSDMSENDKUBUN BETWEEN 1 AND  2       
		GROUP BY
			C_DSDMNUMBER 
	) S		
  ON
       H.C_DISENDID = S.T_ROWID
 UNION ALL
 SELECT
       MST.GROUPING_DMNUMBER as DMPRNNO,
       MIN(DMNAME) as COMMENT1,
       SUM(KENSUU_KOTEI) as KENSU,
       MAX(UPDATEDATE) as INSERTDATE,
       NULL as INSERTID,
       '0' as DELETEFLG
 FROM
       CL_MST MST
 GROUP BY
       MST.GROUPING_DMNUMBER 
 UNION ALL
 SELECT
       S.NUM as DMPRNNO,
       H.C_DSDMNAME as COMMENT1,
       S.CNT as KENSU,
       TO_DATE(S.INSERTDATE,'YYYY/MM/DD HH24:MI:SS.FF3') as INSERTDATE,
       H.DIPREPUSR as INSERTID,
       H.DIELIMFLG as DELETEFLG
 FROM
       C_TBDMSNDHIST H
		INNER JOIN 
		(	
			SELECT
			T.C_DSDMNUMBER as NUM,
			COUNT(T.C_DSDMNUMBER) as CNT,
			MAX(T.C_DISENDID) as T_ROWID,
			MAX(HANYOU.ATTR2) as INSERTDATE
		FROM
			C_TBDMSNDHIST T,
			HANYO_ATTR HANYOU
		WHERE   
			T.C_DSDMSENDKUBUN = '3' AND  
			T.DIELIMFLG = 0 AND  
			HANYOU.KBNMEI = 'QVDM' AND  
			T.C_DSDMNUMBER = HANYOU.ATTR1
		GROUP BY
			T.C_DSDMNUMBER 
	) S		
ON  H.C_DISENDID = S.T_ROWID
 ),
final as (
select
dmprnno::varchar(60) as dmprnno,
comment1::varchar(600) as comment1,
kensu::number(18,0) as kensu,
insertdate::timestamp_ntz(9) as insertdate,
insertid::number(18,0) as insertid,
deleteflg::varchar(2) as deleteflg,
null::timestamp_ntz(9) as inserted_date,
null::varchar(100) as inserted_by,
null::timestamp_ntz(9) as updated_date,
null::varchar(100) as updated_by
from transformed
)
select * from final