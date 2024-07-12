with cim01kokya as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIM01KOKYA
),
c_tbecclient as (
select * from {{ ref('jpndclitg_integration__c_tbecclient') }}
),
tbecorder as (
select * from {{ ref('jpndclitg_integration__tbecorder') }}
),
transformed as (
select 
distinct ck.kokyano as Customer_No, a.C_DSTEMPONAME as main_store --case when ck.kokyano=a.kokyano then a.C_DSTEMPONAME else null end as main_store
from cim01kokya ck
left join ( 
SELECT 
	NVL(LPAD("最多購入店舗"."顧客ID",10,'0'),'0000000000') AS kokyano, "取引先".C_DSTEMPONAME --★main_store
FROM (
	select diEcUsrId AS "顧客ID",max(c_dstempocode) AS "件数店舗コード"
	from ( 
        select diEcUsrId,c_dstempocode, dense_rank() over(PARTITION BY diEcUsrId ORDER BY cnt desc, MAX_dsUriageDt DESC nulls last) as dens_rank
         from (
	SELECT
	diEcUsrID,
	c_dsTempoCode,
	SUM(cnt) AS cnt,
	MAX(MAX_dsUriageDt) AS MAX_dsUriageDt
	FROM (
		SELECT
		diEcUsrID,
		c_dstempocode,
		count(c_dstempocode) AS cnt,
		MAX(dsUriageDt) AS MAX_dsUriageDt
		FROM tbEcOrder
		WHERE c_dspotsalesno IS NOT NULL
		AND dsUriageDt >= to_date(ADD_MONTHS(CONVERT_TIMEZONE('UTC','Asia/Tokyo','2024-07-01'),-36)) --TO_DATE(TO_CHAR(ADD_MONTHS(TO_DATE(current_timestamp()),-36),'YYYYMMDD'))
		AND diCancel = '0'
		AND c_diallhenpinflg = '0'
		AND dielimflg = '0'
		GROUP BY diEcUsrID, c_dstempocode
		)
	GROUP BY diEcUsrID, c_dstempocode
	)
	) where dens_rank = 1
GROUP BY diEcUsrID
)as "最多購入店舗"
LEFT OUTER JOIN c_tbecClient "取引先"
ON "最多購入店舗"."件数店舗コード" = "取引先".c_dstempocode
) a
on ck.kokyano=a.kokyano 
where ck.testusrflg = '通常ユーザ'
),
final as (
select 
CUSTOMER_NO::VARCHAR(68) as customer_no,
MAIN_STORE::VARCHAR(60) as main_store
from transformed
)
select * from final


