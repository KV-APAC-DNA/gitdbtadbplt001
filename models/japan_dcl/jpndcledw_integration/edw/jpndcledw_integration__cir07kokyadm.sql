{{
    config(
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";",
    )
}}
with hanyo_attr as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.HANYO_ATTR
),
cl_mst as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CL_MST
),
cl_meisai as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CL_MEISAI
),
c_tbdmsndhist as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.C_TBDMSNDHIST
),
transformed as (
SELECT
       LPAD(DMH.C_DIUSRID,10,0) as KOKYANO,
       CAST(DMH.C_DISENDID as VARCHAR) as GYONO,
       DMH.C_DSDMNUMBER as DMOUTNO,
       CAST(TO_CHAR(DMH.DSREN,'YYYYMMDD')as INTEGER) as UPDATEDATE
 FROM
       C_TBDMSNDHIST DMH
 WHERE   1= 1
       AND DMH.DIELIMFLG = '0' 
	   AND DMH.C_DSDMSENDKUBUN IN(1,2) 
	   AND NOT EXISTS  
 (SELECT
       'X'
 FROM
       CL_MST CL_MST
 WHERE 
       DMNUMBER = DMH.C_DSDMNUMBER) 
	   AND (DMH.C_DSDMNAME NOT LIKE '%CLﾌﾟﾚﾐｱﾑ%発送%'
	   AND DMH.C_DSDMNAME NOT LIKE '%ｼｰﾗﾊﾞｰ%発送%'
	   AND DMH.C_DSDMNAME NOT LIKE '%CLプレミアム%発送%'
		AND DMH.C_DSDMNAME NOT LIKE '%シーラバー%発送%'
		AND DMH.C_DSDMNAME NOT LIKE '%美研%発送%'
		AND DMH.C_DSDMNAME NOT LIKE '%CLライト版%発送%' )   
 UNION ALL
 SELECT
       KOKYANO as KOKYANO,
       ROW_NUM as GYONO,
       C_DSDMNUMBER as DMOUTNO,
       CAST(TO_CHAR(UPDATEDATE,'YYYYMMDD')as INTEGER) as UPDATEDATE
 FROM
 (SELECT
       MEI.KOKYANO,
       MEI.ROW_NUM,
       MST.GROUPING_DMNUMBER as C_DSDMNUMBER,
       MEI.UPDATEDATE
 FROM
       CL_MST MST
 INNER JOIN 
    CL_MEISAI MEI
  ON
       MST.CLNO = MEI.CLNO AND 
       MST.FILENAME = MEI.FILENAME) CL
 UNION ALL
 SELECT
       LPAD(DMH.C_DIUSRID,10,0) as KOKYANO,
       CAST(DMH.C_DISENDID as VARCHAR) as GYONO,
       DMH.C_DSDMNUMBER as DMOUTNO,
       CAST(REPLACE(SUBSTRING(HANYOU.ATTR2,1,10),'/','')as INTEGER) as UPDATEDATE
 FROM
       C_TBDMSNDHIST DMH,
       HANYO_ATTR HANYOU
 WHERE   
       DMH.C_DSDMSENDKUBUN = '3' AND  
       HANYOU.KBNMEI = 'QVDM' AND  
       DMH.C_DSDMNUMBER = HANYOU.ATTR1 AND  
       DMH.DIELIMFLG = '0'
),
final as (
select
kokyano::varchar(20) as kokyano,
gyono::varchar(40) as gyono,
dmoutno::varchar(80) as dmoutno,
updatedate::number(38,0) as updatedate
from transformed
)
select * from final