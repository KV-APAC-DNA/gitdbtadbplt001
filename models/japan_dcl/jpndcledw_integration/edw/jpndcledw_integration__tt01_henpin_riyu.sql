{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{{ mvref_tt01_henpinriyu() }}"
    )
}}
with C_TBECINQUIREMEISAI as (
    select * from  {{ ref('jpndclitg_integration__c_tbecinquiremeisai') }}
),
HANYO_ATTR as (
    select * from {{ ref('jpndcledw_integration__hanyo_attr') }}
),
transformed as (
SELECT
		       RIYU.DIINQUIREID,
		       RIYU.DIINQUIREKESAIID,
		       RIYU.DIHENPINRIYUID
		FROM
		 (SELECT
		       b.DIINQUIREID,
		       b.DIINQUIREKESAIID,
		       b.DIHENPINRIYUID
		 FROM
		       C_TBECINQUIREMEISAI b
		 INNER JOIN 
		 (SELECT
		       MEI.DIINQUIREID as DIINQUIREID,
		       MEI.DIINQUIREKESAIID as DIINQUIREKESAIID,
		       MIN(NVL(MEI.DIMEISAIID,0)) as DIMEISAIID
		 FROM
		       C_TBECINQUIREMEISAI MEI
		 INNER JOIN 
		 (SELECT
		       M.DIINQUIREID as DIINQUIREID,
		       M.DIINQUIREKESAIID as DIINQUIREKESAIID,
		       MAX(NVL(M.DITOTALPRC,0)) as maxprc
		 FROM
		       C_TBECINQUIREMEISAI M
		 WHERE   
		       M.DIHENPINKUBUN <> 0 AND  
		       M.DSREN >=  (Select dateadd(day,(select distinct(CAST(ATTR1 as INTEGER)) FROM HANYO_ATTR where HANYO_ATTR.KBNMEI = 'DAILYFROM'),CONVERT_TIMEZONE('Asia/Tokyo',current_timestamp())::date))
		 GROUP BY
		       M.DIINQUIREID ,
		       M.DIINQUIREKESAIID ) mtp
		  ON
		       mtp.DIINQUIREID = MEI.DIINQUIREID AND 
		       mtp.maxprc = NVL(MEI.DITOTALPRC,0)
		 WHERE 
		       MEI.DSREN >= (Select dateadd(day,(select distinct(CAST(ATTR1 as INTEGER))  FROM HANYO_ATTR where HANYO_ATTR.KBNMEI = 'DAILYFROM'),CONVERT_TIMEZONE('Asia/Tokyo',current_timestamp())::date))
		 GROUP BY
		       MEI.DIINQUIREID ,
		       MEI.DIINQUIREKESAIID ) a
		  ON
		       a.DIINQUIREID = b.DIINQUIREID AND 
		       a.DIINQUIREKESAIID = b.DIINQUIREKESAIID AND 
		       a.DIMEISAIID = b.DIMEISAIID) RIYU
		LEFT JOIN
		       {{this}} TT01
		  ON
		       TT01.DIINQUIREID = RIYU.DIINQUIREID AND 
		       TT01.DIINQUIREKESAIID = RIYU.DIINQUIREKESAIID
		WHERE 
		    TT01.DIINQUIREID IS NULL 
		    AND TT01.DIINQUIREKESAIID IS NULL 
),
final as (
select
diinquireid::number(38,0) as diinquireid,
diinquirekesaiid::number(38,0) as diinquirekesaiid,
dihenpinriyuid::number(38,0) as dihenpinriyuid,
null::timestamp_ntz(9)as insertdate,
null::timestamp_ntz(9)as updatedate,
current_timestamp()::timestamp_ntz(9) as INSERTED_DATE,
null::varchar(100) as inserted_by,
current_timestamp()::timestamp_ntz(9) as UPDATED_DATE,
null::varchar(100) as updated_by
from transformed
)
select * from final