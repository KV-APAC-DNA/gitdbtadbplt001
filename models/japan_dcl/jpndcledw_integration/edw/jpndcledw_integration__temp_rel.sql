with C_TBMEMBUNITREL as (
    select * from {{ ref('jpndclitg_integration__c_tbmembunitrel') }}
),
transformed as (
SELECT
       LPAD(C_DIPARENTUSRID,10,'0') as C_DIPARENTUSRID,
       LPAD(REL.C_DICHILDUSRID,10,'0') as C_DICHILDUSRID
 FROM
       C_TBMEMBUNITREL REL
 WHERE
       DIELIMFLG = 0 AND
       --TO_CHAR(TO_DATE(DSREN,'YYYY-MM-DD HH24:MI:SS'),'YYYYMMDD')>= TO_CHAR(cast(SYSDATE as date)-7,'YYYYMMDD') 	--Timezone conversion from UTC 20220211
	   TO_CHAR(TO_DATE(DSREN,'YYYY-MM-DD HH24:MI:SS'),'YYYYMMDD')>=
       TO_CHAR(cast(CONVERT_TIMEZONE('Asia/Tokyo',current_timestamp()) as date)-7,'YYYYMMDD')
),
final as (
select
UNIFORM(10000,99999,random())::number(38,0) as ROWNUM,
C_DIPARENTUSRID::VARCHAR(30) as C_DIPARENTUSRID,
C_DICHILDUSRID::VARCHAR(30) as C_DICHILDUSRID,
current_timestamp()::timestamp_ntz(9) as INSERTED_DATE,
null::varchar(100) as inserted_by,
current_timestamp()::timestamp_ntz(9) as UPDATED_DATE,
null::varchar(100) as updated_by
from transformed
)
select * from final
