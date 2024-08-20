{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{{build_tm06dmout()}}"
    )
}}


with CIR07DMPRN as (
    select * from {{ ref('jpndcledw_integration__cir07dmprn') }}
),
transformed as (
SELECT DISTINCT 
a.DMPRNNO,
      NULL as DMNAME,
      NULL as DMRYAKU,
      99991231 as OUTDATE,
      '00' as CHANNELCODE,
      '不明' as CHANNELNAME,
      '00' as DMDAIBUCODE,
      '不明' as DMDAIBUNNAME,
      '000' as DMCHUBUNCODE,
      '不明' as DMCHUBUNNAME,
      '0000' as DMSYOBUNCODE,
      '不明' as DMSYOBUNNAME,
      CAST(TO_CHAR(SYSDATE(), 'yyyymmdd') AS INTEGER) as INSERTDATE,
      CAST(TO_CHAR(SYSDATE(), 'hh24miss') AS INTEGER) as INSERTTIME,
      '004001' as INSERTID,
      CAST(TO_CHAR(SYSDATE(), 'yyyymmdd') AS INTEGER) as UPDATEDATE,
      CAST(TO_CHAR(SYSDATE(), 'hh24miss') AS INTEGER) as UPDATETIME,
      '004001' as UPDATEID,
      sysdate() as INSERTED_DATE,
      null as INSERTED_BY,
      sysdate() as UPDATED_DATE,
      null as UPDATED_BY
FROM CIR07DMPRN a
WHERE NOT EXISTS (
            SELECT 1
            FROM {{this}} c
            WHERE a.DMPRNNO = c.DMPRNNO
            )
),
final as (
select
dmprnno::varchar(96) as dmprnno,
dmname::varchar(300) as dmname,
dmryaku::varchar(300) as dmryaku,
outdate::number(8,0) as outdate,
channelcode::varchar(6) as channelcode,
channelname::varchar(60) as channelname,
dmdaibucode::varchar(6) as dmdaibucode,
dmdaibunname::varchar(300) as dmdaibunname,
dmchubuncode::varchar(9) as dmchubuncode,
dmchubunname::varchar(300) as dmchubunname,
dmsyobuncode::varchar(12) as dmsyobuncode,
dmsyobunname::varchar(300) as dmsyobunname,
insertdate::number(8,0) as insertdate,
inserttime::number(6,0) as inserttime,
insertid::varchar(18) as insertid,
updatedate::number(8,0) as updatedate,
updatetime::number(6,0) as updatetime,
updateid::varchar(18) as updateid,
sysdate()::timestamp_ntz(9) as inserted_date,
null::varchar(100) as inserted_by,
sysdate()::timestamp_ntz(9) as updated_date,
updated_by::varchar(100) as updated_by
from
transformed)
select * from final