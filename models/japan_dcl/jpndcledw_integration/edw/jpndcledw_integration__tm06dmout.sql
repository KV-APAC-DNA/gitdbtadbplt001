{{
    config(
        materialized= "incremental",
        incremental_strategy= "append"
    )
}}


with CIR07DMPRN as (
    select * from DEV_DNA_CORE.JPDCLEDW_INTEGRATION.CIR07DMPRN
),
final as (
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
)
select * from final