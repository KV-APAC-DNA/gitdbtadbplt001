
with tbusrpram as (
select * from {{ ref('jpndclitg_integration__tbusrpram') }}
),
tbecorder as (
select * from {{ ref('jpndclitg_integration__tbecorder') }}
),
tbecordermeisai as (
select * from {{ ref('jpndclitg_integration__tbecordermeisai') }}
),
final as (
SELECT 
  tbecorder.diorderid AS odrreceiveno, 
  lpad(
    (
      (tbecorder.diecusrid):: character varying
    ):: text, 
    10, 
    ('0' :: character varying):: text
  ) AS customerid, 
  tbecorder.dsorderdt AS orderreceivedate, 
  tbecorder.c_dsorderkbn AS orderreceivetype, 
  tbecorder.dirouteid AS orderingwaycode, 
  tbecorder.dielimflg AS deleteflag, 
  tbecorder.dicancel AS cancelflag, 
  sum(
    (
      tbecordermeisai.diusualprc * tbecordermeisai.diitemnum
    )
  ) AS total, 
  tbecorder.dsprep AS insertdate, 
  tbecorder.diprepusr AS insertid, 
  tbecorder.direnusr AS userupdateid, 
  tbecorder.dsprep AS userinsertdate, 
  tbecorder.diprepusr AS userinsertid, 
  tbecorder.c_diexchangepoint AS usepoint, 
  tbusrpram.dsctrlcd AS webcustomerid, 
  tbecorder.c_dikakutokuyoteipoint AS grantpoint, 
  tbecorder.dihaisokeitai AS transcode, 
  tbecorder.c_diuketsukeusrid, 
  tbecorder.c_diintroduceid, 
  tbecorder.c_diinputusrid, 
  trim(
    (tbecorder.dsordermemo):: text
  ) AS dsordermemo 
FROM 
  (
    (
      tbecorder 
      JOIN tbecordermeisai ON (
        (
          tbecorder.diorderid = tbecordermeisai.diorderid
        )
      )
    ) 
    JOIN tbusrpram ON (
      (
        tbusrpram.diusrid = tbecorder.diecusrid
      )
    )
  ) 
GROUP BY 
  tbecorder.diorderid, 
  tbecorder.diecusrid, 
  tbecorder.dsorderdt, 
  tbecorder.c_dsorderkbn, 
  tbecorder.dirouteid, 
  tbecorder.dielimflg, 
  tbecorder.dicancel, 
  tbecorder.dsprep, 
  tbecorder.diprepusr, 
  tbecorder.direnusr, 
  tbecorder.c_diexchangepoint, 
  tbusrpram.dsctrlcd, 
  tbecorder.c_dikakutokuyoteipoint, 
  tbecorder.dihaisokeitai, 
  tbecorder.c_diuketsukeusrid, 
  tbecorder.c_diintroduceid, 
  tbecorder.c_diinputusrid, 
  trim(
    (tbecorder.dsordermemo):: text
  )
)
select * from final