with c_tbecinquire as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.C_TBECINQUIRE
),
final as (
SELECT 
  c_tbecinquire.diinquireid AS odrreturnno, 
  lpad(
    (
      (c_tbecinquire.diecusrid):: character varying
    ):: text, 
    10, 
    ('0' :: character varying):: text
  ) AS customerid, 
  c_tbecinquire.diorderid AS odrreceiveno 
FROM 
c_tbecinquire
)
select * from final