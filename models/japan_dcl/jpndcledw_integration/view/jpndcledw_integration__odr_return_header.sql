with c_tbecinquire as (
select * from {{ ref('jpndclitg_integration__c_tbecinquire') }}
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