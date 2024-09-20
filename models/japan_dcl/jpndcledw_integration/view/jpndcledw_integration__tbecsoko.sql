with tbecsoko as (
select * from {{ ref('jpndclitg_integration__tbecsoko') }}
),
final as (
SELECT 
  tbecsoko.disokoid, 
  tbecsoko.dssokoname, 
  tbecsoko.dssokonameryaku, 
  tbecsoko.dsprep, 
  tbecsoko.dsren, 
  tbecsoko.dselim, 
  tbecsoko.diprepusr, 
  tbecsoko.direnusr, 
  tbecsoko.dielimusr, 
  tbecsoko.dielimflg, 
  tbecsoko.c_dsshipmenttargetflg 
FROM 
  tbecsoko tbecsoko
)
select * from final
