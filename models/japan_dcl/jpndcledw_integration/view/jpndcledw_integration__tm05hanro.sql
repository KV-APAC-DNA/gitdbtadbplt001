with tbecsalesroutemst as (
select * from {{ ref('jpndclitg_integration__tbecsalesroutemst') }}
),
final as (
SELECT 
  tbecsalesroutemst.dirouteid AS hanrocode, 
  tbecsalesroutemst.dsroutename AS hanroname, 
  '' AS konyudaibuncode, 
  '' AS konyuchubuncode, 
  '' AS konyusyobuncode, 
  '' AS konyusaibuncode 
FROM 
  tbecsalesroutemst
)
select * from final
