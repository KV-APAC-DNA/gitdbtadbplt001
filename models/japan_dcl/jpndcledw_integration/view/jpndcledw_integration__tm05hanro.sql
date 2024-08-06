with tbecsalesroutemst as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.TBECSALESROUTEMST
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
