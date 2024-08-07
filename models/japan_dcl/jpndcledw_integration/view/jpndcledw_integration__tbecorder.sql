with tbecorder as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.TBECORDER
),
final as (
SELECT 
  tbecorder.diorderid, 
  tbecorder.diordercode, 
  tbecorder.diecusrid, 
  tbecorder.c_dikakutokuyoteipoint, 
  tbecorder.dsorderdt, 
  tbecorder.dihoryu, 
  tbecorder.dicancel, 
  tbecorder.dielimflg, 
  tbecorder.dsren, 
  tbecorder.dishukkasts 
FROM 
  tbecorder
)
select * from final