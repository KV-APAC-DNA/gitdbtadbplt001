with tbecorder as (
select * from {{ ref('jpndcledw_integration__tbecorder') }}
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