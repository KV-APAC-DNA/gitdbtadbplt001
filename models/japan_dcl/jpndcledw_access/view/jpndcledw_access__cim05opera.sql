with cim05opera as (
    select * from {{ ref('jpndcledw_integration__cim05opera') }}
)
,

final as ( 
select  
opecode as "opecode",
opename as "opename",
bumoncode as "bumoncode",
logincode as "logincode",
ciflg as "ciflg",
join_rec_upddate as "join_rec_upddate"
from cim05opera
)

select * from final