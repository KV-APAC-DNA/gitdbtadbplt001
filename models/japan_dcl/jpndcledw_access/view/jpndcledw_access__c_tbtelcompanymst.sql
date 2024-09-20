with c_tbtelcompanymst as (
    select * from {{ ref('jpndcledw_integration__c_tbtelcompanymst') }}
),

final as (
select  
c_dstelcompanycd as "c_dstelcompanycd",
c_dstelcompayname as "c_dstelcompayname",
didisporder as "didisporder",
dielimflg as "dielimflg"
from c_tbtelcompanymst
)

select * from final