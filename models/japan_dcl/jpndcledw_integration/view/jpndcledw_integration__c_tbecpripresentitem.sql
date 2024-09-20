with c_tbecpripresentitem as 
(
select * from {{ ref('jpndclitg_integration__c_tbecpripresentitem') }}
)
,final as 
(
select c_tbecpripresentitem.diprivilegeid,
    c_tbecpripresentitem.diid,
    c_tbecpripresentitem.c_dinum,
    c_tbecpripresentitem.c_dinondispflg,
    c_tbecpripresentitem.dsprep,
    c_tbecpripresentitem.dsren,
    c_tbecpripresentitem.dselim,
    c_tbecpripresentitem.diprepusr,
    c_tbecpripresentitem.direnusr,
    c_tbecpripresentitem.dielimusr,
    c_tbecpripresentitem.dielimflg,
    c_tbecpripresentitem.disortid
from c_tbecpripresentitem
)
select * from final
