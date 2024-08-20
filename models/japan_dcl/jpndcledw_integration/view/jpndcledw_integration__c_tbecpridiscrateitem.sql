with c_tbecpridiscrateitem as(
select * from {{ ref('jpndclitg_integration__c_tbecpridiscrateitem') }}
)
,final as(
select c_tbecpridiscrateitem.diprivilegeid,
    c_tbecpridiscrateitem.diid,
    c_tbecpridiscrateitem.dsprep,
    c_tbecpridiscrateitem.dsren,
    c_tbecpridiscrateitem.dselim,
    c_tbecpridiscrateitem.diprepusr,
    c_tbecpridiscrateitem.direnusr,
    c_tbecpridiscrateitem.dielimusr,
    c_tbecpridiscrateitem.dielimflg
from c_tbecpridiscrateitem
)
select * from final