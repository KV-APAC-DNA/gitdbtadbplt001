with C_TBECPRIDISCRATEITEM as(
select * from SNAPJPDCLITG_INTEGRATION.C_TBECPRIDISCRATEITEM
)
,final as(
SELECT c_tbecpridiscrateitem.diprivilegeid,
    c_tbecpridiscrateitem.diid,
    c_tbecpridiscrateitem.dsprep,
    c_tbecpridiscrateitem.dsren,
    c_tbecpridiscrateitem.dselim,
    c_tbecpridiscrateitem.diprepusr,
    c_tbecpridiscrateitem.direnusr,
    c_tbecpridiscrateitem.dielimusr,
    c_tbecpridiscrateitem.dielimflg
FROM C_TBECPRIDISCRATEITEM
)
select * from final