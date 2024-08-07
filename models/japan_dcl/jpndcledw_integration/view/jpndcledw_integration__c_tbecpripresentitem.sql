with C_TBECPRIPRESENTITEM as 
(
select * from SNAPJPDCLITG_INTEGRATION.C_TBECPRIPRESENTITEM
)
,final as 
(
SELECT c_tbecpripresentitem.diprivilegeid,
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
FROM C_TBECPRIPRESENTITEM
)
select * from final
