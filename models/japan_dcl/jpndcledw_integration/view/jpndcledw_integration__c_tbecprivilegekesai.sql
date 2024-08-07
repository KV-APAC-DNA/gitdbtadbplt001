with C_TBECPRIVILEGEKESAI as
(
    select * from SNAPJPDCLITG_INTEGRATION.C_TBECPRIVILEGEKESAI
), final as 
(
SELECT c_tbecprivilegekesai.c_diprivilegeid,
    c_tbecprivilegekesai.c_dikesaiid,
    c_tbecprivilegekesai.diorderid,
    c_tbecprivilegekesai.dipromid,
    c_tbecprivilegekesai.c_dsprivilegename,
    c_tbecprivilegekesai.c_dsprivilegenameadm,
    c_tbecprivilegekesai.c_dirivilegeshubetsu,
    c_tbecprivilegekesai.c_diprivilegeprc,
    c_tbecprivilegekesai.c_diprivilegenum,
    c_tbecprivilegekesai.c_diprivilegetotalprc,
    c_tbecprivilegekesai.c_diprivilegeexclusionkbn,
    c_tbecprivilegekesai.c_difrontsortorder,
    c_tbecprivilegekesai.c_dstekiyojyogaiflg,
    c_tbecprivilegekesai.c_dstekiyokbn,
    c_tbecprivilegekesai.c_dinondispflg,
    c_tbecprivilegekesai.c_dsteikitekiyoflg,
    c_tbecprivilegekesai.c_dstsujotekiyoflg,
    c_tbecprivilegekesai.c_dsnoshinshooutputflg,
    c_tbecprivilegekesai.c_dspresentsendkbn,
    c_tbecprivilegekesai.c_dipointreductionflg,
    c_tbecprivilegekesai.c_dipointreductionrate,
    c_tbecprivilegekesai.c_dipointtargetprc,
    c_tbecprivilegekesai.c_dipointgranttype,
    c_tbecprivilegekesai.c_diregdiscticketgivenid,
    c_tbecprivilegekesai.dsprep,
    c_tbecprivilegekesai.dsren,
    c_tbecprivilegekesai.dselim,
    c_tbecprivilegekesai.diprepusr,
    c_tbecprivilegekesai.direnusr,
    c_tbecprivilegekesai.dielimusr,
    c_tbecprivilegekesai.dielimflg
FROM C_TBECPRIVILEGEKESAI
)
select * from final
