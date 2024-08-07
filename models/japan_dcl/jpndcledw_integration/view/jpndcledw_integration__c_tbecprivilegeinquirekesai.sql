with C_TBECPRIVILEGEINQUIREKESAI as 
(
   select * from SNAPJPDCLITG_INTEGRATION.C_TBECPRIVILEGEINQUIREKESAI
),
final as 
(
SELECT c_tbecprivilegeinquirekesai.c_diprivilegeid,
    c_tbecprivilegeinquirekesai.c_dikesaiid,
    c_tbecprivilegeinquirekesai.diinquireid,
    c_tbecprivilegeinquirekesai.c_diinquirekesaiid,
    c_tbecprivilegeinquirekesai.diorderid,
    c_tbecprivilegeinquirekesai.dimeisaino,
    c_tbecprivilegeinquirekesai.dipromid,
    c_tbecprivilegeinquirekesai.c_dsprivilegename,
    c_tbecprivilegeinquirekesai.c_dsprivilegenameadm,
    c_tbecprivilegeinquirekesai.c_dirivilegeshubetsu,
    c_tbecprivilegeinquirekesai.c_diprivilegeprc,
    c_tbecprivilegeinquirekesai.c_diprivilegenum,
    c_tbecprivilegeinquirekesai.c_diprivilegetotalprc,
    c_tbecprivilegeinquirekesai.c_dstekiyojyogaiflg,
    c_tbecprivilegeinquirekesai.c_dstekiyokbn,
    c_tbecprivilegeinquirekesai.c_diprivilegeexclusionkbn,
    c_tbecprivilegeinquirekesai.c_difrontsortorder,
    c_tbecprivilegeinquirekesai.c_dsteikitekiyoflg,
    c_tbecprivilegeinquirekesai.c_dstsujotekiyoflg,
    c_tbecprivilegeinquirekesai.c_dsnoshinshooutputflg,
    c_tbecprivilegeinquirekesai.c_dspresentsendkbn,
    c_tbecprivilegeinquirekesai.c_dipointreductionflg,
    c_tbecprivilegeinquirekesai.c_dipointreductionrate,
    c_tbecprivilegeinquirekesai.c_dipointtargetprc,
    c_tbecprivilegeinquirekesai.c_dipointgranttype,
    c_tbecprivilegeinquirekesai.c_diregdiscticketgivenid,
    c_tbecprivilegeinquirekesai.c_dinondispflg,
    c_tbecprivilegeinquirekesai.dsprep,
    c_tbecprivilegeinquirekesai.dsren,
    c_tbecprivilegeinquirekesai.dselim,
    c_tbecprivilegeinquirekesai.diprepusr,
    c_tbecprivilegeinquirekesai.direnusr,
    c_tbecprivilegeinquirekesai.dielimusr,
    c_tbecprivilegeinquirekesai.dielimflg
FROM C_TBECPRIVILEGEINQUIREKESAI
)
select * from final