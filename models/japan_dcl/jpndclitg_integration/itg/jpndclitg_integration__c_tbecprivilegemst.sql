{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                    delete from {{this}} where c_diprivilegeid in (select c_diprivilegeid from {{ source('jpdclsdl_raw', 'c_tbecprivilegemst') }});
                    {% endif %}"
    )
}}

with source as(
    select * from {{ source('jpdclsdl_raw', 'c_tbecprivilegemst') }}
),
final as(
    select 
        c_diprivilegeid::number(38,0) as c_diprivilegeid,
        c_dicampaignid::number(38,0) as c_dicampaignid,
        c_dsprivilegename::varchar(192) as c_dsprivilegename,
        c_dsprivilegenameadm::varchar(192) as c_dsprivilegenameadm,
        c_didispflg::varchar(1) as c_didispflg,
        c_dsprivilegefrom::timestamp_ntz(9) as c_dsprivilegefrom,
        c_dsprivilegeto::timestamp_ntz(9) as c_dsprivilegeto,
        c_dioneoffflg::varchar(1) as c_dioneoffflg,
        c_dibuyitemuseflg::varchar(1) as c_dibuyitemuseflg,
        c_dibuycateuseflg::varchar(1) as c_dibuycateuseflg,
        c_dibuyprcuseflg::varchar(1) as c_dibuyprcuseflg,
        c_dibuyprcflg::varchar(1) as c_dibuyprcflg,
        c_dibuyprc::number(38,0) as c_dibuyprc,
        c_dibuyprceveryprc::number(38,0) as c_dibuyprceveryprc,
        c_dipromotionuseflg::varchar(1) as c_dipromotionuseflg,
        c_dirouteuseflg::varchar(1) as c_dirouteuseflg,
        c_diprivilegecodeuseflg::varchar(1) as c_diprivilegecodeuseflg,
        c_dsprivilegecode::varchar(48) as c_dsprivilegecode,
        c_diusrlistuseflg::varchar(1) as c_diusrlistuseflg,
        c_diusrlistflg::varchar(1) as c_diusrlistflg,
        c_diusrlisttargid::number(38,0) as c_diusrlisttargid,
        c_diusrlistsrchid::number(38,0) as c_diusrlistsrchid,
        c_dibirthmonthuseflg::varchar(1) as c_dibirthmonthuseflg,
        c_dibuytimesuseflg::varchar(1) as c_dibuytimesuseflg,
        c_dibuytimesfrom::number(18,0) as c_dibuytimesfrom,
        c_dibuytimesto::number(18,0) as c_dibuytimesto,
        c_dimembclassuseflg::varchar(1) as c_dimembclassuseflg,
        c_diprivilegeshubetsu::varchar(1) as c_diprivilegeshubetsu,
        c_dipresentselectflg::varchar(1) as c_dipresentselectflg,
        c_dipresentselectnum::number(38,0) as c_dipresentselectnum,
        c_dipointreductionflg::varchar(1) as c_dipointreductionflg,
        c_dipointreductionpoint::number(38,0) as c_dipointreductionpoint,
        c_dipointreductionrate::number(38,0) as c_dipointreductionrate,
        c_didiscountprc::number(38,0) as c_didiscountprc,
        c_didiscountprcflg::varchar(1) as c_didiscountprcflg,
        c_didiscountprckubun::varchar(1) as c_didiscountprckubun,
        c_didiscountrate::number(38,0) as c_didiscountrate,
        c_didiscountrateflg::varchar(1) as c_didiscountrateflg,
        c_disubscriptionkubun::varchar(1) as c_disubscriptionkubun,
        c_disubscriptionflg::varchar(1) as c_disubscriptionflg,
        c_disubscription::number(18,0) as c_disubscription,
        c_disubscriptiononeoffflg::number(18,0) as c_disubscriptiononeoffflg,
        dsprep::timestamp_ntz(9) as dsprep,
        dsprep::timestamp_ntz(9) as dsren,
        dselim::timestamp_ntz(9) as dselim,
        diprepusr::number(38,0) as diprepusr,
        direnusr::number(38,0) as direnusr,
        dielimusr::number(38,0) as dielimusr,
        dielimflg::varchar(1) as dielimflg,
        c_dideptid::number(38,0) as c_dideptid,
        c_dsprivilegekbn::varchar(3) as c_dsprivilegekbn,
        c_dsteikitekiyoflg::varchar(1) as c_dsteikitekiyoflg,
        c_dstsujotekiyoflg::varchar(1) as c_dstsujotekiyoflg,
        c_dsprivilegekikankbn::varchar(1) as c_dsprivilegekikankbn,
        c_diusetimesnum::number(38,0) as c_diusetimesnum,
        c_dsenforcedflg::varchar(1) as c_dsenforcedflg,
        c_dsbuyprcdecisionkbn::varchar(3) as c_dsbuyprcdecisionkbn,
        c_dibuyprcfrom::number(38,0) as c_dibuyprcfrom,
        c_dibuyprcto::number(38,0) as c_dibuyprcto,
        c_dstrnsuseflg::varchar(1) as c_dstrnsuseflg,
        c_dsbuytimessampleflg::varchar(1) as c_dsbuytimessampleflg,
        c_dspresentsendkbn::varchar(3) as c_dspresentsendkbn,
        c_dspointreductiontargetkbn::varchar(3) as c_dspointreductiontargetkbn,
        c_dipointreductionitemid::number(38,0) as c_dipointreductionitemid,
        c_dsnohinshooutputflg::varchar(1) as c_dsnohinshooutputflg,
        c_dsbuyitemoutflg::varchar(1) as c_dsbuyitemoutflg,
        c_dsbuycategoryoutflg::varchar(4) as c_dsbuycategoryoutflg,
        source_file_date::varchar(10) as source_file_date,
        current_timestamp()::timestamp_ntz(9)  as inserted_date,
        inserted_by::varchar(10) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        updated_by::varchar(100) as updated_by
    from source
)
select * from final