{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
            delete from {{this}} where c_diprivilegeid in (select c_diprivilegeid from {{ source('jpndclsdl_raw', 'c_tbecprivilegekesai') }}) and
            c_dikesaiid	in (select c_dikesaiid from {{ source('jpndclsdl_raw', 'c_tbecprivilegekesai') }});
                    {% endif %}"
    )
}}

with source as(
    select * from {{ source('jpndclsdl_raw', 'c_tbecprivilegekesai') }}
),
final as(
    select 
        c_diprivilegeid::number(38,0) as c_diprivilegeid,
        c_dikesaiid::number(38,0) as c_dikesaiid,
        diorderid::number(38,0) as diorderid,
        dipromid::number(38,0) as dipromid,
        c_dsprivilegename::varchar(192) as c_dsprivilegename,
        c_dsprivilegenameadm::varchar(192) as c_dsprivilegenameadm,
        c_dirivilegeshubetsu::number(38,0) as c_dirivilegeshubetsu,
        c_diprivilegeprc::number(38,0) as c_diprivilegeprc,
        c_diprivilegenum::number(38,0) as c_diprivilegenum,
        c_diprivilegetotalprc::number(38,0) as c_diprivilegetotalprc,
        c_diprivilegeexclusionkbn::number(38,0) as c_diprivilegeexclusionkbn,
        c_difrontsortorder::number(38,0) as c_difrontsortorder,
        c_dstekiyojyogaiflg::varchar(1) as c_dstekiyojyogaiflg,
        c_dstekiyokbn::varchar(1) as c_dstekiyokbn,
        c_dinondispflg::varchar(1) as c_dinondispflg,
        c_dsteikitekiyoflg::varchar(1) as c_dsteikitekiyoflg,
        c_dstsujotekiyoflg::varchar(1) as c_dstsujotekiyoflg,
        c_dsnoshinshooutputflg::varchar(1) as c_dsnoshinshooutputflg,
        c_dspresentsendkbn::varchar(3) as c_dspresentsendkbn,
        c_dipointreductionflg::varchar(1) as c_dipointreductionflg,
        c_dipointreductionrate::number(38,0) as c_dipointreductionrate,
        c_dipointtargetprc::number(38,0) as c_dipointtargetprc,
        c_dipointgranttype::varchar(1) as c_dipointgranttype,
        c_diregdiscticketgivenid::number(38,0) as c_diregdiscticketgivenid,
        current_timestamp()::timestamp_ntz(9) as dsprep,
        current_timestamp()::timestamp_ntz(9) as dsren,
        dselim::TIMESTAMP_NTZ(9) as DSELIM,
        diprepusr::NUMBER(38,0) as DIPREPUSR,
        direnusr::NUMBER(38,0) as DIRENUSR,
        dielimusr::NUMBER(38,0) as DIELIMUSR,
        dielimflg::VARCHAR(4) as DIELIMFLG,
        source_file_date::varchar(10) as source_file_date,
        inserted_date as inserted_date,
        inserted_by::varchar(10) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        updated_by::varchar(100) as updated_by
    from source
)
select * from final