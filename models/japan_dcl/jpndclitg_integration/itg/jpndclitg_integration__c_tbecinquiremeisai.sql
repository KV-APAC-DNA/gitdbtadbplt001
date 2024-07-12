{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                    delete from {{this}} where dimeisaiid in (select dimeisaiid from {{ source('jpdclsdl_raw', 'c_tbecinquiremeisai') }}) and
                    diinquireid	in (select diinquireid from {{ source('jpdclsdl_raw', 'c_tbecinquiremeisai') }}) and
                    diinquirekesaiid in (select diinquirekesaiid from {{ source('jpdclsdl_raw', 'c_tbecinquiremeisai') }});
                    {% endif %}"
    )
}}

with source as(
    select * from {{ source('jpdclsdl_raw', 'c_tbecinquiremeisai') }}
),
final as(
    select 
        dimeisaiid::number(38,0) as dimeisaiid,
        diinquireid::number(38,0) as diinquireid,
        diinquirekesaiid::number(38,0) as diinquirekesaiid,
        c_dikesaiid::number(38,0) as c_dikesaiid,
        diordermeisaiid::number(38,0) as diordermeisaiid,
        c_dshenpinmssts::varchar(1) as c_dshenpinmssts,
        dihenpinkubun::varchar(3) as dihenpinkubun,
        c_dskoukankbn::varchar(3) as c_dskoukankbn,
        dihenpinriyuid::number(38,0) as dihenpinriyuid,
        dihenpinsaki::number(38,0) as dihenpinsaki,
        disetid::number(38,0) as disetid,
        diid::number(38,0) as diid,
        c_diitemtype::varchar(3) as c_diitemtype,
        dsitemid::varchar(45) as dsitemid,
        dsitemname::varchar(192) as dsitemname,
        c_dsitemnameryaku::varchar(192) as c_dsitemnameryaku,
        c_dihenpinnum::number(38,0) as c_dihenpinnum,
        c_disetitemprc::number(38,0) as c_disetitemprc,
        c_dinoshinshoitemprc::number(38,0) as c_dinoshinshoitemprc,
        diusualprc::number(38,0) as diusualprc,
        diitemprc::number(38,0) as diitemprc,
        ditotalprc::number(38,0) as ditotalprc,
        diitemtax::number(38,0) as diitemtax,
        diitemnum::number(38,0) as diitemnum,
        c_diitemtotalprc::number(38,0) as c_diitemtotalprc,
        c_didiscountmeisai::number(38,0) as c_didiscountmeisai,
        c_diadjust::number(38,0) as c_diadjust,
        c_didiscountrate::number(38,0) as c_didiscountrate,
        c_dspointitemflg::varchar(1) as c_dspointitemflg,
        c_dipresentgrantcord::number(38,0) as c_dipresentgrantcord,
        difrommeisaiid::number(38,0) as difrommeisaiid,
        difrominquireid::number(38,0) as difrominquireid,
        difrominquirekesaiid::number(38,0) as difrominquirekesaiid,
        ditomeisaiid::number(38,0) as ditomeisaiid,
        ditoinquireid::number(38,0) as ditoinquireid,
        ditoinquirekesaiid::number(38,0) as ditoinquirekesaiid,
        dioyaordermeisaiid::number(38,0) as dioyaordermeisaiid,
        c_diorderlineno::number(38,0) as c_diorderlineno,
        c_dikesailineno::number(38,0) as c_dikesailineno,
        c_dinondispflg::varchar(1) as c_dinondispflg,
        c_diusepoint::number(18,0) as c_diusepoint,
        current_timestamp()::timestamp_ntz(9) as dsprep,
        current_timestamp()::timestamp_ntz(9) as dsren,
        dselim::timestamp_ntz(9) as dselim,
        diprepusr::number(38,0) as diprepusr,
        direnusr::number(38,0) as direnusr,
        dielimusr::number(38,0) as dielimusr,
        dielimflg::varchar(1) as dielimflg,
        c_dihenpinrenkeinum::number(38,0) as c_dihenpinrenkeinum,
        c_dihenpinzuminum::number(38,0) as c_dihenpinzuminum,
        c_diexchangenum::number(38,0) as c_diexchangenum,
        c_dstaxkbn::varchar(3) as c_dstaxkbn,
        ditaxrate::number(38,0) as ditaxrate,
        NULL::varchar(10) as source_file_date,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        NULL::varchar(10) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        NULL::varchar(100) as updated_by
    from source
)
select * from final