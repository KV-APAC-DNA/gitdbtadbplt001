{{
    config
    (
        materialized='incremental',
        incremental_strategy= 'delete+insert',
        unique_key= ['dimeisaiid']
    )
}}

with source as
(
    select * from {{ source('jpdclsdl_raw', 'tbecordermeisai') }}
),

final as
(
    select 
		dimeisaiid::number(38,0) as dimeisaiid,
		diorderid::number(38,0) as diorderid,
		disetid::number(38,0) as disetid,
		diid::number(38,0) as diid,
		c_diitemtype::varchar(3) as c_diitemtype,
		dsitemid::varchar(45) as dsitemid,
		dsitemname::varchar(192) as dsitemname,
		c_dsitemnameryaku::varchar(192) as c_dsitemnameryaku,
		diusualprc::number(38,0) as diusualprc,
		ditotalprc::number(38,0) as ditotalprc,
		diitemtax::number(38,0) as diitemtax,
		diitemnum::number(38,0) as diitemnum,
		c_diitemtotalprc::number(38,0) as c_diitemtotalprc,
		c_didiscountmeisai::number(38,0) as c_didiscountmeisai,
		dishukkasts::varchar(6) as dishukkasts,
		c_dipresentgrantcord::number(38,0) as c_dipresentgrantcord,
		c_dikesaiid::number(38,0) as c_dikesaiid,
		c_dsregularmeisaiid::number(38,0) as c_dsregularmeisaiid,
		c_dsordertype::varchar(1) as c_dsordertype,
		c_disubscriptionkubun::varchar(1) as c_disubscriptionkubun,
		c_diregularcourseid::number(38,0) as c_diregularcourseid,
		dipromid::number(38,0) as dipromid,
		c_disetitemprc::number(38,0) as c_disetitemprc,
		c_dssetitemkbn::varchar(1) as c_dssetitemkbn,
		c_dssetitemflg::varchar(1) as c_dssetitemflg,
		c_dspickingflg::varchar(1) as c_dspickingflg,
		c_dspointflg::varchar(1) as c_dspointflg,
		c_dspointitemflg::varchar(1) as c_dspointitemflg,
		c_dsnoshinshooutputflg::varchar(1) as c_dsnoshinshooutputflg,
		dicancel::varchar(1) as dicancel,
		c_dshenpinflg::varchar(1) as c_dshenpinflg,
		dsshukaflg::varchar(1) as dsshukaflg,
		dihenpinyoteinum::number(38,0) as dihenpinyoteinum,
		c_dinoshinshoitemprc::number(38,0) as c_dinoshinshoitemprc,
		c_dssampleclasscd::varchar(15) as c_dssampleclasscd,
		c_dssamplelogicd::varchar(3) as c_dssamplelogicd,
		disokoid::number(38,0) as disokoid,
		dirouteid::number(38,0) as dirouteid,
		c_distockchanelid::number(38,0) as c_distockchanelid,
		c_dihikiatenum::number(18,0) as c_dihikiatenum,
		c_diadjustprc::number(38,0) as c_diadjustprc,
		c_dsdemandkbn::varchar(1) as c_dsdemandkbn,
		c_dsdiscpritekiyoflg::varchar(1) as c_dsdiscpritekiyoflg,
		c_didiscountrate::number(38,0) as c_didiscountrate,
		c_dsmessage::varchar(87) as c_dsmessage,
		c_diorderlineno::number(38,0) as c_diorderlineno,
		c_dikesailineno::number(38,0) as c_dikesailineno,
		c_dspotsalesno::varchar(25) as c_dspotsalesno,
		c_dipotlineno::number(38,0) as c_dipotlineno,
		disetmeisaiid::number(38,0) as disetmeisaiid,
		c_difrontsortorder::number(38,0) as c_difrontsortorder,
		c_dinondispflg::varchar(1) as c_dinondispflg,
		c_dsfreekbn::varchar(1) as c_dsfreekbn,
		c_diusepoint::number(18,0) as c_diusepoint,
		dsprep::timestamp_ntz(9) as dsprep,
		dsren::timestamp_ntz(9) as dsren,
		dselim::timestamp_ntz(9) as dselim,
		diprepusr::number(38,0) as diprepusr,
		direnusr::number(38,0) as direnusr,
		dielimusr::number(38,0) as dielimusr,
		dielimflg::varchar(1) as dielimflg,
		c_diwarnitemid::number(38,0) as c_diwarnitemid,
		c_diyoyakuhenpinflg::varchar(1) as c_diyoyakuhenpinflg,
		c_dihenpinzuminum::number(38,0) as c_dihenpinzuminum,
		c_diregulardiscountrate::number(38,0) as c_diregulardiscountrate,
		c_dstaxkbn::varchar(3) as c_dstaxkbn,
		ditaxrate::number(38,0) as ditaxrate,
		null::varchar(10) as source_file_date,
		inserted_date::timestamp_ntz(9) as inserted_date,
		null::varchar(100) as inserted_by,
		updated_date::timestamp_ntz(9) as updated_date,
		null::varchar(100) as updated_by
    from source
)

select * from final