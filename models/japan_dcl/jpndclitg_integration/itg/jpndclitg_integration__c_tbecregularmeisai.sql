{{
    config
    (
        materialized='incremental',
        incremental_strategy= 'delete+insert',
        unique_key= ['c_dsregularmeisaiid']
    )
}}

with source as
(
    select * from {{ source('jpdclsdl_raw', 'c_tbecregularmeisai') }}
),

final as
(
    select 
		c_dsregularmeisaiid::number(38,0) as c_dsregularmeisaiid,
		c_dsdeleveryym::varchar(9) as c_dsdeleveryym,
		c_dstodokedate::timestamp_ntz(9) as c_dstodokedate,
		c_diregularcontractid::number(38,0) as c_diregularcontractid,
		c_diusrid::number(38,0) as c_diusrid,
		c_diregularcontractdate::timestamp_ntz(9) as c_diregularcontractdate,
		dirouteid::number(38,0) as dirouteid,
		diid::number(38,0) as diid,
		dsitemid::varchar(45) as dsitemid,
		c_dipromid::number(38,0) as c_dipromid,
		c_diregularcourseid::number(38,0) as c_diregularcourseid,
		ditodokeid::number(38,0) as ditodokeid,
		dstodokesei::varchar(30) as dstodokesei,
		dstodokemei::varchar(30) as dstodokemei,
		dstodokeseikana::varchar(30) as dstodokeseikana,
		dstodokemeikana::varchar(30) as dstodokemeikana,
		dstodokezip::varchar(15) as dstodokezip,
		c_dstodokeprefcd::varchar(3) as c_dstodokeprefcd,
		dstodokepref::varchar(15) as dstodokepref,
		dstodokecity::varchar(108) as dstodokecity,
		dstodokeaddr::varchar(90) as dstodokeaddr,
		dstodoketatemono::varchar(90) as dstodoketatemono,
		dstodokeaddrkana::varchar(3072) as dstodokeaddrkana,
		dstodoketel::varchar(24) as dstodoketel,
		dstodokefax::varchar(24) as dstodokefax,
		c_dsdosokbn::varchar(3) as c_dsdosokbn,
		dskessaihoho::varchar(3) as dskessaihoho,
		c_dicardid::number(38,0) as c_dicardid,
		dspaymentclass::varchar(3) as dspaymentclass,
		dihaisokeitai::number(38,0) as dihaisokeitai,
		c_dsyupacketkahiflg::varchar(1) as c_dsyupacketkahiflg,
		c_dsdeliveryrulekbn::varchar(3) as c_dsdeliveryrulekbn,
		c_dsdeliveryruledatekbn::varchar(3) as c_dsdeliveryruledatekbn,
		c_dsdeliveryruleweekkbn::varchar(3) as c_dsdeliveryruleweekkbn,
		c_dsdeliveryruledowkbn::varchar(3) as c_dsdeliveryruledowkbn,
		c_dsdeliverytime::varchar(3) as c_dsdeliverytime,
		c_dsnoshinshomemo::varchar(3000) as c_dsnoshinshomemo,
		c_dsokurijomemo::varchar(3000) as c_dsokurijomemo,
		c_dicancelflg::varchar(1) as c_dicancelflg,
		c_dsordercreatekbn::varchar(3) as c_dsordercreatekbn,
		c_dscontractchangekbn::varchar(3) as c_dscontractchangekbn,
		c_dsschedulechg05kbn::varchar(3) as c_dsschedulechg05kbn,
		c_dscategoryfreeflg::varchar(1) as c_dscategoryfreeflg,
		c_dsschedulechg02kbn::varchar(3) as c_dsschedulechg02kbn,
		c_dsschedulechg04kbn::varchar(3) as c_dsschedulechg04kbn,
		c_dsschedulechg08kbn::varchar(3) as c_dsschedulechg08kbn,
		c_dsschedulechg06kbn::varchar(3) as c_dsschedulechg06kbn,
		c_dsschedulechg07kbn::varchar(3) as c_dsschedulechg07kbn,
		c_dsschedulechg09kbn::varchar(3) as c_dsschedulechg09kbn,
		c_dsschedulechg01kbn::varchar(3) as c_dsschedulechg01kbn,
		c_dsschedulechg03kbn::varchar(3) as c_dsschedulechg03kbn,
		c_dsteikifirstflg::varchar(1) as c_dsteikifirstflg,
		c_diregularresult::number(18,0) as c_diregularresult,
		c_dsregularresultflg::varchar(1) as c_dsregularresultflg,
		c_dsschedulehaneiflg::varchar(1) as c_dsschedulehaneiflg,
		c_dsnextcreateflg::varchar(1) as c_dsnextcreateflg,
		c_dsnextdeliverydate::timestamp_ntz(9) as c_dsnextdeliverydate,
		dsprep::timestamp_ntz(9) as dsprep,
		dsren::timestamp_ntz(9) as dsren,
		dselim::timestamp_ntz(9) as dselim,
		diprepusr::number(38,0) as diprepusr,
		direnusr::number(38,0) as direnusr,
		dielimusr::number(38,0) as dielimusr,
		dielimflg::varchar(1) as dielimflg,
		c_didiscountticketissuekesaiid::number(38,0) as c_didiscountticketissuekesaiid,
		c_dsordercreateflg::varchar(1) as c_dsordercreateflg,
		null::varchar(10) as source_file_date,
		current_timestamp()::timestamp_ntz(9) as inserted_date,
		null::varchar(100) as inserted_by,
		current_timestamp()::timestamp_ntz(9) as updated_date,
		null::varchar(100) as updated_by
    from source
)

select * from final