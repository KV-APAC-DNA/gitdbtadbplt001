{{
    config
    (
        materialized='incremental',
        incremental_strategy= 'delete+insert',
        unique_key= ['c_diregularcontractid']
    )
}}

with source as
(
    select * from {{ source('jpdclsdl_raw', 'c_tbecregularcontract') }}
),

final as
(
    select 
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
		c_diuketsukeusrid::number(38,0) as c_diuketsukeusrid,
		c_dsuketsukeusrname::varchar(48) as c_dsuketsukeusrname,
		c_dsuketsuketelcompanycd::varchar(4) as c_dsuketsuketelcompanycd,
		c_diinputusrid::number(38,0) as c_diinputusrid,
		c_dsinputusrname::varchar(48) as c_dsinputusrname,
		c_dsinputtelcompanycd::varchar(4) as c_dsinputtelcompanycd,
		dsprep::timestamp_ntz(9) as dsprep,
		dsren::timestamp_ntz(9) as dsren,
		dselim::timestamp_ntz(9) as dselim,
		diprepusr::number(38,0) as diprepusr,
		direnusr::number(38,0) as direnusr,
		dielimusr::number(38,0) as dielimusr,
		dielimflg::varchar(1) as dielimflg,
		null::varchar(10) as source_file_date,
		current_timestamp()::timestamp_ntz(9) as inserted_date,
		null::varchar(100) as inserted_by,
		current_timestamp()::timestamp_ntz(9) as updated_date,
		null::varchar(100) as updated_by
    from source
)

select * from final