{{
    config
    (
        materialized='incremental',
        incremental_strategy= 'delete+insert',
        unique_key= ['c_dscardcompanyid']
    )
}}

with source as
(
    select * from {{ source('jpdclsdl_raw', 'c_tbeccardcompanymst') }}
),

final as
(
    select 
        c_dscardcompanyid::number(38,0) as c_dscardcompanyid,
		c_dscardcompanyname::varchar(96) as c_dscardcompanyname,
		c_dsnotrevoflg::varchar(1) as c_dsnotrevoflg,
		c_dsnotdivideflg::varchar(1) as c_dsnotdivideflg,
		c_dsmaxdividetimes::varchar(1) as c_dsmaxdividetimes,
		c_dsnotbonusflg::varchar(1) as c_dsnotbonusflg,
		c_dsnotbonusmdfrom1::varchar(6) as c_dsnotbonusmdfrom1,
		c_dsnotbonusmdto1::varchar(6) as c_dsnotbonusmdto1,
		c_dsnotbonusmdfrom2::varchar(6) as c_dsnotbonusmdfrom2,
		c_dsnotbonusmdto2::varchar(6) as c_dsnotbonusmdto2,
		c_dsnotbonusmdfrom3::varchar(6) as c_dsnotbonusmdfrom3,
		c_dsnotbonusmdto3::varchar(6) as c_dsnotbonusmdto3,
		c_dscardnoformat::varchar(10) as c_dscardnoformat,
		dsprep::timestamp_ntz(9) as dsprep,
		dsren::timestamp_ntz(9) as dsren,
		dselim::timestamp_ntz(9) as dselim,
		diprepusr::number(38,0) as diprepusr,
		direnusr::number(38,0) as direnusr,
		dielimusr::number(38,0) as dielimusr,
		dielimflg::varchar(1) as dielimflg,
		null::varchar(10) as source_file_date,
		inserted_date::timestamp_ntz(9) as inserted_date,
		null::varchar(100) as inserted_by,
		updated_date::timestamp_ntz(9) as updated_date,
		null::varchar(100) as updated_by
    from source
)

select * from final