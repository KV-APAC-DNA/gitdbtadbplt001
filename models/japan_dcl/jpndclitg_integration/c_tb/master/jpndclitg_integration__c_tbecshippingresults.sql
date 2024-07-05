{{
    config
    (
        materialized='incremental',
        incremental_strategy= 'delete+insert',
        unique_key= ['c_dishippingresultsid']
    )
}}

with source as
(
    select * from {{ source('jpndclsdl_raw', 'c_tbecshippingresults') }}
),

final as
(
    select 
		c_dishippingresultsid::number(38,0) as c_dishippingresultsid,
		diorderid::number(38,0) as diorderid,
		dimeisaiid::number(38,0) as dimeisaiid,
		diordercode::varchar(18) as diordercode,
		c_dicoopmeisaiid::number(38,0) as c_dicoopmeisaiid,
		c_dicoopedaban::number(38,0) as c_dicoopedaban,
		divouchercode::varchar(768) as divouchercode,
		c_dsshukkadate::timestamp_ntz(9) as c_dsshukkadate,
		c_dihinmokunum::number(38,0) as c_dihinmokunum,
		dirouteid::number(38,0) as dirouteid,
		c_dicommonwithdrawal::number(38,0) as c_dicommonwithdrawal,
		c_diroutewithdrawal::number(38,0) as c_diroutewithdrawal,
		dichakkasts::number(38,0) as dichakkasts,
		diid::number(38,0) as diid,
		dsprep::timestamp_ntz(9) as dsprep,
		dsren::timestamp_ntz(9) as dsren,
		dselim::timestamp_ntz(9) as dselim,
		diprepusr::number(38,0) as diprepusr,
		direnusr::number(38,0) as direnusr,
		dielimusr::number(38,0) as dielimusr,
		dielimflg::varchar(1) as dielimflg,
		c_dikesaiid::number(38,0) as c_dikesaiid,
		c_dsshukkasumimailsendkbn::varchar(1) as c_dsshukkasumimailsendkbn,
		disokoid::number(38,0) as disokoid,
		c_dichanelid::number(38,0) as c_dichanelid,
		null::varchar(10) as source_file_date,
		inserted_date::timestamp_ntz(9) as inserted_date,
		null::varchar(100) as inserted_by,
		updated_date::timestamp_ntz(9) as updated_date,
		null::varchar(100) as updated_by
    from source
)

select * from final