{{
    config
    (
        materialized='incremental',
        incremental_strategy= 'delete+insert',
        unique_key= ['diorderhistid']
    )
}}

with source as
(
    select * from {{ source('jpdclsdl_raw', 'tbecorderhist') }}
),

final as
(
    select 
		diorderhistid::number(38,0) as diorderhistid,
		diorderid::number(38,0) as diorderid,
		diecusrid::number(38,0) as diecusrid,
		c_dsdate::timestamp_ntz(9) as c_dsdate,
		c_dishorikubun::varchar(3) as c_dishorikubun,
		c_dirirekikubun::varchar(3) as c_dirirekikubun,
		c_dihandle::number(38,0) as c_dihandle,
		c_dicontent::varchar(3072) as c_dicontent,
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