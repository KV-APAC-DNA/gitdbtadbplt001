{{
    config
    (
        materialized='incremental',
        incremental_strategy= 'delete+insert',
        unique_key= ['dipointhistid']
    )
}}

with source as
(
    select * from {{ source('jpdclsdl_raw', 'tbecpointhistory') }}
),

final as
(
    select 
        dipointhistid::number(38,0)  as dipointhistid,
		diecusrid::number(38,0) as diecusrid,
		diorderid::number(38,0) as diorderid,
		diregistdivcode::varchar(7)  as diregistdivcode,
		diidentid::number(38,0) as diidentid,
		dsidentname::varchar(384) as dsidentname,
		dipointcode::varchar(1)  as dipointcode,
		divalidflg::varchar(1)  as divalidflg,
		dipoint::number(38,0)  as dipoint,
		dspointmemo::varchar(1728) as dspointmemo,
		dspointren::timestamp_ntz(9)  as dspointren,
		c_dsvaliddate::timestamp_ntz(9) as c_dsvaliddate,
		dsprep::timestamp_ntz(9)  as dsprep,
		dsren::timestamp_ntz(9)  as dsren,
		dselim::timestamp_ntz(9) as dselim,
		diprepusr::number(38,0) as diprepusr,
		direnusr::number(38,0) as direnusr,
		dielimusr::number(38,0) as dielimusr,
		dielimflg::varchar(1) as dielimflg,
		c_dipointissueid::number(38,0) as c_dipointissueid,
		diordercode::varchar(18) as diordercode,
		c_dikesaiid::number(38,0) as c_dikesaiid,
		diinquireid::number(38,0) as diinquireid,
		c_dspointlimitdate::varchar(12)  as c_dspointlimitdate,
		c_dipointchanelid::number(38,0) as c_dipointchanelid,
		c_dideptid::number(38,0) as c_dideptid,
		c_dikokuchipoint::number(38,0) as c_dikokuchipoint,
		c_dstenpoorderno::varchar(19) as c_dstenpoorderno,
		c_dstenpoorderno2::varchar(19) as c_dstenpoorderno2,
		c_diregularreservationid::number(38,0) as c_diregularreservationid,
		source_file_date::varchar(10) as source_file_date,
		current_timestamp()::timestamp_ntz(9) as inserted_date,
		inserted_by::varchar(10) as inserted_by,
		current_timestamp()::timestamp_ntz(9) as updated_date,
		updated_by::varchar(100) as updated_by
    from source
)

select * from final