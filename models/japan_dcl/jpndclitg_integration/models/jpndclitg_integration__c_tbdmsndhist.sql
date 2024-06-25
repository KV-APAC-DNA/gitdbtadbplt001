with source as
(
    select * from {{ source('jpdclsdl_raw', 'c_tbdmsndhist') }}
),

final as
(
    select 
        c_disendid::number(38,0)  as c_disendid,
		c_diusrid::number(38,0)  as c_diusrid,
		c_dsdmnumber::varchar(48) as c_dsdmnumber,
		c_dsdmsendkubun::varchar(1)  as c_dsdmsendkubun,
		c_dsdmsenddate::timestamp_ntz(9)  as c_dsdmsenddate,
		c_dsdmname::varchar(600)  as c_dsdmname,
		c_dsextension1::varchar(768) as c_dsextension1,
		c_dsextension2::varchar(768) as c_dsextension2,
		c_dsextension3::varchar(768) as c_dsextension3,
		c_dsextension4::varchar(768) as c_dsextension4,
		c_dsextension5::varchar(768) as c_dsextension5,
		c_dsdmimportid::number(38,0) as c_dsdmimportid,
		dsprep::timestamp_ntz(9)  as dsprep,
		dsren::timestamp_ntz(9) as dsren,
		diprepusr::number(38,0)  as diprepusr,
		direnusr::number(38,0) as direnusr,
		dielimflg::varchar(1)  as dielimflg,
		dselim::timestamp_ntz(9) as dselim,
		dielimusr::number(38,0) as dielimusr,
		c_diusrchanelid::number(38,0) as c_diusrchanelid,
		null::varchar(10) as source_file_date,
		current_timestamp()::timestamp_ntz(9) as inserted_date,
		null::varchar(100) as inserted_by,
		current_timestamp()::timestamp_ntz(9) as updated_date,
		null::varchar(100) as updated_by,
		null::varchar(250) as rowid
    from source
)

select * from source