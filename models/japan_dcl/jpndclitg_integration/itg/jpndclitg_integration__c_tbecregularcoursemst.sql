{{
    config
    (
        materialized='incremental',
        incremental_strategy= 'delete+insert',
        unique_key= ['c_diregularcourseid']
    )
}}

with source as
(
    select * from {{ source('jpdclsdl_raw', 'c_tbecregularcoursemst') }}
),

final as
(
    select 
		c_diregularcourseid::number(38,0)  as c_diregularcourseid,
		c_dsregularcoursename::varchar(100) as c_dsregularcoursename,
		c_dsregularcoursenameryaku::varchar(20) as c_dsregularcoursenameryaku,
		c_dsregularclasskbn::varchar(10) as c_dsregularclasskbn,
		didiscrate::number(38,0) as didiscrate,
		c_didisporder::number(38,0) as c_didisporder,
		c_diflg::varchar(10) as c_diflg,
		dsprep::timestamp_ntz(9) as dsprep,
		dsren::timestamp_ntz(9) as dsren,
		dselim::timestamp_ntz(9) as dselim,
		diprepusr::number(38,0) as diprepusr,
		direnusr::number(38,0) as direnusr,
		dielimusr::number(38,0) as dielimusr,
		dielimflg::varchar(10) as dielimflg,
		null::varchar(10) as source_file_date,
		inserted_date::timestamp_ntz(9) as inserted_date,
		null::varchar(100) as inserted_by,
		updated_date::timestamp_ntz(9) as updated_date,
		null::varchar(100) as updated_by
    from source
)

select * from final