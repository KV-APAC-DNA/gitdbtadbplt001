{{
    config
    (
        materialized='incremental',
        incremental_strategy= 'delete+insert',
        unique_key= ['c_diusrid']
    )
}}

with source as
(
    select * from {{ source('jpdclsdl_raw', 'c_tbecregnumitemconvhist') }}
),

final as
(
    select 
		c_tbecregnumitemconvhistid::number(18,0) as c_tbecregnumitemconvhistid,
		regnumitemconvid::number(10,0)as regnumitemconvid,
		diolddiid::number(10,0) as diolddiid,
		numberoftimes::number(10,0) as numberoftimes,
		dinewdiid::number(10,0)as dinewdiid,
		c_diusrid::varchar(20)as c_diusrid,
		c_diregularcontractid::number(10,0) as c_diregularcontractid,
		c_dsregularmeisaiid::number(10,0) as c_dsregularmeisaiid,
		dsprep::timestamp_ntz(9) as dsprep,
		dsren::timestamp_ntz(9) as dsren,
		diprepusr::number(10,0)as diprepusr,
		direnusr::number(10,0)as direnusr,
		null::varchar(30)as source_file_date,
		current_timestamp()::timestamp_ntz(9) as inserted_date,
		null::varchar(100)as inserted_by,
		current_timestamp()::timestamp_ntz(9)as updated_date,
		null::varchar(9)as updated_by
    from source
)

select * from final