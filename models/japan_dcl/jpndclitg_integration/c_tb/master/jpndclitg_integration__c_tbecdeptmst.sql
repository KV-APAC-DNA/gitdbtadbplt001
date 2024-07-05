{{
    config
    (
        materialized='incremental',
        incremental_strategy= 'delete+insert',
        unique_key= ['c_dideptid']
    )
}}


with source as
(
    select * from {{ source('jpndclsdl_raw', 'c_tbecdeptmst') }}
),

final as
(
    select
        c_dideptid::number(38,0)  as c_dideptid,
		c_dsdeptname::varchar(96)  as c_dsdeptname,
		c_dsdeptnameryaku::varchar(24)  as c_dsdeptnameryaku,
		disortid::number(38,0) as disortid,
		c_dinondispflg::varchar(1) as c_dinondispflg,
		dsprep::timestamp_ntz(9) as dsprep,
		dsren::timestamp_ntz(9) as dsren,
		dselim::timestamp_ntz(9) as dselim,
		diprepusr::number(38,0) as diprepusr,
		direnusr::number(38,0) as direnusr,
		dielimusr::number(38,0) as dielimusr,
		dielimflg::varchar(1) as dielimflg,
		null::varchar(10) as source_file_date,
		inserted_date::timestamp_ntz(9) as inserted_date,
		null::varchar(10) as inserted_by,
		updated_date::timestamp_ntz(9) as updated_date,
		null::varchar(100) as updated_by
    from source
)

select * from final