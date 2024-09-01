{{
    config
    (
        materialized='incremental',
        incremental_strategy= 'delete+insert',
        unique_key= ['c_dstempocode']
    )
}}

with source as
(
    select * from {{ source('jpdclsdl_raw', 'c_tbecclient') }}
),

final as
(
    select 
        c_dstempocode::varchar(7)  as c_dstempocode,
        c_dstemponame::varchar(60)  as c_dstemponame,
        dirouteid::number(38,0)  as dirouteid,
        dsprep::timestamp_ntz(9) as dsprep,
        dsren::timestamp_ntz(9) as dsren,
        dselim::timestamp_ntz(9) as dselim,
        diprepusr::number(38,0) as diprepusr,
        direnusr::number(38,0) as direnusr,
        dielimusr::number(38,0) as dielimusr,
        dielimflg::varchar(4) as dielimflg,
        source_file_date::varchar(10) as source_file_date,
		current_timestamp()::timestamp_ntz(9) as inserted_date,
		inserted_by::varchar(100) as inserted_by,
		current_timestamp()::timestamp_ntz(9) as updated_date,
		updated_by::varchar(100) as updated_by
    from source
)

select * from final