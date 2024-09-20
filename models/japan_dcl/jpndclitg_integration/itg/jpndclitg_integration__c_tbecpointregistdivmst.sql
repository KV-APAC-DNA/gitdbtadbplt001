{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
            delete from {{this}} where diregistdivcode in (select diregistdivcode from {{ source('jpdclsdl_raw', 'c_tbecpointregistdivmst') }});
                    {% endif %}"
    )
}}

with source as(
    select * from {{ source('jpdclsdl_raw', 'c_tbecpointregistdivmst') }}
),
final as(
    select 
        diregistdivcode::varchar(7) as diregistdivcode,
        c_dsregistdivname::varchar(60) as c_dsregistdivname,
        c_dspointtype::varchar(3) as c_dspointtype,
        c_dinondispflg::varchar(1) as c_dinondispflg,
        disortid::number(38,0) as disortid,
        dsprep::timestamp_ntz(9) as dsprep,
        dsren::timestamp_ntz(9) as dsren,
        dselim::timestamp_ntz(9) as dselim,
        diprepusr::number(38,0) as diprepusr,
        direnusr::number(38,0) as direnusr,
        dielimusr::number(38,0) as dielimusr,
        dielimflg::varchar(1) as dielimflg,
        source_file_date::varchar(10) as source_file_date,
		current_timestamp()::timestamp_ntz(9) as inserted_date,
		inserted_by::varchar(100) as inserted_by,
		current_timestamp()::timestamp_ntz(9) as updated_date,
		updated_by::varchar(100) as updated_by
    from source
)
select * from final