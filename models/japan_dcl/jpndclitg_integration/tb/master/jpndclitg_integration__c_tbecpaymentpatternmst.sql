{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                delete from {{this}} where c_dspaymentptnkbn in (select c_dspaymentptnkbn from {{ source('jpndclsdl_raw', 'c_tbecpaymentpatternmst') }});
                        {% endif %}"
    )
}}

with source as(
    select * from {{ source('jpndclsdl_raw', 'c_tbecpaymentpatternmst') }}
),
final as(
    select 
        c_dspaymentptnkbn::varchar(3) as c_dspaymentptnkbn,
        c_dspaymentlongname::varchar(48) as c_dspaymentlongname,
        c_dspaymentnameryaku::varchar(15) as c_dspaymentnameryaku,
        c_dspaymentoutputname::varchar(15) as c_dspaymentoutputname,
        c_dseffectflg::varchar(1) as c_dseffectflg,
        didisporder::number(38,0) as didisporder,
        c_dsteikitekiyoflg::varchar(1) as c_dsteikitekiyoflg,
        current_timestamp()::timestamp_ntz(9) as dsprep,
        current_timestamp()::timestamp_ntz(9) as dsren,
        dselim::timestamp_ntz(9) as dselim,
        diprepusr::number(38,0) as diprepusr,
        direnusr::number(38,0) as direnusr,
        dielimusr::number(38,0) as dielimusr,
        dielimflg::varchar(1) as dielimflg,
        NULL::varchar(10) as source_file_date,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        NULL::varchar(10) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        NULL::varchar(100) as updated_by
    from source
)
select * from final