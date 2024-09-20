{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                delete from {{this}} where dihenpinriyuid in (select dihenpinriyuid from {{ source('jpdclsdl_raw', 'tbechenpinriyu') }});
                    {% endif %}"
    )
}}

with source as(
    select * from {{ source('jpdclsdl_raw', 'tbechenpinriyu') }}
),
final as(
    select 
        dihenpinriyuid::number(38,0) as dihenpinriyuid,
        dshenpinriyu::varchar(48) as dshenpinriyu,
        dshenpinriyushosai::varchar(96) as dshenpinriyushosai,
        didisporder::number(38,0) as didisporder,
        c_dieditableflg::varchar(1) as c_dieditableflg,
        dsprep::timestamp_ntz(9) as dsprep,
        dsren::timestamp_ntz(9) as dsren,
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