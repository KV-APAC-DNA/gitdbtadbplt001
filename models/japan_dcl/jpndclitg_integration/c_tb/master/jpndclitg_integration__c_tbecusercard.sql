{{
    config
    (
        materialized='incremental',
        incremental_strategy= 'delete+insert',
        unique_key= ['c_dicardid']
    )
}}

with source as
(
    select * from {{ source('jpndclsdl_raw', 'c_tbecusercard') }}
),

final as
(
    select 
        c_dicardid::number(38,0) as c_dicardid,
        diecusrid::number(38,0) as diecusrid,
        c_dscardcompanyid::number(38,0) as c_dscardcompanyid,
        c_dstieupcardflg::varchar(1) as c_dstieupcardflg,
        c_dsgmomemberid::varchar(90) as c_dsgmomemberid,
        c_dsgmocardseq::number(38,0) as c_dsgmocardseq,
        dsprep::timestamp_ntz(9) as dsprep,
        dsren::timestamp_ntz(9) as dsren,
        dselim::timestamp_ntz(9) as dselim,
        diprepusr::number(38,0) as diprepusr,
        direnusr::number(38,0) as direnusr,
        dielimusr::number(38,0) as dielimusr,
        dielimflg::varchar(1) as dielimflg,
        null::varchar(10) as source_file_date,
        inserted_date::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by,
        updated_date::timestamp_ntz(9) as updated_date,
        null::varchar(100) as updated_by
    from source
)

select * from final