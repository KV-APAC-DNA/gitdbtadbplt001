{{
    config(
        materialized= "incremental",
        incremental_strategy= "delete+insert",
        unique_key=['DIECUSRID', 'C_DSAGGREGATEYM']
    )
}}

with source as(
    select * from {{ source('jpndclsdl_raw', 'c_tbecranksumamount') }}
),
final as(
    select 
        diecusrid::number(38,0) as diecusrid,
        c_dsaggregateym::varchar(9) as c_dsaggregateym,
        c_dsranktotalprcbymonth::number(38,0) as c_dsranktotalprcbymonth,
        current_timestamp()::timestamp_ntz(9) as dsprep,
        current_timestamp()::timestamp_ntz(9) as dsren,
        diprepusr::number(38,0) as diprepusr,
        direnusr::number(38,0) as direnusr,
        dielimflg::varchar(1) as dielimflg,
        NULL::varchar(10) as source_file_date,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        NULL::varchar(10) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        NULL::varchar(100) as updated_by
    from source
)
select * from final