{{
    config
    (
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_csl_salesinvoiceorders') }}
),
final as 
(
    select
        distcode::varchar(50) as distcode,
        salinvno::varchar(50) as salinvno,
        orderno::varchar(50) as orderno,
        orderdate::timestamp_ntz(9) as orderdate,
        uploadflag::varchar(10) as uploadflag,
        createddate::timestamp_ntz(9) as createddate,
        syncid::number(38,0) as syncid,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
    {% if is_incremental() %}
    --this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
)
select * from final
