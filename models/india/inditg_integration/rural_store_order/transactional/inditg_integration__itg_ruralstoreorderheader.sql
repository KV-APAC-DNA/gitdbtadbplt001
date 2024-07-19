{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}
with source as
(
    select * from {{ source('indsdl_raw', 'sdl_rrl_ruralstoreorderheader') }}
),
final as
(
    select
    orderid::varchar(50) as orderid,
    orderdate::timestamp_ntz(9) as orderdate,
    deliverydate::timestamp_ntz(9) as deliverydate,
    ovid::number(38,0) as ovid,
    usercode::varchar(100) as usercode,
    ordervalue::number(18,2) as ordervalue,
    linespercall::number(18,0) as linespercall,
    feedback::varchar(500) as feedback,
    orderstarttime::timestamp_ntz(9) as orderstarttime,
    orderendtime::timestamp_ntz(9) as orderendtime,
    islocked::boolean as islocked,
    flag::varchar(2) as flag,
    saletype::varchar(2) as saletype,
    retailerid::varchar(100) as retailerid,
    invoicestatus::varchar(2) as invoicestatus,
    billdiscount::number(18,2) as billdiscount,
    tax::number(18,2) as tax,
    isjointcall::boolean as isjointcall,
    ord_distributorcode::varchar(100) as ord_distributorcode,
    weekid::number(18,0) as weekid,
    rsd_code::varchar(50) as rsd_code,
    route_code::varchar(50) as route_code,
    filename::varchar(100) as filename,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source    
    {% if is_incremental() %}
    --this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
    and source.filename not in (select distinct filename from {{ this }} where filename is not null)
    {% endif %}
)
select * from final
