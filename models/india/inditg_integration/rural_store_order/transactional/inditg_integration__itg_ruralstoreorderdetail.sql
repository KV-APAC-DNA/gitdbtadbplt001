{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{%if is_incremental()%}
    delete from {{this}} itg where itg.filename  in (select sdl.filename from
        {{ source('indsdl_raw', 'sdl_rrl_ruralstoreorderdetail') }} sdl)
        {% endif %}"
    )
}}
with source as
(
    select * from {{ source('indsdl_raw', 'sdl_rrl_ruralstoreorderdetail') }}
),
final as
(
    select
    orderid::varchar(100) as orderid,
    productid::varchar(100) as productid,
    uomid::number(18,0) as uomid,
    qty::number(38,0) as qty,
    price::number(18,2) as price,
    netprice::number(18,2) as netprice,
    discountvalue::number(38,0) as discountvalue,
    foc::number(38,0) as foc,
    tax::number(4,2) as tax,
    status::varchar(4) as status,
    flag::varchar(1) as flag,
    usercode::varchar(50) as usercode,
    ordd_distributorcode::varchar(50) as ordd_distributorcode,
    orderdate::timestamp_ntz(9) as orderdate,
    uom::varchar(50) as uom,
    filename::varchar(100) as filename,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
    {% if is_incremental() %}
    --this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
    and source.filename not in (select distinct filename from {{ this }})
    {% endif %}
)

select * from final
