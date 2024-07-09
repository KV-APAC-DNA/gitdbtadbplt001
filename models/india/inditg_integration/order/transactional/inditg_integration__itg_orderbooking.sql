{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                    delete from {{this}} WHERE createdDate >= (SELECT min(s.createdDate)
                    FROM {{ source('indsdl_raw', 'sdl_csl_orderbooking') }} s);
                    {% endif %}"
    )
}}

with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_csl_orderbooking') }}
),
final as 
(
    select
        distcode::varchar(50) as distcode,
        orderno::varchar(50) as orderno,
        orderdate::timestamp_ntz(9) as orderdate,
        orddlvdate::timestamp_ntz(9) as orddlvdate,
        allowbackorder::varchar(50) as allowbackorder,
        ordtype::varchar(50) as ordtype,
        ordpriority::varchar(50) as ordpriority,
        orddocref::varchar(100) as orddocref,
        remarks::varchar(500) as remarks,
        roundoffamt::number(38,6) as roundoffamt,
        ordtotalamt::number(38,6) as ordtotalamt,
        salesmancode::varchar(100) as salesmancode,
        salesmanname::varchar(200) as salesmanname,
        salesroutecode::varchar(100) as salesroutecode,
        salesroutename::varchar(200) as salesroutename,
        rtrid::varchar(50) as rtrid,
        rtrcode::varchar(100) as rtrcode,
        rtrname::varchar(100) as rtrname,
        prdcode::varchar(50) as prdcode,
        prdbatcde::varchar(50) as prdbatcde,
        prdqty::number(18,0) as prdqty,
        prdbilledqty::number(18,0) as prdbilledqty,
        prdselrate::number(38,6) as prdselrate,
        prdgrossamt::number(38,6) as prdgrossamt,
        uploadflag::varchar(10) as uploadflag,
        recorddate::timestamp_ntz(9) as recorddate,
        createddate::timestamp_ntz(9) as createddate,
        syncid::number(38,0) as syncid,
        recommendedsku::varchar(10) as recommendedsku,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final
