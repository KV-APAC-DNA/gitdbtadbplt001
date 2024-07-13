{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook = "{% if is_incremental() %}
        DELETE FROM {{this}}
        WHERE TO_DATE(idtmngdate) >= (SELECT MIN(TO_DATE(idtmngdate)) 
        FROM {{ source('indsdl_raw', 'sdl_rtbl_idtmanagementupload') }});
        {% endif %}"
    )
}}

with source as
(
    select * from {{ source('indsdl_raw', 'sdl_rtbl_idtmanagementupload') }}
),
final as
(
    SELECT distcode,
        idtmngrefno,
        idtmngdate,
        idtdistcode,
        idtdistname,
        lcncode,
        lcnname,
        stkmgmttype,
        docrefno,
        lrnumber,
        lrdate,
        remarks,
        status,
        idtgrossamt,
        idttaxamt,
        idtnetamt,
        idtpaidamt,
        productcode,
        productname,
        batchcode,
        systemstocktype,
        stocktype,
        baseqty,
        mrp,
        listprice,
        productgrossamt,
        producttaxamt,
        productnetamt,
        downloadstatus,
        syncid,
        serverdate,
        mnfdate,
        expdate,
        createddt,
        convert_timezone('Asia/Kolkata',current_timestamp())::timestamp_ntz AS updt_dttm
    FROM source
)
select distcode::varchar(50) as distcode,
    idtmngrefno::varchar(50) as idtmngrefno,
    idtmngdate::timestamp_ntz(9) as idtmngdate,
    idtdistcode::varchar(50) as idtdistcode,
    idtdistname::varchar(100) as idtdistname,
    lcncode::varchar(50) as lcncode,
    lcnname::varchar(200) as lcnname,
    stkmgmttype::varchar(30) as stkmgmttype,
    docrefno::varchar(100) as docrefno,
    lrnumber::varchar(100) as lrnumber,
    lrdate::timestamp_ntz(9) as lrdate,
    remarks::varchar(200) as remarks,
    status::varchar(100) as status,
    idtgrossamt::number(38,2) as idtgrossamt,
    idttaxamt::number(38,2) as idttaxamt,
    idtnetamt::number(38,2) as idtnetamt,
    idtpaidamt::number(38,2) as idtpaidamt,
    productcode::varchar(50) as productcode,
    productname::varchar(100) as productname,
    batchcode::varchar(50) as batchcode,
    systemstocktype::number(18,0) as systemstocktype,
    stocktype::varchar(50) as stocktype,
    baseqty::number(18,0) as baseqty,
    mrp::number(18,2) as mrp,
    listprice::number(18,2) as listprice,
    productgrossamt::number(38,2) as productgrossamt,
    producttaxamt::number(38,2) as producttaxamt,
    productnetamt::number(38,2) as productnetamt,
    downloadstatus::varchar(4) as downloadstatus,
    syncid::number(38,0) as syncid,
    serverdate::timestamp_ntz(9) as serverdate,
    mnfdate::timestamp_ntz(9) as mnfdate,
    expdate::timestamp_ntz(9) as expdate,
    createddt::timestamp_ntz(9) as createddt,
    updt_dttm::timestamp_ntz(9) as updt_dttm
from final