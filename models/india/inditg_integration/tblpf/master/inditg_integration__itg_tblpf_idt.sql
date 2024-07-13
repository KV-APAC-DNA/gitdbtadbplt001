{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook = "{% if is_incremental() %}
        DELETE FROM {{this}}
        WHERE (MONTH,YEAR) IN (SELECT DISTINCT MONTH, YEAR FROM {{ ref('inditg_integration__itg_tblpf_idt_wrk') }});
        {% endif %}"
    )
}}

with source as
(
    select * from {{ ref('inditg_integration__itg_tblpf_idt_wrk') }}
),
final as
(
    SELECT serno,
        distcode,
        prdcode,
        idtmngdate,
        stkmgmttype,
        status,
        stocktype,
        COALESCE(baseqty,0) AS baseqty,
        COALESCE(mrp,0) AS mrp,
        COALESCE(listprice,0) AS listprice,
        COALESCE(nr,0) AS nr,
        COALESCE(ptr,0) AS ptr,
        month,
        year,
        src,
        crt_dttm,
        updt_dttm
        FROM source
)
select serno::varchar(50) as serno,
    distcode::varchar(50) as distcode,
    prdcode::varchar(50) as prdcode,
    idtmngdate::timestamp_ntz(9) as idtmngdate,
    stkmgmttype::varchar(30) as stkmgmttype,
    status::varchar(100) as status,
    stocktype::varchar(50) as stocktype,
    baseqty::number(18,0) as baseqty,
    mrp::number(18,2) as mrp,
    listprice::number(18,2) as listprice,
    nr::number(18,2) as nr,
    ptr::number(18,2) as ptr,
    month::varchar(50) as month,
    year::varchar(50) as year,
    src::varchar(10) as src,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
from final