{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook = "{% if is_incremental() %}
        DELETE FROM {{this}}
        WHERE (mon,yr) IN (SELECT DISTINCT mon, yr FROM {{ ref('inditg_integration__itg_tblpf_clstkm_wrk') }});
        {% endif %}"
    )
}}

with source as
(
    select * from {{ ref('inditg_integration__itg_tblpf_clstkm_wrk') }}
),
final as
(
    SELECT serno,
       mon,
       yr,
       prdcode,
       distcode,
       COALESCE(salopenstock,0) AS salopenstock,
       COALESCE(unsalopenstock,0) AS unsalopenstock,
       COALESCE(offeropenstock,0) AS offeropenstock,
       COALESCE(unsalclsstock,0) AS unsalclsstock,
       COALESCE(offerclsstock,0) AS offerclsstock,
       COALESCE(clstckqty,0) AS clstckqty,
       COALESCE(lp,0) AS lp,
       COALESCE(ptr,0) AS ptr,
       COALESCE(nr,0) AS nr,
       src,
       COALESCE(value,0) AS value,
       COALESCE(lpvalue,0) AS lpvalue,
       COALESCE(ptrvalue,0) AS ptrvalue,
       iscubeprocess,
       COALESCE(salclsnrvalue,0) AS salclsnrvalue,
       COALESCE(unsalclsnrvalue,0) AS unsalclsnrvalue,
       COALESCE(offerclsnrvalue,0) AS offerclsnrvalue,
       crt_dttm,
       updt_dttm
    FROM source
)
select serno::number(38,0) as serno,
    mon::number(18,0) as mon,
    yr::number(18,0) as yr,
    prdcode::varchar(50) as prdcode,
    distcode::varchar(50) as distcode,
    salopenstock::number(18,3) as salopenstock,
    unsalopenstock::number(18,3) as unsalopenstock,
    offeropenstock::number(18,3) as offeropenstock,
    unsalclsstock::number(18,3) as unsalclsstock,
    offerclsstock::number(18,3) as offerclsstock,
    clstckqty::number(18,3) as clstckqty,
    lp::number(18,3) as lp,
    ptr::number(18,3) as ptr,
    nr::number(18,3) as nr,
    src::varchar(10) as src,
    value::number(18,3) as value,
    lpvalue::number(18,3) as lpvalue,
    ptrvalue::number(18,3) as ptrvalue,
    iscubeprocess::varchar(2) as iscubeprocess,
    salclsnrvalue::number(18,3) as salclsnrvalue,
    unsalclsnrvalue::number(18,3) as unsalclsnrvalue,
    offerclsnrvalue::number(18,3) as offerclsnrvalue,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
from final