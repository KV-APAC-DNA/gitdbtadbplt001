{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook = "{% if is_incremental() %}
        DELETE FROM {{this}}
        WHERE (month,year) IN (SELECT DISTINCT MONTH, YEAR FROM {{ ref('inditg_integration__itg_tblpf_sit_wrk') }});
        {% endif %}"
    )
}}

with source as
(
    select * from {{ ref('inditg_integration__itg_tblpf_sit_wrk') }}
),
final as
(
    SELECT 
    serno,
    distcode,
    prdcode,
    month,
    year,
    subdcode,
    COALESCE(sitqty,0) AS sitqty,
    COALESCE(sitvalue,0) AS sitvalue,
    src,
    createddate,
    crt_dttm,
    updt_dttm
    FROM source
)
select serno::varchar(50) as serno,
    distcode::varchar(50) as distcode,
    prdcode::varchar(50) as prdcode,
    month::varchar(50) as month,
    year::varchar(50) as year,
    subdcode::varchar(50) as subdcode,
    sitqty::number(20,4) as sitqty,
    sitvalue::number(20,4) as sitvalue,
    src::varchar(10) as src,
    createddate::timestamp_ntz(9) as createddate,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
from final