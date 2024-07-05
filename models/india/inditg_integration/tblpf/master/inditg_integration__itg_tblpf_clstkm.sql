{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook ="{% if is_incremental() %}
        delete from {{this}} WHERE (mon,yr) IN (SELECT DISTINCT mon, yr 
        FROM {{ ref('indwks_integration__wks_tblpf_clstkm') }});
        {% endif %}"
    )
}}
with itg_tblpf_clstkm_wrk as
(
    select * from {{ ref('indwks_integration__wks_tblpf_clstkm') }}
),
final as
(
    select
        serno::number(38,0) as serno,
        mon::number(18,0) as mon,
        yr::number(18,0) as yr,
        prdcode::varchar(50) as prdcode,
        distcode::varchar(50) as distcode,
        COALESCE(salopenstock,0)::number(18,3) as salopenstock,
        COALESCE(unsalopenstock,0)::number(18,3) as unsalopenstock,
        COALESCE(offeropenstock,0)::number(18,3) as offeropenstock,
        COALESCE(unsalclsstock,0)::number(18,3) as unsalclsstock,
        COALESCE(offerclsstock,0)::number(18,3) as offerclsstock,
        COALESCE(clstckqty,0)::number(18,3) as clstckqty,
        COALESCE(lp,0)::number(18,3) as lp,
        COALESCE(ptr,0)::number(18,3) as ptr,
        COALESCE(nr,0)::number(18,3) as nr,
        src::varchar(10) as src,
        COALESCE(value,0)::number(18,3) as value,
        COALESCE(lpvalue,0)::number(18,3) as lpvalue,
        COALESCE(ptrvalue,0)::number(18,3) as ptrvalue,
        iscubeprocess::varchar(2) as iscubeprocess,
        COALESCE(salclsnrvalue,0)::number(18,3) as salclsnrvalue,
        COALESCE(unsalclsnrvalue,0)::number(18,3) as unsalclsnrvalue,
        COALESCE(offerclsnrvalue,0)::number(18,3) as offerclsnrvalue,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from itg_tblpf_clstkm_wrk
)
select * from final
