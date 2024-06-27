{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook = "{% if is_incremental() %}
                    DELETE FROM {{this}}
                    WHERE (distcode,transdate) 
                        IN (SELECT DISTINCT distcode,transdate
                        FROM {{ ref('inditg_integration__itg_rmrpstockprocess_clstk') }});
                    {% endif %}"
    )
}}

with itg_rmrpstockprocess_clstk as 
(
    select * from {{ ref('inditg_integration__itg_rmrpstockprocess_clstk') }}
),
final as 
(
    select
        distcode::varchar(50) as distcode,
        transdate::date as transdate,
        productcode::varchar(50) as productcode,
        mrp::number(22,6) as mrp,
        lsp::number(22,6) as lsp,
        selrate::number(22,6) as selrate,
        nrvalue::number(22,6) as nrvalue,
        salopenstock::number(22,6) as salopenstock,
        unsalopenstock::number(18,0) as unsalopenstock,
        offeropenstock::number(18,0) as offeropenstock,
        salclsstock::number(18,0) as salclsstock,
        unsalclsstock::number(18,0) as unsalclsstock,
        offerclsstock::number(18,0) as offerclsstock,
        syncid::number(18,0) as syncid,
        createddate::timestamp_ntz(9) as createddate,
        modifieddate::timestamp_ntz(9) as modifieddate,
        createddt::timestamp_ntz(9) as createddt,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from itg_rmrpstockprocess_clstk
)
select * from final
