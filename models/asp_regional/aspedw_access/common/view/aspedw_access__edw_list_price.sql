with edw_list_price as(
    select * from {{ ref('aspedw_integration__edw_list_price') }}
),
final as (
    Select sls_org as "sls_org",
    material as "material",
    cond_rec_no as "cond_rec_no",
    matl_grp as "matl_grp",
    valid_to as "valid_to",
    knart as "knart",
    dt_from as "dt_from",
    amount as "amount",
    currency as "currency",
    unit as "unit",
    record_mode as "record_mode",
    comp_cd as "comp_cd",
    price_unit as "price_unit",
    zcurrfpa as "zcurrfpa",
    cdl_dttm as "cdl_dttm",
    crtd_dttm as "crtd_dttm",
    updt_dttm as "updt_dttm"
    From edw_list_price
)

select * from final