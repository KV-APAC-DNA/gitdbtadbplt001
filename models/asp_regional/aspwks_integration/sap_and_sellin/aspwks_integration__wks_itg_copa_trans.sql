{{
    config(
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';"
    )
}}

with source as (
    select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_bw_zocopa10') }}
)

final as (
    select
    request_number,
    data_packet,
    data_record,
    comp_code,
    co_area,
    profit_ctr,
    salesorg,
    material,
    customer,
    division,
    plant,
    chrt_accts,
    account,
    distr_chan,
    fiscvarnt,
    version,
    recordmode,
    bill_type,
    sales_off,
    country,
    salesdeal,
    sales_grp,
    salesemply,
    sales_dist,
    cust_group,
    cust_sales,
    bus_area,
    vtype,
    zmercref,
    calday,
    calmonth,
    fiscyear,
    fiscper3,
    fiscper,
    zz_mvgr1,
    zz_mvgr2,
    zz_mvgr3,
    zz_mvgr4,
    zz_mvgr5,
    region,
    prodh6,
    prodh5,
    prodh4,
    prodh3,
    prodh2,
    prodh1,
    zsalesper,
    mat_sales,
    prod_hier,
    zz_wwme,
    zfamocac,
    amocac,
    currency,
    amoccc,
    obj_curr,
    grossamttc,
    curkey_tc,
    quantity,
    unit,
    zqtyieu,
    zunitieu,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source

)

select * from final
