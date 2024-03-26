with source as (
    select * from {{ ref('thaitg_integration__itg_th_sellout_inventory_fact') }}
),
final as (
    select 
        dstrbtr_id as "dstrbtr_id",
        rec_dt as "rec_dt",
        wh_cd as "wh_cd",
        prod_cd as "prod_cd",
        qty as "qty",
        amt as "amt",
        mega_brnd as "mega_brnd",
        brnd as "brnd",
        base_prod as "base_prod",
        vrnt as "vrnt",
        put_up as "put_up",
        isupdt_amt as "isupdt_amt",
        crt_date as "crt_date",
        updt_date as "updt_date",
        cdl_dttm as "cdl_dttm",
        crtd_dttm as "crtd_dttm",
        updt_dttm as "updt_dttm"
    from source
)
select * from final