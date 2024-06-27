with source as (
    select * from {{ ref('ntaedw_integration__edw_ims_inventory_fact') }}
),
final as (
    select
        invnt_dt as "invnt_dt",
        dstr_cd as "dstr_cd",
        dstr_nm as "dstr_nm",
        prod_cd as "prod_cd",
        prod_nm as "prod_nm",
        ean_num as "ean_num",
        cust_nm as "cust_nm",
        invnt_qty as "invnt_qty",
        invnt_amt as "invnt_amt",
        avg_prc_amt as "avg_prc_amt",
        safety_stock as "safety_stock",
        bad_invnt_qty as "bad_invnt_qty",
        book_invnt_qty as "book_invnt_qty",
        convs_amt as "convs_amt",
        prch_disc_amt as "prch_disc_amt",
        end_invnt_qty as "end_invnt_qty",
        batch_no as "batch_no",
        uom as "uom",
        sls_rep_cd as "sls_rep_cd",
        sls_rep_nm as "sls_rep_nm",
        ctry_cd as "ctry_cd",
        crncy_cd as "crncy_cd",
        crt_dttm as "crt_dttm",
        updt_dttm as "updt_dttm",
        chn_uom as "chn_uom",
        storage_name as "storage_name",
        area as "area"
    from source
)
select * from final