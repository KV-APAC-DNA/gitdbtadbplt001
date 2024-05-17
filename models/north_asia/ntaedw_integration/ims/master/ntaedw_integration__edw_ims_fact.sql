{{
    config(
        materialized="incremental",
        incremental_strategy='append',
        pre_hook="delete from {{this}} where dstr_cd in ( select distinct dstr_cd from snapntawks_integration.wks_edw_ims_sls_std);"
    )
}}
with wks_edw_ims_sls_std as (
    select * from snapntawks_integration.wks_edw_ims_sls_std
),
final as
(
    SELECT 
        ims_txn_dt as ims_txn_dt,
        dstr_cd as dstr_cd,
        dstr_nm as dstr_nm,
        cust_cd as cust_cd,
        cust_nm as cust_nm,
        prod_cd as prod_cd,
        prod_nm as prod_nm,
        rpt_per_strt_dt as rpt_per_strt_dt,
        rpt_per_end_dt as rpt_per_end_dt,
        ean_num as ean_num,
        uom as uom,
        unit_prc as unit_prc,
        sls_amt as sls_amt,
        sls_qty as sls_qty,
        rtrn_qty as rtrn_qty,
        rtrn_amt as rtrn_amt,
        ship_cust_nm as ship_cust_nm,
        cust_cls_grp as cust_cls_grp,
        cust_sub_cls as cust_sub_cls,
        prod_spec as prod_spec,
        itm_agn_nm as itm_agn_nm,
        ordr_co as ordr_co,
        rtrn_rsn as rtrn_rsn,
        sls_ofc_cd as sls_ofc_cd,
        sls_grp_cd as sls_grp_cd,
        sls_ofc_nm as sls_ofc_nm,
        sls_grp_nm as sls_grp_nm,
        acc_type as acc_type,
        co_cd as co_cd,
        sls_rep_cd as sls_rep_cd,
        sls_rep_nm as sls_rep_nm,
        doc_dt as doc_dt,
        doc_num as doc_num,
        invc_num as invc_num,
        remark_desc as remark_desc,
        gift_qty as gift_qty,
        sls_bfr_tax_amt as sls_bfr_tax_amt,
        sku_per_box as sku_per_box,
        ctry_cd as ctry_cd,
        crncy_cd as crncy_cd,
        current_timestamp AS CRT_DTTM,
        current_timestamp AS UPDT_DTTM,
        prom_sls_amt as prom_sls_amt,
        prom_rtrn_amt as prom_rtrn_amt,
        prom_prc_amt as prom_prc_amt,
        null::varchar(255) as sap_code,
        null::varchar(20) as sku_type,
        null::varchar(50) as sub_customer_code,
        null::varchar(100) as sub_customer_name,
        null::varchar(10) as sales_priority,
        null::number(21,5) as sales_stores,
        null::number(21,5) as sales_rate
    FROM wks_edw_ims_sls_std
)
select * from final