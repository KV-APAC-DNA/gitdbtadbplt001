with source as (
    select * from -- ref('idnitg_integration__itg_all_distributor_sellout_sales_fact') 
                snapidnitg_integration.itg_all_distributor_sellout_sales_fact
),
final as (
    select
        trans_key,
        bill_doc,
        bill_dt,
        jj_mnth_id,
        jj_wk,
        dstrbtr_grp_cd,
        dstrbtr_id,
        jj_sap_dstrbtr_id,
        dstrbtr_cust_id,
        dstrbtr_prod_id,
        jj_sap_prod_id,
        dstrbtn_chnl,
        grp_outlet,
        dstrbtr_slsmn_id,
        sls_qty,
        grs_val,
        jj_net_val,
        trd_dscnt,
        dstrbtr_net_val,
        rtrn_qty,
        rtrn_val,
        crtd_dttm,
        updt_dttm
    from source
)
select * from final