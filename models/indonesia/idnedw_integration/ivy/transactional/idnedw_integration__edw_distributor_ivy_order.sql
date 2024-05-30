with source as (
    select * from {{ ref('idnitg_integration__itg_distributor_ivy_order') }}
),
final as (
    select 
        trans_key::varchar(100) as trans_key,
        bill_doc::varchar(100) as bill_doc,
        order_dt::timestamp_ntz(9) as order_dt,
        jj_mnth_id::varchar(10) as jj_mnth_id,
        jj_wk::varchar(4) as jj_wk,
        dstrbtr_grp_cd::varchar(20) as dstrbtr_grp_cd,
        dstrbtr_id::varchar(50) as dstrbtr_id,
        jj_sap_dstrbtr_id::varchar(50) as jj_sap_dstrbtr_id,
        dstrbtr_cust_id::varchar(100) as dstrbtr_cust_id,
        dstrbtr_prod_id::varchar(100) as dstrbtr_prod_id,
        jj_sap_prod_id::varchar(50) as jj_sap_prod_id,
        dstrbtn_chnl::varchar(100) as dstrbtn_chnl,
        grp_outlet::varchar(5) as grp_outlet,
        dstrbtr_slsmn_id::varchar(100) as dstrbtr_slsmn_id,
        sls_qty::number(18,4) as sls_qty,
        grs_val::number(18,4) as grs_val,
        jj_net_val::number(18,4) as jj_net_val,
        trd_dscnt::number(18,4) as trd_dscnt,
        dstrbtr_net_val::number(18,4) as dstrbtr_net_val,
        rtrn_qty::number(18,4) as rtrn_qty,
        rtrn_val::number(18,4) as rtrn_val,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from source
)

select * from final 