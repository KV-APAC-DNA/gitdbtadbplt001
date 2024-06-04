{{
    config
    (
        materialized="incremental",
        incremental_strategy="append",
        pre_hook=" {% if is_incremental() %}
                    delete from {{this}} where (dstrbtr_grp_cd||jj_mnth_id) in (select distinct dstrbtr_cd||jj_mnth_id from {{ source('idnsdl_raw', 'sdl_all_distributor_sellout_sales_fact') }});
                    {% endif %}"
    )
}}
with source as (
    select * from  {{ source('idnsdl_raw', 'sdl_all_distributor_sellout_sales_fact') }}
),
final as
(
    select 
        trans_key::varchar(100) as trans_key,
        bill_doc::varchar(100) as bill_doc,
        bill_dt::timestamp_ntz(9) as bill_dt,
        jj_mnth_id::varchar(10) as jj_mnth_id,
        jj_wk::varchar(4) as jj_wk,
        dstrbtr_cd::varchar(20) as dstrbtr_grp_cd,
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
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crtd_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm,
        null::number(18,4) as sls_qty_raw,
        filename::varchar(255) as filename
    from source
)
select * from final