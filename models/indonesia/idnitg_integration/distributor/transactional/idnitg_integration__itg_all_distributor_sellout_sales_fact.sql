{{
    config
    (
        materialized="incremental",
        incremental_strategy="append",
        pre_hook=" {% if is_incremental() %}
                    delete from {{this}} where dstrbtr_grp_cd||jj_mnth_id in (select distinct dstrbtr_cd||jj_mnth_id from {{ source('idnsdl_raw', 'sdl_all_distributor_sellout_sales_fact') }});
                   -- delete from {{this}} where trim(jj_mnth_id) in ( select distinct trim(dm.jj_mnth_id) from DEV_DNA_CORE.IDNEDW_INTEGRATION.EDW_TIME_DIM dm, DEV_DNA_LOAD.SNAPIDNSDL_RAW.SDL_CSA_RAW_SELLOUT_SALES_FACT ds where trim(to_date(dm.cal_date)) = to_date(ds.tgl_inv::timestamp_ntz(9)) ) and trim(upper(dstrbtr_grp_cd)) = 'CSA';
                   -- delete from {{this}} where trim(jj_mnth_id) in ( select distinct trim(dm.jj_mnth_id) from DEV_DNA_CORE.IDNEDW_INTEGRATION.EDW_TIME_DIM dm, DEV_DNA_LOAD.SNAPIDNSDL_RAW.SDL_DNR_RAW_SELLOUT_SALES_FACT ds where to_date(dm.cal_date) = to_date(trim(ds.bill_date)::timestamp_ntz(9)) ) and trim(upper(dstrbtr_grp_cd)) = 'DNR';
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
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        null::number(18,4) as sls_qty_raw,
        filename::varchar(255) as filename
    from source
)
select * from final
-- union1 as(
--     select 
--         trans_key as trans_key,
--         bill_doc as bill_doc,
--         bill_dt::timestamp_ntz(9) as bill_dt,
--         jj_mnth_id as jj_mnth_id,
--         jj_wk as jj_wk,
--         dstrbtr_cd as dstrbtr_grp_cd,
--         dstrbtr_id as dstrbtr_id,
--         jj_sap_dstrbtr_id as jj_sap_dstrbtr_id,
--         dstrbtr_cust_id as dstrbtr_cust_id,
--         dstrbtr_prod_id as dstrbtr_prod_id,
--         jj_sap_prod_id as jj_sap_prod_id,
--         dstrbtn_chnl as dstrbtn_chnl,
--         grp_outlet as grp_outlet,
--         dstrbtr_slsmn_id as dstrbtr_slsmn_id,
--         sls_qty as sls_qty,
--         grs_val as grs_val,
--         jj_net_val as jj_net_val,
--         trd_dscnt as trd_dscnt,
--         dstrbtr_net_val as dstrbtr_net_val,
--         rtrn_qty as rtrn_qty,
--         rtrn_val as rtrn_val,
--         convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crtd_dttm,
--         convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm,
--         filename as filename,
--         null as sls_qty_raw        
--     from source
-- ),
-- union2 as(
--     select * from DEV_DNA_CORE.sm05_workspace.idnwks_integration__wks_csa_sellout_sales_fact
-- ),
-- union3 as(
--     select * from DEV_DNA_CORE.sm05_workspace.idnwks_integration__wks_dnr_sellout_sales_fact
-- ),
-- transformed as(
--     select * from union1
--     union all
--     select * from union2
--     union all
--     select * from union3
-- )
-- select * from transformed

