{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ["file_name"],
        post_hook="{{sap_transaction_processed_files('BWA_ZC_SD','vw_stg_sdl_sap_bw_zc_sd','itg_invc_sls')}}"
    )
}}

--import CTE

with source as (
    select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_bw_zc_sd')}}
),
sap_transactional_processed_files as (
    select * from {{ source('aspwks_integration', 'sap_transactional_processed_files') }}
),
final as(
    select 
    request::VARCHAR(100) as request_number,
    datapakid::VARCHAR(50) as data_packet,
    record::VARCHAR(100) as data_record,
    TRY_TO_DATE(zactdldte,'YYYYMMDD') as act_delv_dt,
    TRY_TO_DATE(act_gi_dte,'YYYYMMDD') as act_good_iss_dt,
    billtoprty::VARCHAR(10) as bill_to_prty,
    TRY_TO_DATE(bill_date,'YYYYMMDD') as bill_dt,
    bill_type::VARCHAR(4) as bill_ty,
    bill_num::VARCHAR(10) as bill_doc,
    comp_code::VARCHAR(4) as cmpy_cd,
    customer::VARCHAR(10) as cust_no,
    TRY_TO_DATE(zdelcrdat,'YYYYMMDD') as delv_doc_crt_dt,
    distr_chan::VARCHAR(2) as dstr_chnl,
    division::VARCHAR(2) as div,
    TRY_TO_DATE(createdon,'YYYYMMDD') as doc_crt_dt,
    TRY_TO_DATE(doc_date,'YYYYMMDD') as doc_dt,
    TRY_TO_DATE(gi_date,'YYYYMMDD') as good_iss_dt,
    material::VARCHAR(18) as mat,
    TRY_TO_DATE(matav_date,'YYYYMMDD') as mat_avail_dt,
    ord_reason::VARCHAR(3) as ord_rsn,
    zzabstk::VARCHAR(1) as ovrl_rej_sts,
    zcmgst::VARCHAR(1) as ovrl_sts_crd_chk,
    payer::VARCHAR(10) as payer,
    plant::VARCHAR(4) as plant,
    zzp_itm::NUMBER(18,0) as prec_doc_itm,
    zzp_num::VARCHAR(10) as prec_doc_num,
    TRY_TO_DATE(zsd_pod,'YYYYMMDD') as proof_delv_dt,
    zrcodekey::VARCHAR(29) as rsn_cd_key,
    reason_rej::VARCHAR(2) as rsn_rej,
    TRY_TO_DATE(gt_cmfre,'YYYYMMDD') as rlse_dt_cr_mgmt,
    TRY_TO_DATE(zreq_date,'YYYYMMDD') as rqst_delv_dt,
    route::VARCHAR(6) as route,
    doc_number::VARCHAR(10) as sls_doc,
    doc_categ::VARCHAR(2) as sls_doc_cat,
    s_ord_item::NUMBER(18,0) as sls_doc_itm,
    doc_type::VARCHAR(4) as sls_doc_typ,
    salesemply::NUMBER(18,0) as sls_emp_hist,
    salesorg::VARCHAR(4) as sls_org,
    item_categ::VARCHAR(4) as sls_doc_itm_cat,
    ship_to::VARCHAR(10) as ship_to_prty,
    sold_to::VARCHAR(10) as sold_to_prty,
    zblqtycse::NUMBER(15,4) as bill_qty_cse,
    bill_qty::NUMBER(15,4) as bill_qty_pc,
    zbilqdift::NUMBER(15,4) as bill_qty_difot,
    zbilqotif::NUMBER(15,4) as bill_qty_otif,
    inv_qty::NUMBER(15,4) as bill_qty_sls_uom,
    zcfmqdift::NUMBER(15,4) as cnfrm_qty_difot,
    cml_cd_qty::NUMBER(15,4) as cnfrm_qty_pc,
    zdlqtycse::NUMBER(15,4) as delv_qty_cse,
    zdelqtybu::NUMBER(15,4) as delv_qty_pc,
    dlv_qty::NUMBER(15,4) as delv_qty_sls_uom,
    subtotal_6::NUMBER(15,4) as est_nts,
    zfs_ntsb::NUMBER(15,4) as nts_bill,
    zfs_ninv::NUMBER(15,4) as net_invc_sls,
    zfs_qtybu::NUMBER(15,4) as fut_sls_qty,
    subtotal_1::NUMBER(15,4) as gros_trd_sls,
    subtotal_5::NUMBER(15,4) as net_amt,
    net_price::NUMBER(15,4) as net_prc,
    netval_inv::NUMBER(15,4) as net_bill_val,
    net_value::NUMBER(15,4) as net_ord_val,
    zorqtycse::NUMBER(15,4) as ord_qty_cse,
    zordqtybu::NUMBER(15,4) as ord_qty_pc,
    zordqty::NUMBER(15,4) as ord_pc_qty,
    cml_or_qty::NUMBER(15,4) as ord_sls_qty,
    zktranlt::NUMBER(15,4) as tran_ldtm,
    zunsuppc::NUMBER(15,4) as unspp_qty,
    zunsupva::NUMBER(15,4) as unspp_val,
    volume_dl::NUMBER(15,4) as vol_delv,
    volume_ap::NUMBER(20,4) as vol_ord,
    TRY_TO_DATE(calday,'YYYYMMDD') as cal_day,
    base_uom::VARCHAR(4) as base_uom,
    currency::VARCHAR(5) as curr_key,
    doc_currcy::VARCHAR(5) as doc_curr,
    sales_unit::VARCHAR(4) as sls_unit,
    fiscper::VARCHAR(10) as fisc_yr,
    current_timestamp()::timestamp_ntz(9) as crt_dttm, 
    current_timestamp()::timestamp_ntz(9) as updt_dttm,
    file_name
    from source
    where not exists (
    select 
        act_file_name 
    from sap_transactional_processed_files 
    where target_table_name='itg_invc_sls' and sap_transactional_processed_files.act_file_name=source.file_name
  )

)

select * from final