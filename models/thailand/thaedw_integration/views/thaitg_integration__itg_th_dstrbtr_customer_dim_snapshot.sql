{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ["to_char(snap_shot_dt,'YYYYMM')"]
    )
}}

with itg_th_dstrbtr_customer_dim as (
    select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_TH_DSTRBTR_CUSTOMER_DIM
),
itg_th_gt_dstrbtr_control as (
select * from DEV_DNA_CORE.NNARAS01_WORKSPACE.THAITG_INTEGRATION__ITG_TH_GT_DSTRBTR_CONTROL
),
final as (
select DATEADD(HOUR, -1, CONVERT_TIMEZONE('Asia/Bangkok', CURRENT_TIMESTAMP())) as snap_shot_dt,
       cust.dstrbtr_id,
       cust.ar_cd,
       cust.old_cust_id,
       cust.ar_nm,
       cust.ar_adres,
       cust.tel_phn,
       cust.fax,
       cust.city,
       cust.region,
       cust.ar_typ_cd,
       cust.sls_dist,
       cust.sls_office,
       cust.sls_grp,
       cust.sls_emp,
       cust.sls_nm,
       cust.src_file,
       cust.bill_no,
       cust.bill_moo,
       cust.bill_soi,
       cust.bill_rd,
       cust.bill_subdist,
       cust.bill_dist,
       cust.bill_prvnce,
       cust.bill_zip_cd,
       cust.modify_dt,
       cust.routestep1,
       cust.routestep2,
       cust.routestep3,
       cust.routestep4,
       cust.routestep5,
       cust.routestep6,
       cust.routestep7,
       cust.routestep8,
       cust.routestep9,
       cust.routestep10,
       cust.store,
       cust.re_nm,
       cust.actv_status,
       cust.cust_type,
       cust.dstrbtr_nm,
       cust.dstrbtr_cost_lvl,
       cust.dstrbtr_status,
       cust.dstrbtr_region,
       cust.dstrbtr_cntry,
       cust.curnt_dist,
       cust.dstrbtr_inv_day,
       cust.dstrbtr_cd,
       cust.dstrbtr_fee,
       cust.grp_nm,
       cust.ar_typ_grp,
       cust.typ_grp_nm,
       cust.sales_district,
       cust.sls_dist_city,
       cust.sls_dist_region,
       cust.sls_dist_city_eng,
       cust.sls_dist_blng_to_dstrbtr,
       cust.region_desc,
       cust.dist_nm,
       cust.sub_dist_nm,
       cust.pricelevel,
       cust.salesareaname,
       cust.branchcode,
       cust.branchname,
       cust.frequencyofvisit,
       cust.cdl_dttm,
       cust.crtd_dttm,
       cust.updt_dttm
from itg_th_dstrbtr_customer_dim cust
where case when (select count(*) from itg_th_dstrbtr_customer_dim) > 0 then 1 else 0 end = 1
and dstrbtr_id in (select distinct upper(distributor_id) from itg_th_gt_dstrbtr_control where upper(stores_flag) = 'Y')
)
select * from final 

