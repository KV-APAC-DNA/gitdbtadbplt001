{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= "delete from {{this}} where to_char(snap_shot_dt,'yyyymm') = to_char(dateadd(hour, -1, convert_timezone('Asia/Bangkok', current_timestamp())),'yyyymm')
                  and   case when (select count(*) from {{ ref('thaitg_integration__itg_th_dstrbtr_customer_dim') }}) > 0 then 1 else 0 end = 1 "

    )
}}

with itg_th_dstrbtr_customer_dim as (
    select * from {{ ref('thaitg_integration__itg_th_dstrbtr_customer_dim')}}
),
itg_th_gt_dstrbtr_control as (
select * from {{ ref('thaitg_integration__itg_th_gt_dstrbtr_control')}}
),
final as (
select 
	   dateadd(hour, -1, convert_timezone('Asia/Bangkok', current_timestamp()))::timestamp_ntz(9) as snap_shot_dt,
       cust.dstrbtr_id::varchar(10) as dstrbtr_id,
       cust.ar_cd::varchar(20) as ar_cd,
       cust.old_cust_id::varchar(25) as old_cust_id,
       cust.ar_nm::varchar(500) as ar_nm,
       cust.ar_adres::varchar(500) as ar_adres,
       cust.tel_phn::varchar(150) as tel_phn,
       cust.fax::varchar(150) as fax,
       cust.city::varchar(500) as city,
       cust.region::varchar(20) as region,
       cust.ar_typ_cd::varchar(20) as ar_typ_cd,
       cust.sls_dist::varchar(200) as sls_dist,
       cust.sls_office::varchar(10) as sls_office,
       cust.sls_grp::varchar(10) as sls_grp,
       cust.sls_emp::varchar(150) as sls_emp,
       cust.sls_nm::varchar(250) as sls_nm,
       cust.src_file::varchar(255) as src_file,
       cust.bill_no::varchar(500) as bill_no,
       cust.bill_moo::varchar(250) as bill_moo,
       cust.bill_soi::varchar(255) as bill_soi,
       cust.bill_rd::varchar(255) as bill_rd,
       cust.bill_subdist::varchar(30) as bill_subdist,
       cust.bill_dist::varchar(30) as bill_dist,
       cust.bill_prvnce::varchar(30) as bill_prvnce,
       cust.bill_zip_cd::varchar(50) as bill_zip_cd,
       cust.modify_dt::timestamp_ntz(9) as modify_dt,
       cust.routestep1::varchar(10) as routestep1,
       cust.routestep2::varchar(10) as routestep2,
       cust.routestep3::varchar(10) as routestep3,
       cust.routestep4::varchar(10) as routestep4,
       cust.routestep5::varchar(10) as routestep5,
       cust.routestep6::varchar(10) as routestep6,
       cust.routestep7::varchar(10) as routestep7,
       cust.routestep8::varchar(10) as routestep8,
       cust.routestep9::varchar(10) as routestep9,
       cust.routestep10::varchar(10) as routestep10,
       cust.store::varchar(50) as store,
       cust.re_nm::varchar(100) as re_nm,
       cust.actv_status::number(18,0) as actv_status,
       cust.cust_type::varchar(50) as cust_type,
       cust.dstrbtr_nm::varchar(100) as dstrbtr_nm,
       cust.dstrbtr_cost_lvl::number(18,0) as dstrbtr_cost_lvl,
       cust.dstrbtr_status::varchar(10) as dstrbtr_status,
       cust.dstrbtr_region::varchar(20) as dstrbtr_region,
       cust.dstrbtr_cntry::varchar(20) as dstrbtr_cntry,
       cust.curnt_dist::varchar(10) as curnt_dist,
       cust.dstrbtr_inv_day::number(3,0) as dstrbtr_inv_day,
       cust.dstrbtr_cd::varchar(255) as dstrbtr_cd,
       cust.dstrbtr_fee::float as dstrbtr_fee,
       cust.grp_nm::varchar(100) as grp_nm,
       cust.ar_typ_grp::varchar(10) as ar_typ_grp,
       cust.typ_grp_nm::varchar(50) as typ_grp_nm,
       cust.sales_district::varchar(50) as sales_district,
       cust.sls_dist_city::varchar(100) as sls_dist_city,
       cust.sls_dist_region::varchar(100) as sls_dist_region,
       cust.sls_dist_city_eng::varchar(100) as sls_dist_city_eng,
       cust.sls_dist_blng_to_dstrbtr::varchar(5) as sls_dist_blng_to_dstrbtr,
       cust.region_desc::varchar(100) as region_desc,
       cust.dist_nm::varchar(100) as dist_nm,
       cust.sub_dist_nm::varchar(100) as sub_dist_nm,
       cust.pricelevel::varchar(50) as pricelevel,
       cust.salesareaname::varchar(150) as salesareaname,
       cust.branchcode::varchar(50) as branchcode,
       cust.branchname::varchar(150) as branchname,
       cust.frequencyofvisit::varchar(50) as frequencyofvisit,
       cust.cdl_dttm::varchar(255) as cdl_dttm,
       cust.crtd_dttm::timestamp_ntz(9) as crtd_dttm,
       cust.updt_dttm::timestamp_ntz(9) as updt_dttm,
from itg_th_dstrbtr_customer_dim cust
where case when (select count(*) from itg_th_dstrbtr_customer_dim) > 0 then 1 else 0 end = 1
and dstrbtr_id in (select distinct upper(distributor_id) from itg_th_gt_dstrbtr_control where upper(stores_flag) = 'Y')
)
select * from final 
