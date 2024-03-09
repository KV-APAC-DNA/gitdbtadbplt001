{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['dstrbtr_id','ar_cd']
    )
}}

with 
itg_th_dms_customer_dim as (
    select * from {{ ref('thaitg_integration__itg_th_dms_customer_dim') }}
),

itg_th_dtsdistributor as (
    select * from {{ ref('thaitg_integration__itg_th_dtsdistributor') }}
),

itg_th_dtscustgroup as (
    select * from {{ ref('thaitg_integration__itg_th_dtscustgroup') }}
),

itg_th_dtssaledistrict as (
    select * from {{ ref('thaitg_integration__itg_th_dtssaledistrict') }}
),

itg_th_dtsregion as (
    select * from {{ ref('thaitg_integration__itg_th_dtsregion') }}
),

itg_th_dtsdistrict as (
    select * from {{ ref('thaitg_integration__itg_th_dtsdistrict') }}
),

itg_th_dtssubdistrict as (
    select * from {{ ref('thaitg_integration__itg_th_dtssubdistrict') }}
),

itg_th_gt_dstrbtr_control as (
    select * from {{ ref('thaitg_integration__itg_th_gt_dstrbtr_control') }}
),

itg_th_dms_chana_customer_dim as (
    select * from {{ ref('thaitg_integration__itg_th_dms_chana_customer_dim') }}
),

cust as 
(
select 
      distributorid, 
      arcode, 
      arname, 
      araddress, 
      telephone, 
      fax, 
      city, 
      region, 
      artypecode, 
      saledistrict, 
      saleoffice, 
      salegroup, 
      saleemployee, 
      salename, 
      sourcefile, 
      billno, 
      billmoo, 
      billsoi, 
      billroad, 
      billsubdist, 
      billdistrict, 
      billprovince, 
      billzipcode, 
      nvl(
        old_custid, 
        upper(arcode)
      ) as old_cust_id, 
      modifydate, 
      routestep1, 
      routestep2, 
      routestep3, 
      routestep4, 
      routestep5, 
      routestep6, 
      routestep7, 
      routestep8, 
      routestep9, 
      routestep10, 
      activestatus, 
      null as cust_type, 
      to_char(current_timestamp(), 'yyyymmddhh24missms') as cdl_dttm, 
      store 
    from 
      itg_th_dms_customer_dim
),

trans_cust as 
(
select 
  upper(trim(cust.distributorid)) as dstrbtr_id, 
  upper(trim(cust.arcode)) as ar_cd, 
  trim(cust.old_cust_id) as old_cust_id, 
  trim(cust.arname) as ar_nm, 
  trim(cust.araddress) as ar_adres, 
  trim(cust.telephone) as tel_phn, 
  trim(cust.fax) as fax, 
  trim(cust.city) as city, 
  trim(cust.region) as region, 
  trim(cust.artypecode) as ar_typ_cd, 
  trim(cust.saledistrict) as sls_dist, 
  trim(cust.saleoffice) as sls_office, 
  coalesce(
    trim(cust.salegroup), 
    'N/A'
  ) as sls_grp, 
  trim(cust.saleemployee) as sls_emp, 
  trim(cust.salename) as sls_nm, 
  trim(cust.sourcefile) as src_file, 
  trim(cust.billno) as bill_no, 
  trim(cust.billmoo) as bill_moo, 
  trim(cust.billsoi) as bill_soi, 
  trim(cust.billroad) as bill_rd, 
  trim(cust.billsubdist) as bill_subdist, 
  trim(cust.billdistrict) as bill_dist, 
  trim(cust.billprovince) as bill_prvnce, 
  trim(cust.billzipcode) as bill_zip_cd, 
  cust.modifydate, 
  trim(cust.routestep1) as routestep1, 
  trim(cust.routestep2) as routestep2, 
  trim(cust.routestep3) as routestep3, 
  trim(cust.routestep4) as routestep4, 
  trim(cust.routestep5) as routestep5, 
  trim(cust.routestep6) as routestep6, 
  trim(cust.routestep7) as routestep7, 
  trim(cust.routestep8) as routestep8, 
  trim(cust.routestep9) as routestep9, 
  trim(cust.routestep10) as routestep10, 
  cust.activestatus, 
  trim(cust.cust_type) as cust_type, 
  trim(dist.dist_nm) as dstrbtr_dist_nm, 
  dist.cost_lvl as dstrbtr_cost_lvl, 
  trim(dist.status) as dstrbtr_status, 
  trim(dist.region) as dstrbtr_region, 
  trim(dist.cntry) as dstrbtr_cntry, 
  trim(dist.curnt_dist) as dstrbtr_curnt_dist, 
  dist.inv_day as dstrbtr_inv_day, 
  trim(dist.dstrbtr_cd) as dstrbtr_cd, 
  dist.dstrbtr_fee, 
  coalesce(
    trim(cg.grp_nm), 
    'N/A'
  ) as grp_nm, 
  trim(cg.ar_typ_grp) as ar_typ_grp, 
  trim(cg.typ_grp_nm) as typ_grp_nm, 
  trim(sd.sls_dist) as sales_district, 
  trim(sd.city) as sls_dist_city, 
  trim(sd.region) as sls_dist_region, 
  trim(sd.city_eng) as sls_dist_city_eng, 
  trim(sd.blng_to_dstrbtr) as sls_dist_blng_to_dstrbtr, 
  trim(r.region_desc) as region_desc, 
  trim(d.dist_nm) as dist_nm, 
  trim(subd.sub_dist_nm) as sub_dist_nm, 
  cust.cdl_dttm, 
  current_timestamp()::timestamp_ntz(9) as crtd_dttm, 
  current_timestamp()::timestamp_ntz(9) as updt_dttm, 
  'N/A' as pricelevel, 
  'N/A' as salesareaname, 
  'N/A' as branchcode, 
  'N/A' as branchname, 
  'N/A' as frequencyofvisit, 
  trim(cust.store) as store, 
  coalesce(
    trim(cd.grp_nm), 
    'N/A'
  ) as re_nm 
from 
   cust 
  left outer join itg_th_dtsdistributor dist on upper (
    trim (cust.distributorid)
  ) = upper (
    trim (dist.dstrbtr_id)
  ) 
  left outer join itg_th_dtscustgroup cd on upper (
    trim (cust.store)
  ) = upper (
    trim (cd.ar_typ_cd)
  ) 
  left outer join itg_th_dtscustgroup cg on upper (
    trim (cust.artypecode)
  ) = upper (
    trim (cg.ar_typ_cd)
  ) 
  left outer join itg_th_dtssaledistrict sd on upper (
    trim (cust.billprovince)
  ) = upper (
    trim (sd.sls_dist)
  ) 
  left outer join itg_th_dtsregion r on upper (
    trim (sd.region)
  ) = upper (
    trim (r.region)
  ) 
  left outer join itg_th_dtsdistrict d on upper (
    trim (d.dist)
  ) = upper (
    trim (cust.billdistrict)
  ) 
  left outer join itg_th_dtssubdistrict subd on upper (
    trim (subd.sub_dist)
  ) = upper (
    trim (cust.billsubdist)
  ) 
where 
  upper(cust.distributorid) not in (
    select 
      distinct upper(distributor_id) distributor_id 
    from 
      itg_th_gt_dstrbtr_control 
    where 
      upper(chana_cust_load_flag) = 'Y'
  )
),

cust_2 as 
(
    select 
      distributorid, 
      arcode, 
      arname, 
      araddress, 
      telephone, 
      fax, 
      city, 
      region, 
      artypecode, 
      saledistrict, 
      saleoffice, 
      salegroup, 
      saleemployee, 
      salename, 
      null as sourcefile, 
      billno, 
      billmoo, 
      billsoi, 
      billroad, 
      billsubdist, 
      billdistrict, 
      billprovince, 
      billzipcode, 
      upper(arcode) as old_cust_id, 
      current_timestamp()::timestamp_ntz(9) as modifydate, 
      routestep1, 
      routestep2, 
      routestep3, 
      routestep4, 
      routestep5, 
      routestep6, 
      routestep7, 
      routestep8, 
      routestep9, 
      routestep10, 
      store, 
      activestatus, 
      pricelevel, 
      salesareaname, 
      branchcode, 
      branchname, 
      frequencyofvisit, 
      null as cust_type, 
      run_id as cdl_dttm 
    from 
      itg_th_dms_chana_customer_dim
),

trans_cust_2 as 
(
select 
  upper(trim(cust_2.distributorid)) as dstrbtr_id, 
  upper(trim(cust_2.arcode)) as ar_cd, 
  trim(cust_2.arcode) as old_cust_id, 
  trim(cust_2.arname) as ar_nm, 
  trim(cust_2.araddress) as ar_adres, 
  trim(cust_2.telephone) as tel_phn, 
  trim(cust_2.fax) as fax, 
  trim(cust_2.city) as city, 
  trim(cust_2.region) as region, 
  trim(cust_2.artypecode) as ar_typ_cd, 
  trim(cust_2.saledistrict) as sls_dist, 
  trim(cust_2.saleoffice) as sls_office, 
  coalesce(trim(cust_2.salegroup), 'N/A') as sls_grp, 
  trim(cust_2.saleemployee) as sls_emp, 
  trim(cust_2.salename) as sls_nm, 
  trim(cust_2.sourcefile) src_file, 
  trim(cust_2.billno) as bill_no, 
  trim(cust_2.billmoo) as bill_moo, 
  trim(cust_2.billsoi) as bill_soi, 
  trim(cust_2.billroad) as bill_rd, 
  trim(cust_2.billsubdist) as bill_subdist, 
  trim(cust_2.billdistrict) as bill_dist, 
  trim(cust_2.billprovince) as bill_prvnce, 
  trim(cust_2.billzipcode) as bill_zip_cd, 
  cust_2.modifydate, 
  trim(cust_2.routestep1) as routestep1, 
  trim(cust_2.routestep2) as routestep2, 
  trim(cust_2.routestep3) as routestep3, 
  trim(cust_2.routestep4) as routestep4, 
  trim(cust_2.routestep5) as routestep5, 
  trim(cust_2.routestep6) as routestep6, 
  trim(cust_2.routestep7) as routestep7, 
  trim(cust_2.routestep8) as routestep8, 
  trim(cust_2.routestep9) as routestep9, 
  trim(cust_2.routestep10) as routestep10, 
  cust_2.activestatus, 
  trim(cust_2.cust_type) as cust_type, 
  trim(dist.dist_nm) as dstrbtr_dist_nm, 
  dist.cost_lvl as dstrbtr_cost_lvl, 
  trim(dist.status) as dstrbtr_status, 
  trim(dist.region) as dstrbtr_region, 
  trim(dist.cntry) as dstrbtr_cntry, 
  trim(dist.curnt_dist) as dstrbtr_curnt_dist, 
  dist.inv_day as dstrbtr_inv_day, 
  trim(dist.dstrbtr_cd) as dstrbtr_cd, 
  dist.dstrbtr_fee, 
  coalesce(trim(cg.grp_nm), 'N/A') as grp_nm, 
  trim(cg.ar_typ_grp) as ar_typ_grp, 
  trim(cg.typ_grp_nm) as typ_grp_nm, 
  trim(sd.sls_dist) as sales_district, 
  trim(sd.city) as sls_dist_city, 
  trim(sd.region) as sls_dist_region, 
  trim(sd.city_eng) as sls_dist_city_eng, 
  trim(sd.blng_to_dstrbtr) as sls_dist_blng_to_dstrbtr, 
  trim(r.region_desc) as region_desc, 
  trim(d.dist_nm) as dist_nm, 
  trim(subd.sub_dist_nm) as sub_dist_nm, 
  cust_2.cdl_dttm, 
  current_timestamp()::timestamp_ntz(9) as crtd_dttm, 
  current_timestamp()::timestamp_ntz(9) as updt_dttm, 
  trim(cust_2.pricelevel) as pricelevel, 
  coalesce(
    trim(cust_2.salesareaname), 
    'N/A'
  ) as salesareaname, 
  trim(cust_2.branchcode) as branchcode, 
  trim(cust_2.branchname) as branchname, 
  trim(cust_2.frequencyofvisit) as frequencyofvisit, 
  trim(cust_2.store) as store, 
  coalesce(
    trim(cd.grp_nm), 
    'N/A'
  ) as re_nm 
from cust_2
  left outer join itg_th_dtsdistributor dist on upper (
    trim (cust_2.distributorid)
  ) = upper (
    trim (dist.dstrbtr_id)
  ) 
  left outer join itg_th_dtscustgroup cd on upper (
    trim (cust_2.store)
  ) = upper (
    trim (cd.ar_typ_cd)
  ) 
  left outer join itg_th_dtscustgroup cg on upper (
    trim (cust_2.artypecode)
  ) = upper (
    trim (cg.ar_typ_cd)
  ) 
  left outer join itg_th_dtssaledistrict sd on upper (
    trim (cust_2.billprovince)
  ) = upper (
    trim (sd.sls_dist)
  ) 
  left outer join itg_th_dtsregion r on upper (
    trim (sd.region)
  ) = upper (
    trim (r.region)
  ) 
  left outer join itg_th_dtsdistrict d on upper (
    trim (d.dist)
  ) = upper (
    trim (cust_2.billdistrict)
  ) 
  left outer join itg_th_dtssubdistrict subd on upper (
    trim (subd.sub_dist)
  ) = upper (
    trim (cust_2.billsubdist)
  )
),

transformed as
(
    select * from trans_cust
    union all
    select * from trans_cust_2
),

final as 
(
select
    dstrbtr_id::varchar(10) as dstrbtr_id,
    ar_cd::varchar(20) as ar_cd,
    old_cust_id::varchar(25) as old_cust_id,
    ar_nm::varchar(500) as ar_nm,
    ar_adres::varchar(500) as ar_adres,
    tel_phn::varchar(150) as tel_phn,
    fax::varchar(150) as fax,
    city::varchar(500) as city,
    region::varchar(20) as "region",
    ar_typ_cd::varchar(20) as ar_typ_cd,
    sls_dist::varchar(10) as sls_dist,
    sls_office::varchar(10) as sls_office,
    sls_grp::varchar(10) as sls_grp,
    sls_emp::varchar(150) as sls_emp,
    sls_nm::varchar(250) as sls_nm,
    src_file::varchar(255) as src_file,
    bill_no::varchar(500) as bill_no,
    bill_moo::varchar(250) as bill_moo,
    bill_soi::varchar(255) as bill_soi,
    bill_rd::varchar(255) as bill_rd,
    bill_subdist::varchar(30) as bill_subdist,
    bill_dist::varchar(30) as bill_dist,
    bill_prvnce::varchar(30) as bill_prvnce,
    bill_zip_cd::varchar(50) as bill_zip_cd,
    modifydate::timestamp_ntz(9) as modify_dt,
    routestep1::varchar(10) as routestep1,
    routestep2::varchar(10) as routestep2,
    routestep3::varchar(10) as routestep3,
    routestep4::varchar(10) as routestep4,
    routestep5::varchar(10) as routestep5,
    routestep6::varchar(10) as routestep6,
    routestep7::varchar(10) as routestep7,
    routestep8::varchar(10) as routestep8,
    routestep9::varchar(10) as routestep9,
    routestep10::varchar(10) as routestep10,
    activestatus::number(18,0) as actv_status,
    cust_type::varchar(50) as cust_type,
    dstrbtr_dist_nm::varchar(100) as dstrbtr_nm,
    dstrbtr_cost_lvl::number(18,0) as dstrbtr_cost_lvl,
    dstrbtr_status::varchar(10) as dstrbtr_status,
    dstrbtr_region::varchar(20) as dstrbtr_region,
    dstrbtr_cntry::varchar(20) as dstrbtr_cntry,
    dstrbtr_curnt_dist::varchar(10) as curnt_dist,
    dstrbtr_inv_day::number(3,0) as dstrbtr_inv_day,
    dstrbtr_cd::varchar(255) as dstrbtr_cd,
    dstrbtr_fee::float as dstrbtr_fee,
    grp_nm::varchar(100) as grp_nm,
    ar_typ_grp::varchar(10) as ar_typ_grp,
    typ_grp_nm::varchar(50) as typ_grp_nm,
    sales_district::varchar(50) as sales_district,
    sls_dist_city::varchar(100) as sls_dist_city,
    sls_dist_region::varchar(100) as sls_dist_region,
    sls_dist_city_eng::varchar(100) as sls_dist_city_eng,
    sls_dist_blng_to_dstrbtr::varchar(5) as sls_dist_blng_to_dstrbtr,
    region_desc::varchar(100) as region_desc,
    dist_nm::varchar(100) as dist_nm,
    sub_dist_nm::varchar(100) as sub_dist_nm,
    cdl_dttm::varchar(255) as cdl_dttm,
    crtd_dttm::timestamp_ntz(9) as crtd_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm,
    pricelevel::varchar(50) as pricelevel,
    salesareaname::varchar(150) as salesareaname,
    branchcode::varchar(50) as branchcode,
    branchname::varchar(150) as branchname,
    frequencyofvisit::varchar(50) as frequencyofvisit,
    store::varchar(200) as store,
    re_nm::varchar(100) as re_nm
from transformed
)

select * from final