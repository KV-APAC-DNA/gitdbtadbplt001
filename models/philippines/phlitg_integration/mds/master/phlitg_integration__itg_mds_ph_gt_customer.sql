{{
    config(
        pre_hook="{{build_itg_mds_ph_gt_customer()}}"
    )
}}
with sdl_mds_ph_gt_customer as 
(
    select * from {{ source('phlsdl_raw', 'sdl_mds_ph_gt_customer') }}
),
trans as
(
    (with wks
as
(select dstrbtr_cust_id,
       dstrbtr_cust_nm,
       slsmn,
       slsmn_desc,
       rep_grp2,
       rep_grp2_desc,
       rep_grp3,
       rep_grp3_desc,
       rep_grp4,
       rep_grp4_desc,
       rep_grp5,
       rep_grp5_desc,
       rep_grp6,
       rep_grp6_desc,
       status,
       address,
       zip,
       slm_grp_cd,
       frequency_visit,
       store_prioritization,
       latitude,
       longitude,
       rpt_grp9,
       rpt_grp9_desc,
       rpt_grp11,
       rpt_grp11_desc,
       sls_dist,
       sls_dist_desc,
       dstrbtr_grp_cd,
       dstrbtr_grp_nm,
       rpt_grp_15_desc,
       last_chg_datetime,
       effective_from,
       case
            when
                to_date(effective_to) = '9999-12-31'
                then dateadd(day, -1, current_timestamp)
            else effective_to
       end as effective_to,
       'N' as active,
       crtd_dttm,
       current_timestamp()::timestamp_ntz(9) as updt_dttm,
       zip_code,
       zip_cd_name,
       barangay_code,
       barangay_cd_name,
       long_lat_source
from (select itg.*
      from {{this}} itg,
           sdl_mds_ph_gt_customer sdl
      where sdl.lastchgdatetime != itg.last_chg_datetime
      and   trim(sdl.code) = itg.dstrbtr_cust_id)
union all
select trim(code) as dstrbtr_cust_id,
       trim(name) as dstrbtr_cust_nm,
       trim(salesman) as slsmn,
       trim(salesmandesc) as slsmn_desc,
       trim(reportgroup2) as rep_grp2,
       trim(reportgroup2desc) as rep_grp2_desc,
       trim(reportgroup3) as rep_grp3,
       trim(reportgroup3desc) as rep_grp3_desc,
       trim(reportgroup4_code) as rep_grp4,
       trim(reportgroup4_name) as rep_grp4_desc,
       trim(reportgroup5) as rep_grp5,
       trim(reportgroup5desc) as rep_grp5_desc,
       trim(reportgroup6) as rep_grp6,
       trim(reportgroup6desc) as rep_grp6_desc,
       trim(status) as status,
       trim(address) as address,
       trim(zip) as zip,
       trim(slmgrpcode) as slm_grp_cd,
       trim(frequencyvisit) as frequency_visit,
       trim(storeprioritization) as store_prioritization,
       trim(latitude) as latitude,
       trim(longitude) as longitude,
       trim(reportgroup9_code) as rpt_grp9,
       trim(reportgroup9_name) as rpt_grp9_desc,
       trim(reportgroup11_code) as rpt_grp11,
       trim(reportgroup11_name) as rpt_grp11_desc,
       trim(sdtrct_code) as sls_dist,
       trim(sdtrct_name) as sls_dist_desc,
       trim(distcode_code) as dstrbtr_grp_cd,
       trim(distcode_name) as dstrbtr_grp_nm,
       trim(reportgroup15desc) as rpt_grp_15_desc,
       lastchgdatetime as last_chg_datetime,
       current_timestamp()::timestamp_ntz(9) as effective_from,
       '9999-12-31' as effective_to,
       'Y' as active,
       current_timestamp()::timestamp_ntz(9) as crtd_dttm,
       current_timestamp()::timestamp_ntz(9) as updt_dttm,
       trim(zipcode_code) as zip_code,
       trim(zipcode_name) as zip_cd_name,
       trim(barangaycode_code) as barangay_code,
       trim(barangaycode_name) as barangay_cd_name,
       longlatsource as long_lat_source
from (select sdl.*
      from {{this}} itg,
           sdl_mds_ph_gt_customer sdl
      where sdl.lastchgdatetime != itg.last_chg_datetime
      and   trim(sdl.code) = itg.dstrbtr_cust_id
      and   itg.active = 'Y')
union all
select trim(code) as dstrbtr_cust_id,
       trim(name) as dstrbtr_cust_nm,
       trim(salesman) as slsmn,
       trim(salesmandesc) as slsmn_desc,
       trim(reportgroup2) as rep_grp2,
       trim(reportgroup2desc) as rep_grp2_desc,
       trim(reportgroup3) as rep_grp3,
       trim(reportgroup3desc) as rep_grp3_desc,
       trim(reportgroup4_code) as rep_grp4,
       trim(reportgroup4_name) as rep_grp4_desc,
       trim(reportgroup5) as rep_grp5,
       trim(reportgroup5desc) as rep_grp5_desc,
       trim(reportgroup6) as rep_grp6,
       trim(reportgroup6desc) as rep_grp6_desc,
       trim(status) as status,
       trim(address) as address,
       trim(zip) as zip,
       trim(slmgrpcode) as slm_grp_cd,
       trim(frequencyvisit) as frequency_visit,
       trim(storeprioritization) as store_prioritization,
       trim(latitude) as latitude,
       trim(longitude) as longitude,
       trim(reportgroup9_code) as rpt_grp9,
       trim(reportgroup9_name) as rpt_grp9_desc,
       trim(reportgroup11_code) as rpt_grp11,
       trim(reportgroup11_name) as rpt_grp11_desc,
       trim(sdtrct_code) as sls_dist,
       trim(sdtrct_name) as sls_dist_desc,
       trim(distcode_code) as dstrbtr_grp_cd,
       trim(distcode_name) as dstrbtr_grp_nm,
       trim(reportgroup15desc) as rpt_grp_15_desc,
       lastchgdatetime as last_chg_datetime,
       effective_from,
       '9999-12-31' as effective_to,
       'Y' as active,
       current_timestamp()::timestamp_ntz(9) as crtd_dttm,
       current_timestamp()::timestamp_ntz(9) as updt_dttm,
       trim(zipcode_code) as zip_code,
       trim(zipcode_name) as zip_cd_name,
       trim(barangaycode_code) as barangay_code,
       trim(barangaycode_name) as barangay_cd_name,
       longlatsource as long_lat_source
from (select sdl.*,itg.effective_from
      from {{this}} itg,
           sdl_mds_ph_gt_customer sdl
      where sdl.lastchgdatetime = itg.last_chg_datetime
      and   trim(sdl.code) = itg.dstrbtr_cust_id)      
union all
select trim(code) as dstrbtr_cust_id,
       trim(name) as dstrbtr_cust_nm,
       trim(salesman) as slsmn,
       trim(salesmandesc) as slsmn_desc,
       trim(reportgroup2) as rep_grp2,
       trim(reportgroup2desc) as rep_grp2_desc,
       trim(reportgroup3) as rep_grp3,
       trim(reportgroup3desc) as rep_grp3_desc,
       trim(reportgroup4_code) as rep_grp4,
       trim(reportgroup4_name) as rep_grp4_desc,
       trim(reportgroup5) as rep_grp5,
       trim(reportgroup5desc) as rep_grp5_desc,
       trim(reportgroup6) as rep_grp6,
       trim(reportgroup6desc) as rep_grp6_desc,
       trim(status) as status,
       trim(address) as address,
       trim(zip) as zip,
       trim(slmgrpcode) as slm_grp_cd,
       trim(frequencyvisit) as frequency_visit,
       trim(storeprioritization) as store_prioritization,
       trim(latitude) as latitude,
       trim(longitude) as longitude,
       trim(reportgroup9_code) as rpt_grp9,
       trim(reportgroup9_name) as rpt_grp9_desc,
       trim(reportgroup11_code) as rpt_grp11,
       trim(reportgroup11_name) as rpt_grp11_desc,
       trim(sdtrct_code) as sls_dist,
       trim(sdtrct_name) as sls_dist_desc,
       trim(distcode_code) as dstrbtr_grp_cd,
       trim(distcode_name) as dstrbtr_grp_nm,
       trim(reportgroup15desc) as rpt_grp_15_desc,
       lastchgdatetime as last_chg_datetime,
       current_timestamp()::timestamp_ntz(9) as effective_from,
       '9999-12-31' as effective_to,
       'Y' as active,
       current_timestamp()::timestamp_ntz(9) as crtd_dttm,
       current_timestamp()::timestamp_ntz(9) as updt_dttm,
       trim(zipcode_code) as zip_code,
       trim(zipcode_name) as zip_cd_name,
       trim(barangaycode_code) as barangay_code,
       trim(barangaycode_name) as barangay_cd_name,
       longlatsource as long_lat_source
from (select *
      from sdl_mds_ph_gt_customer sdl
      where trim(code) not in (select distinct dstrbtr_cust_id
                         from {{this}}))) 
     select*
     from wks
union all
select *
from {{this}}
where dstrbtr_cust_id not in (select trim(dstrbtr_cust_id) from wks))
),
final as
(
    select 
    dstrbtr_cust_id::varchar(50) as dstrbtr_cust_id,
    dstrbtr_cust_nm::varchar(255) as dstrbtr_cust_nm,
    slsmn::varchar(50) as slsmn,
    slsmn_desc::varchar(255) as slsmn_desc,
    rep_grp2::varchar(50) as rep_grp2,
    rep_grp2_desc::varchar(255) as rep_grp2_desc,
    rep_grp3::varchar(50) as rep_grp3,
    rep_grp3_desc::varchar(255) as rep_grp3_desc,
    rep_grp4::varchar(50) as rep_grp4,
    rep_grp4_desc::varchar(255) as rep_grp4_desc,
    rep_grp5::varchar(50) as rep_grp5,
    rep_grp5_desc::varchar(255) as rep_grp5_desc,
    rep_grp6::varchar(50) as rep_grp6,
    rep_grp6_desc::varchar(255) as rep_grp6_desc,
    status::varchar(50) as status,
    address::varchar(255) as address,
    zip::varchar(50) as zip,
    slm_grp_cd::varchar(255) as slm_grp_cd,
    frequency_visit::varchar(50) as frequency_visit,
    store_prioritization::varchar(255) as store_prioritization,
    latitude::varchar(50) as latitude,
    longitude::varchar(255) as longitude,
    rpt_grp9::varchar(50) as rpt_grp9,
    rpt_grp9_desc::varchar(255) as rpt_grp9_desc,
    rpt_grp11::varchar(50) as rpt_grp11,
    rpt_grp11_desc::varchar(255) as rpt_grp11_desc,
    sls_dist::varchar(50) as sls_dist,
    sls_dist_desc::varchar(255) as sls_dist_desc,
    dstrbtr_grp_cd::varchar(50) as dstrbtr_grp_cd,
    dstrbtr_grp_nm::varchar(255) as dstrbtr_grp_nm,
    rpt_grp_15_desc::varchar(255) as rpt_grp_15_desc,
    last_chg_datetime::timestamp_ntz(9) as last_chg_datetime,
    effective_from::timestamp_ntz(9) as effective_from,
    effective_to::timestamp_ntz(9) as effective_to,
    active::varchar(10) as active,
    current_timestamp()::timestamp_ntz(9) as crtd_dttm ,
    current_timestamp()::timestamp_ntz(9) as updt_dttm ,
    zip_code::varchar(100) as zip_code,
    zip_cd_name::varchar(255) as zip_cd_name,
    barangay_code::varchar(100) as barangay_code,
    barangay_cd_name::varchar(255) as barangay_cd_name,
    long_lat_source::number(20,10) as long_lat_source
from trans
where len(address)<=255
)
select * from final