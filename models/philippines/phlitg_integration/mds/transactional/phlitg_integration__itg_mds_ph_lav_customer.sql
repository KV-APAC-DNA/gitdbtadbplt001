{{
    config(
        pre_hook="{{build_itg_mds_ph_lav_customer()}}"
    )
}}

with
sdl_mds_ph_lav_customer as (
    select * from {{ source('phlsdl_raw', 'sdl_mds_ph_lav_customer') }}
),
subq1 as (
    select distinct cust_id from {{this}}
),
wks as
(select cust_id,

       cust_nm,

       parent_cust_cd,

       parent_cust_nm,

       dstrbtr_grp_cd,

       dstrbtr_grp_nm,

       region_cd,

       region_nm,

       province_cd,

       province_nm,

       mun_cd,

       mun_nm,

       channel_cd,

       channel_desc,

       sub_chnl_cd,

       sub_chnl_desc,

       rpt_grp_3_cd,

       rpt_grp_3_desc,

       rpt_grp_4_cd,

       rpt_grp_4_desc,

       rpt_grp_5_cd,

       rpt_grp_5_desc,

       rpt_grp_6_cd,

       rpt_grp_6_desc,

       rpt_grp_7_cd,

       rpt_grp_7_desc,

       rpt_grp_9_cd,

       rpt_grp_9_desc,

       last_chg_datetime,

       effective_from,

       case

         when to_date(effective_to) = '9999-12-31' then dateadd (day,-1,current_timestamp())

         else effective_to

       end as effective_to,

       'N' as active,

       crtd_dttm,

       current_timestamp() as updt_dttm,

       zip_code,

       zip_cd_name,

       barangay_code,

       barangay_cd_name,

       latitude,

       longitude,

       long_lat_source

from (select itg.*

      from {{this}} itg,

           sdl_mds_ph_lav_customer sdl

      where sdl.lastchgdatetime != itg.last_chg_datetime

      and   trim(sdl.code) = itg.cust_id)

union all

select trim(code) as cust_id,

       trim(name) as cust_nm,

       trim(parentcustomer_code) as parent_cust_cd,

       trim(parentcustomer_name) as parent_cust_nm,

       trim(distcode_code) as dstrbtr_grp_cd,

       trim(distcode_name) as dstrbtr_grp_nm,

       trim(regioncode_code) as region_cd,

       trim(regioncode_name) as region_nm,

       trim(provincecode_code) as province_cd,

       trim(provincecode_name) as province_nm,

       trim(muncode_code) as mun_cd,

       trim(muncode_name) as mun_nm,

       trim(channelcode_code) as channel_cd,

       trim(channelcode_name) as channel_desc,

       trim(channelsubgroupcode_code) as sub_chnl_cd,

       trim(channelsubgroupcode_name) as sub_chnl_desc,

       trim(reportgroup3desc_code) as rpt_grp_3_cd,

       trim(reportgroup3desc_name) as rpt_grp_3_desc,

       trim(reportgroup4desc_code) as rpt_grp_4_cd,

       trim(reportgroup4desc_name) as rpt_grp_4_desc,

       trim(reportgroup5desc_code) as rpt_grp_5_cd,

       trim(reportgroup5desc_name) as rpt_grp_5_desc,

       trim(reportgroup6desc_code) as rpt_grp_6_cd,

       trim(reportgroup6desc_name) as rpt_grp_6_desc,

       trim(reportgroup7desc_code) as rpt_grp_7_cd,

       trim(reportgroup7desc_name) as rpt_grp_7_desc,

       trim(reportgroup9desc_code) as rpt_grp_9_cd,

       trim(reportgroup9desc_name) as rpt_grp_9_desc,

       lastchgdatetime as last_chg_datetime,

       current_timestamp() as effective_from,

       '9999-12-31' as effective_to,

       'Y' as active,

       current_timestamp() as crtd_dttm,

       current_timestamp() as updt_dttm,

       trim(zipcode_code) as zip_code,

       trim(zipcode_name) as zip_cd_name,

       trim(barangaycode_code) as barangay_code,

       trim(barangaycode_name) as barangay_cd_name,

       latitude,

       longitude,

       longlatsource as long_lat_source

from (select sdl.*

      from {{this}} itg,

           sdl_mds_ph_lav_customer sdl

      where sdl.lastchgdatetime != itg.last_chg_datetime

      and   trim(sdl.code) = itg.cust_id

      and   itg.active = 'Y')

union all

select trim(code) as cust_id,

       trim(name) as cust_nm,

       trim(parentcustomer_code) as parent_cust_cd,

       trim(parentcustomer_name) as parent_cust_nm,

       trim(distcode_code) as dstrbtr_grp_cd,

       trim(distcode_name) as dstrbtr_grp_nm,

       trim(regioncode_code) as region_cd,

       trim(regioncode_name) as region_nm,

       trim(provincecode_code) as province_cd,

       trim(provincecode_name) as province_nm,

       trim(muncode_code) as mun_cd,

       trim(muncode_name) as mun_nm,

       trim(channelcode_code) as channel_cd,

       trim(channelcode_name) as channel_desc,

       trim(channelsubgroupcode_code) as sub_chnl_cd,

       trim(channelsubgroupcode_name) as sub_chnl_desc,

       trim(reportgroup3desc_code) as rpt_grp_3_cd,

       trim(reportgroup3desc_name) as rpt_grp_3_desc,

       trim(reportgroup4desc_code) as rpt_grp_4_cd,

       trim(reportgroup4desc_name) as rpt_grp_4_desc,

       trim(reportgroup5desc_code) as rpt_grp_5_cd,

       trim(reportgroup5desc_name) as rpt_grp_5_desc,

       trim(reportgroup6desc_code) as rpt_grp_6_cd,

       trim(reportgroup6desc_name) as rpt_grp_6_desc,

       trim(reportgroup7desc_code) as rpt_grp_7_cd,

       trim(reportgroup7desc_name) as rpt_grp_7_desc,

       trim(reportgroup9desc_code) as rpt_grp_9_cd,

       trim(reportgroup9desc_name) as rpt_grp_9_desc,

       lastchgdatetime as last_chg_datetime,

       effective_from,

       '9999-12-31' as effective_to,

       'Y' as active,

       current_timestamp() as crtd_dttm,

       current_timestamp() as updt_dttm,

       trim(zipcode_code) as zip_code,

       trim(zipcode_name) as zip_cd_name,

       trim(barangaycode_code) as barangay_code,

       trim(barangaycode_name) as barangay_cd_name,

       latitude,

       longitude,

       longlatsource as long_lat_source

from (select sdl.*,itg.effective_from

      from {{this}} itg,

           sdl_mds_ph_lav_customer sdl

      where sdl.lastchgdatetime = itg.last_chg_datetime

      and   trim(sdl.code) = itg.cust_id)      

union all

select trim(code) as cust_id,

       trim(name) as cust_nm,

       trim(parentcustomer_code) as parent_cust_cd,

       trim(parentcustomer_name) as parent_cust_nm,

       trim(distcode_code) as dstrbtr_grp_cd,

       trim(distcode_name) as dstrbtr_grp_nm,

       trim(regioncode_code) as region_cd,

       trim(regioncode_name) as region_nm,

       trim(provincecode_code) as province_cd,

       trim(provincecode_name) as province_nm,

       trim(muncode_code) as mun_cd,

       trim(muncode_name) as mun_nm,

       trim(channelcode_code) as channel_cd,

       trim(channelcode_name) as channel_desc,

       trim(channelsubgroupcode_code) as sub_chnl_cd,

       trim(channelsubgroupcode_name) as sub_chnl_desc,

       trim(reportgroup3desc_code) as rpt_grp_3_cd,

       trim(reportgroup3desc_name) as rpt_grp_3_desc,

       trim(reportgroup4desc_code) as rpt_grp_4_cd,

       trim(reportgroup4desc_name) as rpt_grp_4_desc,

       trim(reportgroup5desc_code) as rpt_grp_5_cd,

       trim(reportgroup5desc_name) as rpt_grp_5_desc,

       trim(reportgroup6desc_code) as rpt_grp_6_cd,

       trim(reportgroup6desc_name) as rpt_grp_6_desc,

       trim(reportgroup7desc_code) as rpt_grp_7_cd,

       trim(reportgroup7desc_name) as rpt_grp_7_desc,

       trim(reportgroup9desc_code) as rpt_grp_9_cd,

       trim(reportgroup9desc_name) as rpt_grp_9_desc,

       lastchgdatetime as last_chg_datetime,

       current_timestamp() as effective_from,

       '9999-12-31' as effective_to,

       'Y' as active,

       current_timestamp() as crtd_dttm,

       current_timestamp() as updt_dttm,

       trim(zipcode_code) as zip_code,

       trim(zipcode_name) as zip_cd_name,

       trim(barangaycode_code) as barangay_code,

       trim(barangaycode_name) as barangay_cd_name,

       latitude,

       longitude,

       longlatsource as long_lat_source

from (select *

      from sdl_mds_ph_lav_customer sdl

      where trim(code) not in (select * from subq1))
      ) ,
transformed as (
select * from wks

union all

select *

from {{this}}

where cust_id not in (select trim(cust_id) from wks))
,

final as (
    select
            cust_id::varchar(50) as cust_id,
            cust_nm::varchar(255) as cust_nm,
            parent_cust_cd::varchar(50) as parent_cust_cd,
            parent_cust_nm::varchar(255) as parent_cust_nm,
            dstrbtr_grp_cd::varchar(50) as dstrbtr_grp_cd,
            dstrbtr_grp_nm::varchar(255) as dstrbtr_grp_nm,
            region_cd::varchar(50) as region_cd,
            region_nm::varchar(255) as region_nm,
            province_cd::varchar(50) as province_cd,
            province_nm::varchar(255) as province_nm,
            mun_cd::varchar(50) as mun_cd,
            mun_nm::varchar(255) as mun_nm,
            channel_cd::varchar(50) as channel_cd,
            channel_desc::varchar(255) as channel_desc,
            sub_chnl_cd::varchar(50) as sub_chnl_cd,
            sub_chnl_desc::varchar(255) as sub_chnl_desc,
            rpt_grp_3_cd::varchar(50) as rpt_grp_3_cd,
            rpt_grp_3_desc::varchar(255) as rpt_grp_3_desc,
            rpt_grp_4_cd::varchar(50) as rpt_grp_4_cd,
            rpt_grp_4_desc::varchar(255) as rpt_grp_4_desc,
            rpt_grp_5_cd::varchar(50) as rpt_grp_5_cd,
            rpt_grp_5_desc::varchar(255) as rpt_grp_5_desc,
            rpt_grp_6_cd::varchar(50) as rpt_grp_6_cd,
            rpt_grp_6_desc::varchar(255) as rpt_grp_6_desc,
            rpt_grp_7_cd::varchar(50) as rpt_grp_7_cd,
            rpt_grp_7_desc::varchar(255) as rpt_grp_7_desc,
            rpt_grp_9_cd::varchar(50) as rpt_grp_9_cd,
            rpt_grp_9_desc::varchar(255) as rpt_grp_9_desc,
            last_chg_datetime::timestamp_ntz(9) as last_chg_datetime,
            effective_from::timestamp_ntz(9) as effective_from,
            effective_to::timestamp_ntz(9) as effective_to,
            active::varchar(10) as active,
            crtd_dttm::timestamp_ntz(9) as crtd_dttm,
            updt_dttm::timestamp_ntz(9) as updt_dttm,
            zip_code::varchar(100) as zip_code,
            zip_cd_name::varchar(255) as zip_cd_name,
            barangay_code::varchar(100) as barangay_code,
            barangay_cd_name::varchar(255) as barangay_cd_name,
            latitude::number(20,10) as latitude,
            longitude::number(20,10) as longitude,
            long_lat_source::number(20,10) as long_lat_source 
from transformed
)
select * from final