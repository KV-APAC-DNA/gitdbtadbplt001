{{
    config(
        pre_hook="{{build_itg_mds_ph_ref_parent_customer()}}"
    )
}}
with sdl_mds_ph_ref_parent_customer as 
(
    select * from {{ source('phlsdl_raw', 'sdl_mds_ph_ref_parent_customer') }}
),
trans as
(
    (with wks
as
(select parent_cust_cd,
       parent_cust_nm,
       rpt_grp_11,
       rpt_grp_11_desc,
       rpt_grp_12,
       rpt_grp_12_desc,
       rpt_grp_1,
       rpt_grp_1_desc,
       rpt_grp_2,
       rpt_grp_2_desc,
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
       current_timestamp as updt_dttm,
	   customer_segmentation1,
	   customer_segmentation2,
	   channel
from (select itg.*
      from {{this}} itg,
           sdl_mds_ph_ref_parent_customer sdl
      where sdl.lastchgdatetime != itg.last_chg_datetime
      and   trim(sdl.code) = itg.parent_cust_cd)
union all
select trim(code) as parent_cust_cd,
       trim(name) as parent_cust_nm,
       trim(reportgroup11desc_code) as rpt_grp_11,
       trim(reportgroup11desc_name) as rpt_grp_11_desc,
       trim(reportgroup12desc_code) as rpt_grp_12,
       trim(reportgroup12desc_name) as rpt_grp_12_desc,
       trim(reportgroup1desc_code) as rpt_grp_1,
       trim(reportgroup1desc_name) as rpt_grp_1_desc,
       trim(reportgroup2desc_code) as rpt_grp_2,
       trim(reportgroup2desc_name) as rpt_grp_2_desc,
       lastchgdatetime as last_chg_datetime,
       current_timestamp as effective_from,
       '9999-12-31' as effective_to,
       'Y' as active,
       current_timestamp as crtd_dttm,
       current_timestamp as updt_dttm,
	   customer_segmentation_1_code,
	   customer_segmentation_2_code,
	   channel
from (select sdl.*
      from {{this}} itg,
           sdl_mds_ph_ref_parent_customer sdl
      where sdl.lastchgdatetime != itg.last_chg_datetime
      and   trim(sdl.code) = itg.parent_cust_cd
      and   itg.active = 'Y')
union all
select trim(code) as parent_cust_cd,
       trim(name) as parent_cust_nm,
       trim(reportgroup11desc_code) as rpt_grp_11,
       trim(reportgroup11desc_name) as rpt_grp_11_desc,
       trim(reportgroup12desc_code) as rpt_grp_12,
       trim(reportgroup12desc_name) as rpt_grp_12_desc,
       trim(reportgroup1desc_code) as rpt_grp_1,
       trim(reportgroup1desc_name) as rpt_grp_1_desc,
       trim(reportgroup2desc_code) as rpt_grp_2,
       trim(reportgroup2desc_name) as rpt_grp_2_desc,
       lastchgdatetime as last_chg_datetime,
       effective_from,
       '9999-12-31' as effective_to,
       'Y' as active,
       current_timestamp as crtd_dttm,
       current_timestamp as updt_dttm,
	   customer_segmentation_1_code,
	   customer_segmentation_2_code,
	   channel
from (select sdl.*,itg.effective_from
      from {{this}} itg,
           sdl_mds_ph_ref_parent_customer sdl
      where sdl.lastchgdatetime = itg.last_chg_datetime
      and   trim(sdl.code) = itg.parent_cust_cd)      
union all
select trim(code) as parent_cust_cd,
       trim(name) as parent_cust_nm,
       trim(reportgroup11desc_code) as rpt_grp_11,
       trim(reportgroup11desc_name) as rpt_grp_11_desc,
       trim(reportgroup12desc_code) as rpt_grp_12,
       trim(reportgroup12desc_name) as rpt_grp_12_desc,
       trim(reportgroup1desc_code) as rpt_grp_1,
       trim(reportgroup1desc_name) as rpt_grp_1_desc,
       trim(reportgroup2desc_code) as rpt_grp_2,
       trim(reportgroup2desc_name) as rpt_grp_2_desc,
       lastchgdatetime as last_chg_datetime,
       current_timestamp as effective_from,
       '9999-12-31' as effective_to,
       'Y' as active,
       current_timestamp as crtd_dttm,
       current_timestamp as updt_dttm,
	   customer_segmentation_1_code,
	   customer_segmentation_2_code,
	   channel
from (select *
      from sdl_mds_ph_ref_parent_customer sdl
      where trim(code) not in (select distinct parent_cust_cd
                         from {{this}}))) 
     select*
     from wks
union all
select *
from {{this}}
where parent_cust_cd not in (select trim(parent_cust_cd) from wks))
),
final as
(
select 
    parent_cust_cd::varchar(30) as parent_cust_cd,
    parent_cust_nm::varchar(255) as parent_cust_nm,
    rpt_grp_11::varchar(50) as rpt_grp_11,
    rpt_grp_11_desc::varchar(255) as rpt_grp_11_desc,
    rpt_grp_12::varchar(50) as rpt_grp_12,
    rpt_grp_12_desc::varchar(255) as rpt_grp_12_desc,
    rpt_grp_1::varchar(50) as rpt_grp_1,
    rpt_grp_1_desc::varchar(255) as rpt_grp_1_desc,
    rpt_grp_2::varchar(50) as rpt_grp_2,
    rpt_grp_2_desc::varchar(255) as rpt_grp_2_desc,
    last_chg_datetime::timestamp_ntz(9) as last_chg_datetime,
    effective_from::timestamp_ntz(9) as effective_from,
    effective_to::timestamp_ntz(9) as effective_to,
    active::varchar(10) as active,
    current_timestamp()::timestamp_ntz(9) as crtd_dttm ,
    current_timestamp()::timestamp_ntz(9) as updt_dttm,
    customer_segmentation1::varchar(256) as customer_segmentation1,
    customer_segmentation2::varchar(256) as customer_segmentation2,
    channel::varchar(256) as channel
from trans
)
select * from final