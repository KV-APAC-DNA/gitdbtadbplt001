{{
    config(
        pre_hook="{{build_itg_mds_ph_distributor_product()}}"
    )
}}

with
sdl_mds_ph_distributor_product as (
    select * from {{ source('phlsdl_raw', 'sdl_mds_ph_distributor_product') }}
),
subq1 as (
select distinct trim(dstrbtr_item_cd) ||trim(dstrbtr_grp_cd) from {{this}}
),
wks
as
(select trim(dstrbtr_item_cd) as dstrbtr_item_cd,

       dstrbtr_item_nm,

       trim(sap_item_cd) as sap_item_cd,

       trim(sap_item_nm) as sap_item_nm,

       trim(dstrbtr_grp_cd) as dstrbtr_grp_cd,

       dstrbtr_grp_nm,

       promo_strt_period,

       promo_end_period,

       promo_reg_ind,

       promo_reg_nm,

       last_chg_datetime,

       effective_from,

       case

         when to_date(effective_to) = '9999-12-31' then dateadd (day,-1,current_timestamp())

         else effective_to

       end as effective_to,

       'N' as active,

       crtd_dttm,

       current_timestamp() as updt_dttm

from (select itg.*

      from {{this}} itg,

           sdl_mds_ph_distributor_product sdl

      where sdl.lastchgdatetime != itg.last_chg_datetime

      and   trim(sdl.itemcode) = trim(itg.dstrbtr_item_cd)

      and   trim(sdl.distcode_code) = trim(itg.dstrbtr_grp_cd))

union all

select trim(itemcode) as dstrbtr_item_cd,

       trim(name) as dstrbtr_item_nm,

       trim(baseitemcode_code) as sap_item_cd,

       trim(baseitemcode_name) as sap_item_nm,

       trim(distcode_code) as dstrbtr_grp_cd,

       trim(distcode_name) as dstrbtr_grp_nm,

       trim(promostartperiod) as promo_strt_period,

       trim(promoendperiod) as promo_end_period,

       trim(promoreg_code) as promo_reg_ind,

       trim(promoreg_name) as promo_reg_nm,

       lastchgdatetime as last_chg_datetime,

       current_timestamp() as effective_from,

       '9999-12-31' as effective_to,

       'Y' as active,

       current_timestamp() as crtd_dttm,

       current_timestamp() as updt_dttm

from (select sdl.*

      from {{this}} itg,

           sdl_mds_ph_distributor_product sdl

      where sdl.lastchgdatetime != itg.last_chg_datetime

      and   trim(sdl.itemcode) = trim(itg.dstrbtr_item_cd)

      and   trim(sdl.distcode_code) = trim(itg.dstrbtr_grp_cd)

      and   itg.active = 'Y')

union all

select trim(itemcode) as dstrbtr_item_cd,

       trim(name) as dstrbtr_item_nm,

       trim(baseitemcode_code) as sap_item_cd,

       trim(baseitemcode_name) as sap_item_nm,

       trim(distcode_code) as dstrbtr_grp_cd,

       trim(distcode_name) as dstrbtr_grp_nm,

       trim(promostartperiod) as promo_strt_period,

       trim(promoendperiod) as promo_end_period,

       trim(promoreg_code) as promo_reg_ind,

       trim(promoreg_name) as promo_reg_nm,

       lastchgdatetime as last_chg_datetime,

       effective_from,

       '9999-12-31' as effective_to,

       'Y' as active,

       current_timestamp() as crtd_dttm,

       current_timestamp() as updt_dttm

from (select sdl.*,

             itg.effective_from

      from {{this}} itg,

           sdl_mds_ph_distributor_product sdl

      where sdl.lastchgdatetime = itg.last_chg_datetime

      and   trim(sdl.itemcode) = trim(itg.dstrbtr_item_cd)

      and   trim(sdl.distcode_code) = trim(itg.dstrbtr_grp_cd))

union all

select trim(itemcode) as dstrbtr_item_cd,

       trim(name) as dstrbtr_item_nm,

       trim(baseitemcode_code) as sap_item_cd,

       trim(baseitemcode_name) as sap_item_nm,

       trim(distcode_code) as dstrbtr_grp_cd,

       trim(distcode_name) as dstrbtr_grp_nm,

       trim(promostartperiod) as promo_strt_period,

       trim(promoendperiod) as promo_end_period,

       trim(promoreg_code) as promo_reg_ind,

       trim(promoreg_name) as promo_reg_nm,

       lastchgdatetime as last_chg_datetime,

       current_timestamp() as effective_from,

       '9999-12-31' as effective_to,

       'Y' as active,

       current_timestamp() as crtd_dttm,

       current_timestamp() as updt_dttm

from (select *

      from sdl_mds_ph_distributor_product sdl

      where trim(itemcode) ||trim(distcode_code) not in (select * from subq1) )
), 
transformed as (
select * from wks

union all

select *

from {{this}}

where dstrbtr_item_cd||dstrbtr_grp_cd not in (select trim(dstrbtr_item_cd)||trim(dstrbtr_grp_cd) from wks)
),
final as (
    select
    dstrbtr_item_cd::varchar(50) as dstrbtr_item_cd,
    dstrbtr_item_nm::varchar(255) as dstrbtr_item_nm,
    sap_item_cd::varchar(50) as sap_item_cd,
    sap_item_nm::varchar(255) as sap_item_nm,
    dstrbtr_grp_cd::varchar(50) as dstrbtr_grp_cd,
    dstrbtr_grp_nm::varchar(255) as dstrbtr_grp_nm,
    promo_strt_period::varchar(50) as promo_strt_period,
    promo_end_period::varchar(50) as promo_end_period,
    promo_reg_ind::varchar(50) as promo_reg_ind,
    promo_reg_nm::varchar(50) as promo_reg_nm,
    last_chg_datetime::timestamp_ntz(9) as last_chg_datetime,
    effective_from::timestamp_ntz(9) as effective_from,
    effective_to::timestamp_ntz(9) as effective_to,
    active::varchar(10) as active,
    crtd_dttm::timestamp_ntz(9) as crtd_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
from transformed
)
select * from final