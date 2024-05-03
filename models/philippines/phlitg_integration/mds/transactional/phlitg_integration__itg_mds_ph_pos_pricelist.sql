{{
    config(
        pre_hook="{{build_itg_mds_ph_pos_pricelist()}}"
    )
}}

with
sdl_mds_ph_pos_pricelist as (
    select * from {{ source('phlsdl_raw', 'sdl_mds_ph_pos_pricelist') }}
),
wks as
(select code,

       item_cd,

       item_desc,

       jj_mnth_id,

       consumer_bar_cd,

       shippers_bar_cd,

       dz_per_case,

       lst_price_case,

       lst_price_dz,

       lst_price_unit,

       srp,

       status,

       status_nm,

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

           sdl_mds_ph_pos_pricelist sdl

      where sdl.lastchgdatetime != itg.last_chg_datetime

      and   trim(sdl.product_code_code) = itg.item_cd

      and   trim(sdl.yearmo) = itg.jj_mnth_id)

union all

select trim(code) as code,

       trim(product_code_code) as item_cd,

       trim(name) as item_desc,

       trim(yearmo) as jj_mnth_id,

       trim(consumerbarcode) as consumer_bar_cd,

       trim(shippersbarcode) as shippers_bar_cd,

       cast((case when dzpercase = '' or upper(dzpercase) = 'NULL' then null else dzpercase end) as numeric(15,4)) as dz_per_case,

       listpricecase as lst_price_case,

       listpricedz as lst_price_dz,

       listpriceunit as lst_price_unit,

       srp as srp,

       trim(status_code) as status,

       trim(status_name) as status_nm,

       lastchgdatetime as last_chg_datetime,

       current_timestamp() as effective_from,

       '9999-12-31' as effective_to,

       'Y' as active,

       current_timestamp() as crtd_dttm,

       current_timestamp() as updt_dttm

from (select sdl.*

      from {{this}} itg,

           sdl_mds_ph_pos_pricelist sdl

      where sdl.lastchgdatetime != itg.last_chg_datetime

      and   trim(sdl.product_code_code) = itg.item_cd

      and   trim(sdl.yearmo) = itg.jj_mnth_id

      and   itg.active = 'Y')

union all

select trim(code) as code,

       trim(product_code_code) as item_cd,

       trim(name) as item_desc,

       trim(yearmo) as jj_mnth_id,

       trim(consumerbarcode) as consumer_bar_cd,

       trim(shippersbarcode) as shippers_bar_cd,

       cast((case when dzpercase = '' or upper(dzpercase) = 'NULL' then null else dzpercase end) as numeric(15,4)) as dz_per_case,

       listpricecase as lst_price_case,

       listpricedz as lst_price_dz,

       listpriceunit as lst_price_unit,

       srp as srp,

       trim(status_code) as status,

       trim(status_name) as status_nm,

       lastchgdatetime as last_chg_datetime,

       effective_from,

       '9999-12-31' as effective_to,

       'Y' as active,

       current_timestamp() as crtd_dttm,

       current_timestamp() as updt_dttm

from (select sdl.*,

             itg.effective_from

      from {{this}} itg,

           sdl_mds_ph_pos_pricelist sdl

      where sdl.lastchgdatetime = itg.last_chg_datetime

      and   trim(sdl.product_code_code) = itg.item_cd

      and   trim(sdl.yearmo) = itg.jj_mnth_id)

union all

select trim(code) as code,

       trim(product_code_code) as item_cd,

       trim(name) as item_desc,

       trim(yearmo) as jj_mnth_id,

       trim(consumerbarcode) as consumer_bar_cd,

       trim(shippersbarcode) as shippers_bar_cd,

       cast((case when dzpercase = '' or upper(dzpercase) = 'NULL' then null else dzpercase end) as numeric(15,4)) as dz_per_case,

       listpricecase as lst_price_case,

       listpricedz as lst_price_dz,

       listpriceunit as lst_price_unit,

       srp as srp,

       trim(status_code) as status,

       trim(status_name) as status_nm,

       lastchgdatetime as last_chg_datetime,

       current_timestamp() as effective_from,

       '9999-12-31' as effective_to,

       'Y' as active,

       current_timestamp() as crtd_dttm,

       current_timestamp() as updt_dttm

from (select *

      from sdl_mds_ph_pos_pricelist sdl

      where trim(yearmo)||trim(product_code_code) not in (select distinct jj_mnth_id||item_cd

                                         from {{this}}))) 

,
transformed as (
select * from wks
union all
select *
from {{this}}
where jj_mnth_id||item_cd not in (select trim(jj_mnth_id)||trim(item_cd) from wks))
,
final as (
    select
    code::varchar(100) as code,
    item_cd::varchar(50) as item_cd,
    item_desc::varchar(255) as item_desc,
    jj_mnth_id::varchar(50) as jj_mnth_id,
    consumer_bar_cd::varchar(50) as consumer_bar_cd,
    shippers_bar_cd::varchar(50) as shippers_bar_cd,
    dz_per_case::number(15,4) as dz_per_case,
    lst_price_case::number(15,4) as lst_price_case,
    lst_price_dz::number(15,4) as lst_price_dz,
    lst_price_unit::number(15,4) as lst_price_unit,
    srp::number(15,4) as srp,
    status::varchar(50) as status,
    status_nm::varchar(255) as status_nm,
    last_chg_datetime::timestamp_ntz(9) as last_chg_datetime,
    effective_from::timestamp_ntz(9) as effective_from,
    effective_to::timestamp_ntz(9) as effective_to,
    active::varchar(10) as active,
    crtd_dttm::timestamp_ntz(9) as crtd_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm

from transformed
)
select * from final