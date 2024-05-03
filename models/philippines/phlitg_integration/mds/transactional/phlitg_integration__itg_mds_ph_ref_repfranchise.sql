{{
    config(
        pre_hook="{{build_itg_mds_ph_ref_repfranchise()}}"
    )
}}

with
sdl_mds_ph_ref_repfranchise as (
    select * from {{ source('phlsdl_raw', 'sdl_mds_ph_ref_repfranchise') }}
),
wks as (
    select franchise_id,

       franchise_code,

       franchise_nm,

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

           sdl_mds_ph_ref_repfranchise sdl

      where sdl.lastchgdatetime != itg.last_chg_datetime

      and   trim(sdl.code) = itg.franchise_code)

union all

select cast(trim(franid) as varchar(50)) as franchise_id,

       trim(code) as franchise_code,

       trim(name) as franchise_nm,

       lastchgdatetime as last_chg_datetime,

       current_timestamp() as effective_from,

       '9999-12-31' as effective_to,

       'Y' as active,

       current_timestamp() as crtd_dttm,

       current_timestamp() as updt_dttm

from (select sdl.*

      from {{this}} itg,

           sdl_mds_ph_ref_repfranchise sdl

      where sdl.lastchgdatetime != itg.last_chg_datetime

      and   trim(sdl.code) = itg.franchise_code

      and   itg.active = 'Y')

union all

select cast(trim(franid) as varchar(50)) as franchise_id,

       trim(code) as franchise_code,

       trim(name) as franchise_nm,

       lastchgdatetime as last_chg_datetime,

       effective_from,

       '9999-12-31' as effective_to,

       'Y' as active,

       current_timestamp() as crtd_dttm,

       current_timestamp() as updt_dttm

from (select sdl.*,itg.effective_from

      from {{this}} itg,

           sdl_mds_ph_ref_repfranchise sdl

      where sdl.lastchgdatetime = itg.last_chg_datetime

      and   trim(sdl.code) = itg.franchise_code)

union all

select cast(trim(franid) as varchar(50)) as franchise_id,

       trim(code) as franchise_code,

       trim(name) as franchise_nm,

       lastchgdatetime as last_chg_datetime,

       current_timestamp() as effective_from,

       '9999-12-31' as effective_to,

       'Y' as active,

       current_timestamp() as crtd_dttm,

       current_timestamp() as updt_dttm

from (select *
      from sdl_mds_ph_ref_repfranchise sdl
      where trim(code) not in (select distinct franchise_code
                         from {{this}}))
    ),
transformed as (
select * from wks
union all
select *
from {{this}}
where franchise_code not in (select trim(franchise_code) from wks)
),
final as (
    select
    franchise_id::varchar(30) as franchise_id,
    franchise_code::varchar(50) as franchise_code,
    franchise_nm::varchar(255) as franchise_nm,
    last_chg_datetime::timestamp_ntz(9) as last_chg_datetime,
    effective_from::timestamp_ntz(9) as effective_from,
    effective_to::timestamp_ntz(9) as effective_to,
    active::varchar(10) as active,
    crtd_dttm::timestamp_ntz(9) as crtd_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
from transformed
)
select * from final