{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['yearmo','mnth_type']
    )
}}

with wks_my_listprice as (
    select * from {{ ref('myswks_integration__wks_my_listprice') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
b as (select item_cd,
                   min(snapshot_dt) as snapshot_dt
            from wks_my_listprice
            where replace(substring(to_char(snapshot_dt,'YYYY-MM-DD'),0,8),'-','') = replace(substring(to_char(dateadd(day, -1, current_timestamp::date), 'YYYY-MM-DD'),0,8), '-', '')
            group by item_cd),
max_date as (select a.item_cd,
             a.snapshot_dt,
             max(valid_to) as valid_to,
             max(valid_from) as valid_from
      from wks_my_listprice a, b
      where a.item_cd = b.item_cd
      and   a.snapshot_dt = b.snapshot_dt
      group by a.item_cd,
               a.snapshot_dt),
union_1 as (
select plant as plant,
       cnty as cnty,
       wml.item_cd as item_cd,
       wml.item_desc as item_desc,
       wml.valid_from as valid_from,
       wml.valid_to as valid_to,
       rate as rate,
       currency as currency,
       price_unit as price_unit,
       uom as uom,
       replace(substring(to_char(wml.snapshot_dt,'YYYY-MM-DD'),0,8),'-','') as yearmo,
       'CAL' as mnth_type,
       wml.snapshot_dt as snapshot_dt,
       current_timestamp() as crtd_dttm,
       cast(null as date) as updt_dttm
from wks_my_listprice wml, max_date
where wml.item_cd = max_date.item_cd
and   wml.snapshot_dt = max_date.snapshot_dt
and   wml.valid_to = max_date.valid_to
and   wml.valid_from = max_date.valid_from),
veotd as (select mnth_id as jj_mnth_id,
                         cal_date
                  from edw_vw_os_time_dim a
                  group by mnth_id,
                           cal_date),
b_1 as (select item_cd,
                   veotd.jj_mnth_id,
                   min(snapshot_dt) as snapshot_dt
            from wks_my_listprice c,
                  veotd
            where c.snapshot_dt = veotd.cal_date
            and   veotd.jj_mnth_id in (select mnth_id
                                       from edw_vw_os_time_dim a
                                       where 
                                        replace(to_char(dateadd(day, -1, current_timestamp::date), 'YYYYMMDD'), '-', '') = a.cal_date_id)
            group by item_cd,
                     veotd.jj_mnth_id),
max_date_1 as (select a.item_cd,
             a.snapshot_dt,
             jj_mnth_id,
             max(valid_to) as valid_to,
             max(valid_from) as valid_from
      from wks_my_listprice a,b_1
      where a.item_cd = b_1.item_cd
      and   a.snapshot_dt = b_1.snapshot_dt
      group by a.item_cd,
               a.snapshot_dt,
               jj_mnth_id),
union_2 as (
select plant as plant,
       cnty as cnty,
       wml.item_cd as item_cd,
       wml.item_desc as item_desc,
       wml.valid_from as valid_from,
       wml.valid_to as valid_to,
       rate as rate,
       currency as currency,
       price_unit as price_unit,
       uom as uom,
       max_date_1.jj_mnth_id as yearmo,
       'JJ' as mnth_type,
       wml.snapshot_dt as snapshot_dt,
       current_timestamp as crtd_dttm,
       cast(null as date) as updt_dttm
from wks_my_listprice wml, max_date_1
where wml.item_cd = max_date_1.item_cd
and   wml.snapshot_dt = max_date_1.snapshot_dt
and   wml.valid_to = max_date_1.valid_to
and   wml.valid_from = max_date_1.valid_from),
transformed as (
       select * from union_1 union all select * from union_2),
final as (
    select
    plant::varchar(4) as plant,
    cnty::varchar(4) as cnty,
    item_cd::varchar(20) as item_cd,
    item_desc::varchar(100) as item_desc,
    valid_from::varchar(10) as valid_from,
    valid_to::varchar(10) as valid_to,
    rate::number(20,4) as rate,
    currency::varchar(4) as currency,
    price_unit::number(16,4) as price_unit,
    uom::varchar(6) as uom,
    yearmo::varchar(6) as yearmo,
    mnth_type::varchar(6) as mnth_type,
    snapshot_dt::date as snapshot_dt,
    crtd_dttm::timestamp_ntz(9) as crtd_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
from transformed
)
select * from final
